const express = require("express"); // إضافة express لضمان استقرار السيرفر
const http = require("http");
const { Pool } = require("pg");
const { Server } = require("socket.io");
const cors = require("cors");

// إعداد Express و HTTP Server
const app = express();
app.use(cors());
const server = http.createServer(app);

// إعداد Socket.io بشكل صحيح ليعمل مع Render وفلاتر
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
});

const PORT = process.env.PORT || 3000;
const DEBUG = true;

function log(...args) {
  if (DEBUG) {
    console.log(new Date().toISOString(), ...args);
  }
}

// التحقق من قاعدة البيانات
if (!process.env.DATABASE_URL) {
  log("❌ DATABASE_URL is missing!");
  // لا نوقف السيرفر فوراً لكي لا ينهار عند التشغيل المحلي بدون DB
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost")
    ? false
    : { rejectUnauthorized: false },
});

// دالة تهيئة الجداول
async function initDb() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS friendships (
        user_a TEXT NOT NULL,
        user_b TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_a, user_b)
      );

      CREATE TABLE IF NOT EXISTS pending_friend_requests (
        target_username TEXT NOT NULL,
        sender_username TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (target_username, sender_username)
      );

      CREATE TABLE IF NOT EXISTS private_messages (
        id BIGSERIAL PRIMARY KEY,
        sender TEXT NOT NULL,
        receiver TEXT NOT NULL,
        message_text TEXT NOT NULL,
        is_read BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    log("✅ [DB] tables initialized");
  } catch (err) {
    console.error("❌ [DB] Error during initialization:", err);
  }
}

// --- دوال مساعدة لقاعدة البيانات ---
function normalizePair(a, b) {
  return a < b ? [a, b] : [b, a];
}

async function dbAreFriends(userA, userB) {
  const [a, b] = normalizePair(userA, userB);
  const result = await pool.query(
    `SELECT 1 FROM friendships WHERE user_a = $1 AND user_b = $2 LIMIT 1`,
    [a, b]
  );
  return result.rowCount > 0;
}

async function dbGetFriendsFor(username) {
  const result = await pool.query(
    `SELECT CASE WHEN user_a = $1 THEN user_b ELSE user_a END AS friend_name 
     FROM friendships WHERE user_a = $1 OR user_b = $1 ORDER BY friend_name ASC`,
    [username]
  );
  return result.rows.map((r) => r.friend_name);
}

// --- منطق إدارة المستخدمين ---
let waitingUsers = [];
const activeChats = new Map();
const onlineUsers = new Map(); // username -> Set of socket IDs
const socketToUsername = new Map(); // socket ID -> username
const lastSeenMap = new Map();

function isUserOnline(username) {
  return onlineUsers.has(username) && onlineUsers.get(username).size > 0;
}

function emitToUser(username, event, data) {
  const userSockets = onlineUsers.get(username);
  if (!userSockets) return false;
  userSockets.forEach((id) => io.to(id).emit(event, data));
  return true;
}

// --- معالجة اتصالات Socket.io ---
io.on("connection", (socket) => {
  log("👤 New Connection:", socket.id);

  // 1. تسجيل دخول المستخدم
  socket.on("register_user", async (username) => {
    if (!username) return;
    const safeName = username.trim();
    socket.username = safeName;
    
    if (!onlineUsers.has(safeName)) onlineUsers.set(safeName, new Set());
    onlineUsers.get(safeName).add(socket.id);
    socketToUsername.set(socket.id, safeName);

    log(`📍 User Registered: ${safeName}`);
    
    // إبلاغ الأصدقاء أنني أصبحت متصلاً
    const friends = await dbGetFriendsFor(safeName);
    friends.forEach(f => emitToUser(f, "update_status", { user: safeName, online: true }));
  });

  // 2. إرسال رسالة خاصة وحفظها (هنا تم الإصلاح)
  socket.on("private_message", async (data, callback) => {
    try {
      const { to, from, text } = data;
      if (!to || !text) return;

      // حفظ في قاعدة البيانات
      const result = await pool.query(
        `INSERT INTO private_messages (sender, receiver, message_text) VALUES ($1, $2, $3) RETURNING *`,
        [from, to, text]
      );

      const payload = {
        id: result.rows[0].id,
        from: from,
        to: to,
        text: text,
        time: result.rows[0].created_at
      };

      // إرسال للطرف الآخر
      const delivered = emitToUser(to, "private_message_received", payload);
      
      if (callback) callback({ success: true, delivered, payload });
    } catch (err) {
      console.error("Private Message Error:", err);
      if (callback) callback({ success: false });
    }
  });

  // 3. جلب الأرشيف (حل مشكلة اختفاء الرسائل)
  socket.on("get_private_history", async (data, callback) => {
    try {
      const { user, friend } = data;
      const result = await pool.query(
        `SELECT id, sender as from, receiver as to, message_text as text, created_at as time 
         FROM private_messages 
         WHERE (sender = $1 AND receiver = $2) OR (sender = $2 AND receiver = $1)
         ORDER BY created_at ASC LIMIT 100`,
        [user, friend]
      );
      if (callback) callback({ success: true, history: result.rows });
    } catch (err) {
      if (callback) callback({ success: false });
    }
  });

  // 4. منطق التخطى والبحث (Random Chat)
  socket.on("find_partner", () => {
    // إزالة من قائمة الانتظار القديمة إن وجد
    waitingUsers = waitingUsers.filter(id => id !== socket.id);
    
    if (waitingUsers.length > 0) {
      const partnerId = waitingUsers.shift();
      activeChats.set(socket.id, partnerId);
      activeChats.set(partnerId, socket.id);

      io.to(socket.id).emit("system_msg", "تم العثور على صديق!");
      io.to(partnerId).emit("system_msg", "تم العثور على صديق!");
    } else {
      waitingUsers.push(socket.id);
      socket.emit("system_msg", "جاري البحث عن شخص...");
    }
  });

  // 5. الانقطاع
  socket.on("disconnect", async () => {
    const username = socketToUsername.get(socket.id);
    if (username) {
      onlineUsers.get(username)?.delete(socket.id);
      if (onlineUsers.get(username)?.size === 0) {
        onlineUsers.delete(username);
        lastSeenMap.set(username, new Date());
        // إبلاغ الأصدقاء
        const friends = await dbGetFriendsFor(username);
        friends.forEach(f => emitToUser(f, "update_status", { user: username, online: false, lastSeen: new Date() }));
      }
    }
    socketToUsername.delete(socket.id);
    waitingUsers = waitingUsers.filter(id => id !== socket.id);
    log("❌ Disconnected:", socket.id);
  });
});

// تشغيل السيرفر
server.listen(PORT, () => {
  log(`🚀 Server running on http://localhost:${PORT}`);
  initDb();
});
