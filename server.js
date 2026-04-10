// ==========================================
// 1. استدعاء المكتبات وإعدادات البيئة
// ==========================================
const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

// مصفوفة مسارات ملفات الإعدادات لضمان المرونة القصوى
const envCandidates = [
  ".env",
  ".nenv",
  ".env.txt",
  "config.env",
  "vars.env",
  ".env.production",
  ".env.development"
];

const envPath = envCandidates
  .map((name) => path.join(__dirname, name))
  .find((candidate) => fs.existsSync(candidate));

if (envPath) {
  dotenv.config({ path: envPath });
} else {
  dotenv.config();
}

const express = require("express");
const http = require("http");
const cors = require("cors");
const mongoose = require("mongoose");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

// إعدادات Socket.io المتقدمة لضمان استقرار الاتصال في Flutter
const io = new Server(server, {
  pingTimeout: 60000,
  pingInterval: 25000,
  connectTimeout: 45000,
  allowEIO3: true,
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  },
});

const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI;

if (!DATABASE_URL) {
  console.error("FATAL ERROR: DATABASE_URL is not defined in any environment file!");
  process.exit(1);
}

// إعدادات Mongoose لمنع التحذيرات وضمان استقرار قاعدة البيانات
mongoose.set("strictQuery", true);
mongoose.set("bufferCommands", false);

// ==========================================
// 2. تعريف مخططات قاعدة البيانات (Mongoose Schemas)
// ==========================================

const userSchema = new mongoose.Schema(
  {
    userName: { 
        type: String, 
        required: true, 
        unique: true, 
        trim: true, 
        index: true,
        lowercase: true 
    },
    socketId: { type: String, default: null },
    online: { type: Boolean, default: false },
    lastSeen: { type: Date, default: Date.now },
    profileImage: { type: String, default: "" },
    country: { type: String, default: "" },
    age: { type: Number, default: null },
    bio: { type: String, default: "" },
    gender: { type: String, default: "unspecified" },
    fcmToken: { type: String, default: "" },
    isBanned: { type: Boolean, default: false },
    reports: { type: Number, default: 0 }
  },
  { timestamps: true }
);

const friendshipSchema = new mongoose.Schema(
  {
    userA: { type: String, required: true, trim: true, index: true },
    userB: { type: String, required: true, trim: true, index: true },
    pairKey: { type: String, required: true, unique: true, index: true },
    friendsSince: { type: Date, default: Date.now }
  },
  { timestamps: true }
);

const friendRequestSchema = new mongoose.Schema(
  {
    from: { type: String, required: true, trim: true, index: true },
    to: { type: String, required: true, trim: true, index: true },
    status: { 
        type: String, 
        enum: ["pending", "accepted", "rejected"], 
        default: "pending", 
        index: true 
    },
    sentAt: { type: Date, default: Date.now }
  },
  { timestamps: true }
);
friendRequestSchema.index({ from: 1, to: 1, status: 1 });

const privateMessageSchema = new mongoose.Schema(
  {
    from: { type: String, required: true, trim: true, index: true },
    to: { type: String, required: true, trim: true, index: true },
    text: { type: String, default: "", trim: true },
    imageUrl: { type: String, default: "" },
    time: { type: Date, default: Date.now, index: true },
    conversationKey: { type: String, required: true, index: true },
    readBy: { type: [String], default: [] },
    isDeleted: { type: Boolean, default: false },
    messageType: { type: String, default: "text" }
  },
  { timestamps: true }
);
privateMessageSchema.index({ conversationKey: 1, time: 1 });

const randomChatMessageSchema = new mongoose.Schema(
  {
    from: { type: String, required: true, trim: true, index: true },
    to: { type: String, required: true, trim: true, index: true },
    type: { type: String, enum: ["text", "image", "voice", "system"], required: true, index: true },
    text: { type: String, default: "", trim: true },
    image: { type: String, default: "" },
    time: { type: Date, default: Date.now, index: true },
    conversationKey: { type: String, required: true, index: true },
  },
  { timestamps: true }
);

const User = mongoose.model("User", userSchema);
const Friendship = mongoose.model("Friendship", friendshipSchema);
const FriendRequest = mongoose.model("FriendRequest", friendRequestSchema);
const PrivateMessage = mongoose.model("PrivateMessage", privateMessageSchema);
const RandomChatMessage = mongoose.model("RandomChatMessage", randomChatMessageSchema);

// ==========================================
// 3. إدارة الحالة في الذاكرة (Memory State)
// ==========================================
const socketToUser = new Map();
const waitingQueue = [];
const activeMatches = new Map();
const pendingMatches = new Map();
const pendingMatchByUser = new Map();
const userTypingTimeout = new Map();

// إضافات إصلاحية بدون حذف منطقك
const userToSocket = new Map();
const matchmakingLocks = new Set();
const MATCH_PROPOSAL_TTL = 30000;

// ==========================================
// 4. الدوال المساعدة (Helper Functions)
// ==========================================

function logInfo(scope, message, extra = undefined) {
  const prefix = `[${new Date().toISOString()}] [${scope.toUpperCase()}]`;
  if (extra === undefined) {
    console.log(`${prefix} ${message}`);
  } else {
    console.log(`${prefix} ${message}`, JSON.stringify(extra, null, 2));
  }
}

function normalizeName(value) {
  if (!value) return "";
  return String(value).trim().toLowerCase();
}

function pairKey(a, b) {
  const x = normalizeName(a);
  const y = normalizeName(b);
  return [x, y].sort().join("__");
}

function removeFromQueue(userName) {
  const cleanName = normalizeName(userName);
  const index = waitingQueue.indexOf(cleanName);
  if (index !== -1) {
    waitingQueue.splice(index, 1);
    logInfo("Queue", `User ${cleanName} removed from waiting list.`);
  }
}

async function checkDatabaseConnection() {
  return mongoose.connection.readyState === 1;
}

async function getUserSocket(userName) {
  try {
    const cleanName = normalizeName(userName);

    // أولوية لذاكرة السيرفر السريعة
    const directSocketId = userToSocket.get(cleanName);
    if (directSocketId) {
      const directSocket = io.sockets.sockets.get(directSocketId);
      if (directSocket) return directSocket;
      userToSocket.delete(cleanName);
    }

    const user = await User.findOne({ userName: cleanName }).select("socketId").lean();
    if (!user || !user.socketId) return null;
    const socket = io.sockets.sockets.get(user.socketId);
    return socket || null;
  } catch (err) {
    logInfo("Critical", `Error in getUserSocket for ${userName}`, err);
    return null;
  }
}

async function emitToUser(userName, event, payload) {
  try {
    const socket = await getUserSocket(userName);
    if (socket) {
      socket.emit(event, payload);
      return true;
    }
    return false;
  } catch (err) {
    logInfo("Socket", `Failed to emit ${event} to ${userName}`, err);
    return false;
  }
}

async function getFullUserProfile(userName) {
  try {
    const cleanName = normalizeName(userName);
    const user = await User.findOne({ userName: cleanName }).lean();
    if (!user) return null;
    return {
      userName: user.userName,
      profileImage: user.profileImage,
      country: user.country,
      age: user.age,
      bio: user.bio,
      gender: user.gender,
      lastSeen: user.lastSeen,
      online: user.online
    };
  } catch (err) {
    logInfo("DB", `Error fetching profile for ${userName}`, err);
    return null;
  }
}

function acquireMatchLock(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;
  if (matchmakingLocks.has(cleanName)) return false;
  matchmakingLocks.add(cleanName);
  return true;
}

function releaseMatchLock(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;
  matchmakingLocks.delete(cleanName);
}

async function cleanupPendingProposalByKey(key, reason = "cancelled", requeueOther = false, requeueMe = false) {
  const proposal = pendingMatches.get(key);
  if (!proposal) return null;

  if (proposal.timeoutId) {
    clearTimeout(proposal.timeoutId);
  }

  pendingMatches.delete(key);
  pendingMatchByUser.delete(proposal.userA);
  pendingMatchByUser.delete(proposal.userB);

  const userA = proposal.userA;
  const userB = proposal.userB;

  await emitToUser(userA, "match_cancelled", { reason });
  await emitToUser(userB, "match_cancelled", { reason });

  if (requeueMe) {
    tryMatch(userA);
  }
  if (requeueOther) {
    tryMatch(userB);
  }

  return proposal;
}

function createPendingMatchEntry(userA, userB) {
  const key = pairKey(userA, userB);

  const timeoutId = setTimeout(async () => {
    try {
      const proposal = pendingMatches.get(key);
      if (!proposal) return;

      pendingMatches.delete(key);
      pendingMatchByUser.delete(proposal.userA);
      pendingMatchByUser.delete(proposal.userB);

      await emitToUser(proposal.userA, "match_timeout", { reason: "no_response" });
      await emitToUser(proposal.userB, "match_timeout", { reason: "no_response" });

      tryMatch(proposal.userA);
      tryMatch(proposal.userB);

      logInfo("Matchmaking", `Pending match expired between ${proposal.userA} and ${proposal.userB}`);
    } catch (err) {
      logInfo("Error", "Pending match timeout cleanup failed", err);
    }
  }, MATCH_PROPOSAL_TTL);

  pendingMatches.set(key, {
    userA,
    userB,
    acceptedBy: new Set(),
    createdAt: Date.now(),
    timeoutId
  });

  pendingMatchByUser.set(userA, key);
  pendingMatchByUser.set(userB, key);

  return key;
}

// ==========================================
// 5. منطق المطابقة (Matchmaking System)
// ==========================================

async function tryMatch(userName) {
  const me = normalizeName(userName);
  if (!me) return;

  if (!acquireMatchLock(me)) {
    logInfo("Matchmaking", `User ${me} matchmaking request ignored due to active lock.`);
    return;
  }

  try {
    logInfo("Matchmaking", `User ${me} requested a new match.`);

    // منع المستخدم إذا كان بالفعل في محادثة أو طلب معلق
    if (activeMatches.has(me) || pendingMatchByUser.has(me)) {
      logInfo("Matchmaking", `User ${me} is already busy, skipping request.`);
      return;
    }

    removeFromQueue(me);

    let partner = null;
    // فحص طابور الانتظار
    for (let i = 0; i < waitingQueue.length; i++) {
      const candidate = waitingQueue[i];
      if (candidate === me) continue;

      if (matchmakingLocks.has(candidate)) {
        continue;
      }

      const partnerSocket = await getUserSocket(candidate);
      if (partnerSocket && !activeMatches.has(candidate) && !pendingMatchByUser.has(candidate)) {
        partner = candidate;
        waitingQueue.splice(i, 1);
        break;
      } else {
        // تنظيف الطابور من المستخدمين غير المتصلين
        waitingQueue.splice(i, 1);
        i--;
      }
    }

    if (partner) {
      const key = createPendingMatchEntry(me, partner);

      const myProfile = await getFullUserProfile(me);
      const partnerProfile = await getFullUserProfile(partner);

      await emitToUser(me, "match_found", { partner: partnerProfile, proposalKey: key });
      await emitToUser(partner, "match_found", { partner: myProfile, proposalKey: key });
      logInfo("Matchmaking", `Proposed match between ${me} and ${partner}`);
    } else {
      if (!waitingQueue.includes(me)) {
        waitingQueue.push(me);
      }
      await emitToUser(me, "waiting_in_queue", { 
          status: "searching", 
          message: "Searching for a partner..." 
      });
      logInfo("Matchmaking", `User ${me} added to queue. Queue size: ${waitingQueue.length}`);
    }
  } catch (err) {
    logInfo("Error", `Matchmaking failure for ${me}`, err);
  } finally {
    releaseMatchLock(me);
  }
}

// ==========================================
// 6. أحداث السوكيت (Socket.io Events)
// ==========================================

io.on("connection", (socket) => {
  logInfo("Network", `Socket connected: ${socket.id}`);

  // حدث التسجيل الأساسي
  socket.on("register_user", async (rawName) => {
    try {
      const userName = normalizeName(rawName);
      if (!userName) return;

      // إغلاق أي جلسة قديمة لنفس المستخدم
      const existingUser = await User.findOne({ userName }).select("socketId").lean();
      if (existingUser?.socketId && existingUser.socketId !== socket.id) {
        const oldSocket = io.sockets.sockets.get(existingUser.socketId);
        if (oldSocket) {
          oldSocket.emit("session_replaced", { message: "Logged in from another device" });
          oldSocket.disconnect(true);
        }
        socketToUser.delete(existingUser.socketId);
      }

      socket.data.userName = userName;
      socketToUser.set(socket.id, userName);
      userToSocket.set(userName, socket.id);

      await User.findOneAndUpdate(
        { userName },
        { 
            socketId: socket.id, 
            online: true, 
            lastSeen: new Date() 
        },
        { upsert: true, new: true }
      );
      
      logInfo("User", `Registered & Online: ${userName}`);
      socket.emit("registration_success", { userName, timestamp: new Date() });
    } catch (err) {
      logInfo("Error", "Registration process failed", err);
      socket.emit("error_msg", { message: "Registration process failed" });
    }
  });

  // تحديث بيانات الملف الشخصي
  socket.on("update_profile", async (data) => {
    try {
      const me = socket.data.userName;
      if (!me) return;
      
      const updatedUser = await User.findOneAndUpdate(
        { userName: me },
        { 
          profileImage: data?.profileImage ?? "",
          country: data?.country ?? "",
          age: data?.age ?? null,
          bio: data?.bio ?? "",
          gender: data?.gender ?? "unspecified"
        },
        { new: true }
      );
      
      socket.emit("profile_updated", { 
          success: true, 
          user: updatedUser 
      });
      logInfo("User", `Profile updated for: ${me}`);
    } catch (err) {
      logInfo("Error", "Failed to update profile", err);
      socket.emit("error_msg", { message: "Failed to update profile" });
    }
  });

  // أوامر الشات العشوائي
  socket.on("find_partner", () => {
    tryMatch(socket.data.userName);
  });

  socket.on("accept_match", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const key = pendingMatchByUser.get(me);
      if (!key) return;

      const proposal = pendingMatches.get(key);
      if (!proposal) return;

      proposal.acceptedBy.add(me);
      const partner = proposal.userA === me ? proposal.userB : proposal.userA;

      if (proposal.acceptedBy.size === 2) {
        if (proposal.timeoutId) {
          clearTimeout(proposal.timeoutId);
        }

        pendingMatches.delete(key);
        pendingMatchByUser.delete(proposal.userA);
        pendingMatchByUser.delete(proposal.userB);

        activeMatches.set(proposal.userA, proposal.userB);
        activeMatches.set(proposal.userB, proposal.userA);

        await emitToUser(proposal.userA, "match_confirmed", { partnerName: proposal.userB });
        await emitToUser(proposal.userB, "match_confirmed", { partnerName: proposal.userA });
        logInfo("Matchmaking", `Chat started between ${proposal.userA} and ${proposal.userB}`);
      } else {
        await emitToUser(partner, "partner_accepted", { message: "Partner is ready" });
      }
    } catch (err) {
      logInfo("Error", "accept_match failed", err);
    }
  });

  socket.on("skip_partner", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const pendingKey = pendingMatchByUser.get(me);
      if (pendingKey) {
        const proposal = pendingMatches.get(pendingKey);
        if (proposal) {
          if (proposal.timeoutId) {
            clearTimeout(proposal.timeoutId);
          }

          const other = proposal.userA === me ? proposal.userB : proposal.userA;
          pendingMatches.delete(pendingKey);
          pendingMatchByUser.delete(me);
          pendingMatchByUser.delete(other);

          await emitToUser(other, "match_cancelled", { reason: "partner_skipped" });
          logInfo("Matchmaking", `Pending match cancelled by ${me} with ${other}`);
        }

        return tryMatch(me);
      }

      const partner = activeMatches.get(me);
      
      if (partner) {
        activeMatches.delete(me);
        activeMatches.delete(partner);
        await emitToUser(partner, "match_closed", { reason: "partner_skipped" });
      }
      
      // محاولة مطابقة فورية بعد السكيب
      tryMatch(me);
    } catch (err) {
      logInfo("Error", "skip_partner failed", err);
    }
  });

  // مراسلات الشات العشوائي
  socket.on("message", async (msgContent) => {
    const me = socket.data.userName;
    const partner = activeMatches.get(me);
    const cleanText = String(msgContent || "").trim();
    if (!partner || !cleanText) return;

    try {
      const msgData = {
        from: me,
        to: partner,
        type: "text",
        text: cleanText,
        time: new Date(),
        conversationKey: pairKey(me, partner)
      };
      
      await RandomChatMessage.create(msgData);
      await emitToUser(partner, "message", msgData);
    } catch (err) {
      logInfo("Error", "Random chat message failed to send", err);
    }
  });

  socket.on("send_image", async (imgData) => {
    const me = socket.data.userName;
    const partner = activeMatches.get(me);
    if (!partner || !imgData?.url) return;

    try {
      const data = {
        from: me,
        to: partner,
        type: "image",
        image: imgData.url,
        time: new Date(),
        conversationKey: pairKey(me, partner)
      };
      await RandomChatMessage.create(data);
      await emitToUser(partner, "image_received", data);
    } catch (err) {
      logInfo("Error", "Image transmission failed", err);
    }
  });

  socket.on("typing", (isTyping) => {
    const me = socket.data.userName;
    const partner = activeMatches.get(me);
    if (!partner) return;

    const key = pairKey(me, partner);
    const oldTimeout = userTypingTimeout.get(key);
    if (oldTimeout) {
      clearTimeout(oldTimeout);
    }

    emitToUser(partner, "partner_typing", { isTyping: Boolean(isTyping) });

    if (isTyping) {
      const timeoutId = setTimeout(() => {
        emitToUser(partner, "partner_typing", { isTyping: false });
        userTypingTimeout.delete(key);
      }, 2500);
      userTypingTimeout.set(key, timeoutId);
    } else {
      userTypingTimeout.delete(key);
    }
  });

  // نظام الأصدقاء
  socket.on("send_friend_request", async (targetName) => {
    const me = socket.data.userName;
    const to = normalizeName(targetName);
    if (!me || !to || me === to) return;

    try {
      const targetUser = await User.findOne({ userName: to }).select("_id userName").lean();
      if (!targetUser) {
        return socket.emit("error_msg", { message: "Target user does not exist" });
      }

      const alreadyFriends = await Friendship.findOne({ pairKey: pairKey(me, to) }).lean();
      if (alreadyFriends) {
        return socket.emit("error_msg", { message: "Already friends" });
      }

      const existingReq = await FriendRequest.findOne({ 
          from: me, 
          to, 
          status: "pending" 
      }).lean();

      const reverseReq = await FriendRequest.findOne({
          from: to,
          to: me,
          status: "pending"
      }).lean();
      
      if (reverseReq) {
        return socket.emit("error_msg", { message: "There is already a pending request from this user" });
      }
      
      if (!existingReq) {
        await FriendRequest.create({ from: me, to });
        await emitToUser(to, "new_friend_request", { from: me });
        socket.emit("request_sent", { success: true, to });
        logInfo("Social", `Friend request from ${me} to ${to}`);
      } else {
        socket.emit("error_msg", { message: "Friend request already pending" });
      }
    } catch (err) {
      logInfo("Error", "Friend request system failure", err);
      socket.emit("error_msg", { message: "Friend request failed" });
    }
  });

  socket.on("respond_to_request", async (data) => {
    const me = socket.data.userName;
    const from = normalizeName(data?.from);
    const accept = Boolean(data?.accept);

    try {
      const request = await FriendRequest.findOne({ from, to: me, status: "pending" });
      if (!request) return;

      if (accept) {
        request.status = "accepted";
        await request.save();

        const existingFriendship = await Friendship.findOne({ pairKey: pairKey(from, me) }).lean();
        if (!existingFriendship) {
          await Friendship.create({ 
              userA: from, 
              userB: me, 
              pairKey: pairKey(from, me) 
          });
        }

        await emitToUser(from, "friend_request_accepted", { by: me });
        logInfo("Social", `${me} and ${from} are now friends.`);
      } else {
        request.status = "rejected";
        await request.save();
        await emitToUser(from, "friend_request_rejected", { by: me });
      }
    } catch (err) {
      logInfo("Error", "Friend response failed", err);
      socket.emit("error_msg", { message: "Failed to respond to friend request" });
    }
  });

  socket.on("private_message", async (data) => {
    const me = socket.data.userName;
    const to = normalizeName(data?.to);
    const cleanText = String(data?.text || "").trim();
    if (!me || !to || !cleanText) return;

    try {
      // التأكد من وجود صداقة قبل إرسال رسالة خاصة
      const isFriend = await Friendship.findOne({ pairKey: pairKey(me, to) });
      if (!isFriend) {
          return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const msg = await PrivateMessage.create({
        from: me,
        to,
        text: cleanText,
        conversationKey: pairKey(me, to)
      });
      
      await emitToUser(to, "private_message_received", msg);
      socket.emit("pm_sent_success", { msgId: msg._id });
    } catch (err) {
      logInfo("Error", "Private message system failure", err);
      socket.emit("error_msg", { message: "Private message failed" });
    }
  });

  // إدارة انقطاع الاتصال
  socket.on("disconnect", async () => {
    const me = socket.data.userName;
    logInfo("Network", `Socket disconnected: ${socket.id} (User: ${me || "Guest"})`);
    
    if (me) {
      // إنهاء المحادثات النشطة
      const partner = activeMatches.get(me);
      if (partner) {
        activeMatches.delete(me);
        activeMatches.delete(partner);
        await emitToUser(partner, "match_closed", { reason: "partner_disconnected" });
      }

      // تنظيف طلبات المطابقة المعلقة
      const pKey = pendingMatchByUser.get(me);
      if (pKey) {
          const prop = pendingMatches.get(pKey);
          if (prop) {
              if (prop.timeoutId) {
                clearTimeout(prop.timeoutId);
              }
              const other = prop.userA === me ? prop.userB : prop.userA;
              pendingMatches.delete(pKey);
              pendingMatchByUser.delete(me);
              pendingMatchByUser.delete(other);
              await emitToUser(other, "match_cancelled", { reason: "partner_left" });
          }
      }

      removeFromQueue(me);

      // تنظيف مؤشر typing إن وجد
      const partnerName = activeMatches.get(me);
      if (partnerName) {
        const typingKey = pairKey(me, partnerName);
        const timeoutId = userTypingTimeout.get(typingKey);
        if (timeoutId) {
          clearTimeout(timeoutId);
          userTypingTimeout.delete(typingKey);
        }
      }
      
      // تحديث حالة قاعدة البيانات
      const currentSocketId = userToSocket.get(me);
      if (currentSocketId === socket.id) {
        userToSocket.delete(me);
        await User.findOneAndUpdate(
          { userName: me }, 
          { online: false, lastSeen: new Date(), socketId: null }
        );
      }
    }
    socketToUser.delete(socket.id);
  });
});

// ==========================================
// 7. نقاط النهاية (API Endpoints)
// ==========================================

app.get("/", (req, res) => {
    res.send("Fadfad Master Server is running perfectly.");
});

app.get("/health", async (req, res) => {
  const dbStatus = mongoose.connection.readyState;
  res.json({
    status: "active",
    database: dbStatus === 1 ? "connected" : "error",
    metrics: {
        onlineUsers: userToSocket.size,
        queueSize: waitingQueue.length,
        ongoingChats: activeMatches.size / 2,
        pendingProposals: pendingMatches.size
    },
    system: {
        uptime: process.uptime(),
        memory: process.memoryUsage().heapUsed / 1024 / 1024 + " MB",
        platform: process.platform
    }
  });
});

// ==========================================
// 8. تشغيل السيرفر وحماية العمليات
// ==========================================

async function startMasterServer() {
  try {
    logInfo("System", "Initializing database connection...");
    await mongoose.connect(DATABASE_URL, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000
    });
    logInfo("System", "Database connection established.");

    server.listen(PORT, "0.0.0.0", () => {
      logInfo("System", `MASTER SERVER IS LIVE ON PORT ${PORT}`);
      logInfo("System", "Ready to receive connections from Flutter app.");
    });
  } catch (err) {
    logInfo("Critical", "Failed to start Master Server", err);
    process.exit(1);
  }
}

// تشغيل النظام
startMasterServer();

// معالجة الأخطاء الكارثية لمنع السيرفر من الانهيار الدائم
process.on("unhandledRejection", (reason, promise) => {
  logInfo("Critical", "Unhandled Promise Rejection detected", reason);
});

process.on("uncaughtException", (err) => {
  logInfo("Critical", "Uncaught Exception detected! System remains stable.", err);
});
