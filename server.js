const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

// تحسين تحميل ملفات البيئة
const envCandidates = [".env", ".nenv", ".env.txt"];
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

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

const PORT = process.env.PORT || 10000; // متوافق مع Render
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI;

if (!DATABASE_URL) {
  console.error("CRITICAL: DATABASE_URL is missing!");
  process.exit(1);
}

// إعدادات Mongoose الحديثة
mongoose.set("strictQuery", true);

// --- Models ---
const User = mongoose.model("User", new mongoose.Schema({
  userName: { type: String, required: true, unique: true, index: true },
  socketId: { type: String, default: null },
  online: { type: Boolean, default: false },
  lastSeen: { type: Date, default: Date.now },
  profileImage: { type: String, default: "" },
  country: { type: String, default: "" },
  age: { type: Number, default: null },
}, { timestamps: true }));

const Friendship = mongoose.model("Friendship", new mongoose.Schema({
  userA: { type: String, required: true, index: true },
  userB: { type: String, required: true, index: true },
  pairKey: { type: String, required: true, unique: true },
}, { timestamps: true }));

const FriendRequest = mongoose.model("FriendRequest", new mongoose.Schema({
  from: { type: String, required: true, index: true },
  to: { type: String, required: true, index: true },
  status: { type: String, enum: ["pending", "accepted", "rejected"], default: "pending" },
}, { timestamps: true }));

const PrivateMessage = mongoose.model("PrivateMessage", new mongoose.Schema({
  from: String, to: String, text: String, time: { type: Date, default: Date.now },
  conversationKey: { type: String, index: true },
  readBy: [String],
}, { timestamps: true }));

const RandomChatMessage = mongoose.model("RandomChatMessage", new mongoose.Schema({
  from: String, to: String, type: { type: String, enum: ["text", "image"] },
  text: String, image: String, time: { type: Date, default: Date.now },
  conversationKey: { type: String, index: true },
}, { timestamps: true }));

// --- App State ---
const socketToUser = new Map();
const waitingQueue = [];
const activeMatches = new Map();
const pendingMatches = new Map();
const pendingMatchByUser = new Map();

// --- Utilities ---
function normalizeName(v) { return String(v || "").trim(); }
function pairKey(a, b) {
  const x = normalizeName(a).toLowerCase();
  const y = normalizeName(b).toLowerCase();
  return [x, y].sort().join("__");
}

async function getUserSocket(userName) {
  const user = await User.findOne({ userName: normalizeName(userName) }).lean();
  if (!user || !user.socketId) return null;
  return io.sockets.sockets.get(user.socketId) || null;
}

async function emitToUser(userName, event, payload) {
  const socket = await getUserSocket(userName);
  if (socket) socket.emit(event, payload);
}

// --- Logic ---
async function tryMatch(userName) {
  userName = normalizeName(userName);
  if (!userName || activeMatches.has(userName) || pendingMatchByUser.has(userName)) return;

  // إزالة التكرار من الطابور
  const idx = waitingQueue.indexOf(userName);
  if (idx > -1) waitingQueue.splice(idx, 1);

  let partner = null;
  while (waitingQueue.length > 0) {
    let candidate = waitingQueue.shift();
    let socket = await getUserSocket(candidate);
    if (socket && !activeMatches.has(candidate) && !pendingMatchByUser.has(candidate)) {
      partner = candidate;
      break;
    }
  }

  if (partner) {
    const key = pairKey(userName, partner);
    pendingMatches.set(key, { userA: userName, userB: partner, acceptedBy: new Set() });
    pendingMatchByUser.set(userName, key);
    pendingMatchByUser.set(partner, key);

    await emitToUser(userName, "match_found", { partnerName: partner });
    await emitToUser(partner, "match_found", { partnerName: userName });
  } else {
    waitingQueue.push(userName);
    await emitToUser(userName, "system_msg", "جاري البحث عن شريك...");
  }
}

// --- Socket.io ---
io.on("connection", (socket) => {
  
  socket.on("register_user", async (name) => {
    const userName = normalizeName(name);
    if (!userName) return;
    socket.data.userName = userName;
    socketToUser.set(socket.id, userName);
    // إصلاح التحذير هنا باستخدام returnDocument
    await User.findOneAndUpdate(
      { userName },
      { socketId: socket.id, online: true, lastSeen: new Date() },
      { upsert: true, returnDocument: "after" }
    );
  });

  socket.on("find_partner", () => tryMatch(socket.data.userName));

  socket.on("accept_match", async () => {
    const me = socket.data.userName;
    const key = pendingMatchByUser.get(me);
    if (!key) return;

    const proposal = pendingMatches.get(key);
    if (!proposal) return;

    proposal.acceptedBy.add(me);
    const partner = proposal.userA === me ? proposal.userB : proposal.userA;

    if (proposal.acceptedBy.size === 2) {
      pendingMatches.delete(key);
      pendingMatchByUser.delete(proposal.userA);
      pendingMatchByUser.delete(proposal.userB);
      
      activeMatches.set(proposal.userA, proposal.userB);
      activeMatches.set(proposal.userB, proposal.userA);

      await emitToUser(proposal.userA, "match_confirmed", { partnerName: proposal.userB });
      await emitToUser(proposal.userB, "match_confirmed", { partnerName: proposal.userA });
    } else {
      await emitToUser(partner, "match_partner_ready", { partnerName: me });
    }
  });

  socket.on("message", async (msg) => {
    const me = socket.data.userName;
    const partner = activeMatches.get(me);
    if (partner && msg) {
      await RandomChatMessage.create({
        from: me, to: partner, type: "text", text: String(msg),
        conversationKey: pairKey(me, partner)
      });
      await emitToUser(partner, "message", String(msg));
    }
  });

  socket.on("disconnect", async () => {
    const me = socket.data.userName;
    if (!me) return;

    // تنظيف البيانات عند الخروج
    const partner = activeMatches.get(me);
    if (partner) {
      activeMatches.delete(me);
      activeMatches.delete(partner);
      await emitToUser(partner, "system_msg", "غادر الطرف الآخر المحادثة");
    }

    const pKey = pendingMatchByUser.get(me);
    if (pKey) {
      const prop = pendingMatches.get(pKey);
      if (prop) {
        const pName = prop.userA === me ? prop.userB : prop.userA;
        pendingMatches.delete(pKey);
        pendingMatchByUser.delete(me);
        pendingMatchByUser.delete(pName);
        await emitToUser(pName, "match_closed", { reason: "disconnected" });
      }
    }

    const qIdx = waitingQueue.indexOf(me);
    if (qIdx > -1) waitingQueue.splice(qIdx, 1);

    await User.findOneAndUpdate({ userName: me }, { socketId: null, online: false }, { returnDocument: "after" });
    socketToUser.delete(socket.id);
  });
});

// --- Startup ---
async function start() {
  try {
    await mongoose.connect(DATABASE_URL);
    console.log("✅ Connected to MongoDB");
    server.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
  } catch (err) {
    console.error("❌ DB Error:", err);
  }
}

start();
