const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

// --- إعدادات ملفات البيئة ---
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

const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI;

if (!DATABASE_URL) {
  console.error("DATABASE_URL is missing!");
  process.exit(1);
}

// إعدادات Mongoose الحديثة لتجنب التحذيرات
mongoose.set("strictQuery", true);
mongoose.set("bufferCommands", false);

// --- الجداول (Schemas) ---

const userSchema = new mongoose.Schema({
  userName: { type: String, required: true, unique: true, trim: true, index: true },
  socketId: { type: String, default: null },
  online: { type: Boolean, default: false },
  lastSeen: { type: Date, default: Date.now },
  profileImage: { type: String, default: "" },
  country: { type: String, default: "" },
  age: { type: Number, default: null },
}, { timestamps: true });

const friendshipSchema = new mongoose.Schema({
  userA: { type: String, required: true, index: true },
  userB: { type: String, required: true, index: true },
  pairKey: { type: String, required: true, unique: true, index: true },
}, { timestamps: true });

const friendRequestSchema = new mongoose.Schema({
  from: { type: String, required: true, index: true },
  to: { type: String, required: true, index: true },
  status: { type: String, enum: ["pending", "accepted", "rejected"], default: "pending", index: true },
}, { timestamps: true });

const privateMessageSchema = new mongoose.Schema({
  from: { type: String, required: true },
  to: { type: String, required: true },
  text: { type: String, default: "" },
  time: { type: Date, default: Date.now, index: true },
  conversationKey: { type: String, required: true, index: true },
  readBy: { type: [String], default: [] },
}, { timestamps: true });

const randomChatMessageSchema = new mongoose.Schema({
  from: { type: String, required: true },
  to: { type: String, required: true },
  type: { type: String, enum: ["text", "image"], required: true },
  text: { type: String, default: "" },
  image: { type: String, default: "" },
  time: { type: Date, default: Date.now, index: true },
  conversationKey: { type: String, required: true, index: true },
}, { timestamps: true });

const User = mongoose.model("User", userSchema);
const Friendship = mongoose.model("Friendship", friendshipSchema);
const FriendRequest = mongoose.model("FriendRequest", friendRequestSchema);
const PrivateMessage = mongoose.model("PrivateMessage", privateMessageSchema);
const RandomChatMessage = mongoose.model("RandomChatMessage", randomChatMessageSchema);

// --- متغيرات الحالة المؤقتة ---
const socketToUser = new Map();
const waitingQueue = [];
const activeMatches = new Map();
const pendingMatches = new Map();
const pendingMatchByUser = new Map();

// --- الدوال المساعدة (Helpers) ---
function normalizeName(v) { return String(v || "").trim(); }
function pairKey(a, b) {
  const x = normalizeName(a).toLowerCase();
  const y = normalizeName(b).toLowerCase();
  return [x, y].sort().join("__");
}

function removeFromQueue(userName) {
  const i = waitingQueue.indexOf(userName);
  if (i !== -1) waitingQueue.splice(i, 1);
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

// --- منطق البحث والمطابقة (Matchmaking) ---
async function tryMatch(userName) {
  userName = normalizeName(userName);
  if (!userName || activeMatches.has(userName) || pendingMatchByUser.has(userName)) return;

  removeFromQueue(userName);

  let partner = null;
  for (const candidate of [...waitingQueue]) {
    const socket = await getUserSocket(candidate);
    if (socket && candidate !== userName) {
      partner = candidate;
      break;
    }
    removeFromQueue(candidate);
  }

  if (partner) {
    removeFromQueue(partner);
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

// --- الأحداث (Socket Events) ---
io.on("connection", (socket) => {
  console.log(`Socket Connected: ${socket.id}`);

  socket.on("register_user", async (rawName) => {
    const userName = normalizeName(rawName);
    if (!userName) return;
    socket.data.userName = userName;
    socketToUser.set(socket.id, userName);
    await User.findOneAndUpdate(
      { userName },
      { socketId: socket.id, online: true, lastSeen: new Date() },
      { upsert: true, returnDocument: "after" }
    );
  });

  socket.on("sync_profile", async (data) => {
    const userName = normalizeName(data?.userName || socket.data.userName);
    if (!userName) return;
    await User.findOneAndUpdate(
      { userName },
      { 
        profileImage: data.profileImage, 
        country: data.country, 
        age: data.age,
        lastSeen: new Date() 
      },
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
      await emitToUser(me, "system_msg", "بانتظار موافقة الطرف الآخر...");
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

  // --- نظام الأصدقاء والرسائل الخاصة ---
  socket.on("send_friend_request", async (data) => {
    const from = socket.data.userName;
    const to = normalizeName(data.to);
    if (!from || !to || from === to) return;

    await FriendRequest.findOneAndUpdate(
      { from, to },
      { status: "pending" },
      { upsert: true, returnDocument: "after" }
    );
    await emitToUser(to, "friend_request_received", { from });
  });

  socket.on("respond_friend_request", async (data) => {
    const me = socket.data.userName;
    const from = normalizeName(data.from);
    if (data.accepted) {
      await FriendRequest.deleteOne({ from, to: me });
      const key = pairKey(from, me);
      await Friendship.findOneAndUpdate({ pairKey: key }, { userA: from, userB: me }, { upsert: true });
      await emitToUser(from, "friend_added_successfully", me);
      socket.emit("friend_added_successfully", from);
    } else {
      await FriendRequest.deleteOne({ from, to: me });
    }
  });

  socket.on("private_message", async (data) => {
    const from = socket.data.userName;
    const to = normalizeName(data.to);
    if (!from || !to) return;

    const msg = await PrivateMessage.create({
      from, to, text: data.text,
      conversationKey: pairKey(from, to)
    });
    await emitToUser(to, "private_message_received", msg);
  });

  socket.on("disconnect", async () => {
    const me = socket.data.userName;
    if (!me) return;

    const partner = activeMatches.get(me);
    if (partner) {
      activeMatches.delete(me);
      activeMatches.delete(partner);
      await emitToUser(partner, "match_closed", { reason: "partner_left" });
    }

    removeFromQueue(me);
    await User.findOneAndUpdate({ userName: me }, { socketId: null, online: false }, { returnDocument: "after" });
    socketToUser.delete(socket.id);
  });
});

// --- تشغيل السيرفر ---
async function start() {
  try {
    await mongoose.connect(DATABASE_URL);
    console.log("✅ MongoDB Connected");
    server.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
  } catch (err) {
    console.error("❌ Fatal Error:", err);
  }
}

start();
