const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

// تحميل ملف .nenv
const nenvPath = path.join(__dirname, ".nenv");
if (fs.existsSync(nenvPath)) {
  dotenv.config({ path: nenvPath });
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

/* =========================
   Config
========================= */
const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI;

if (!DATABASE_URL) {
  console.error("❌ DATABASE_URL is missing in .nenv");
  process.exit(1);
}

/* =========================
   MongoDB settings
========================= */
mongoose.set("strictQuery", true);
mongoose.set("bufferCommands", false);

/* =========================
   Schemas
========================= */
const userSchema = new mongoose.Schema(
  {
    userName: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      index: true,
    },
    socketId: {
      type: String,
      default: null,
    },
    online: {
      type: Boolean,
      default: false,
    },
    lastSeen: {
      type: Date,
      default: Date.now,
    },
    profileImage: {
      type: String,
      default: "",
    },
    country: {
      type: String,
      default: "",
    },
    age: {
      type: Number,
      default: null,
    },
  },
  { timestamps: true }
);

const friendshipSchema = new mongoose.Schema(
  {
    userA: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    userB: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    pairKey: {
      type: String,
      required: true,
      unique: true,
      index: true,
    },
  },
  { timestamps: true }
);

const friendRequestSchema = new mongoose.Schema(
  {
    from: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    to: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    status: {
      type: String,
      enum: ["pending", "accepted", "rejected"],
      default: "pending",
      index: true,
    },
  },
  { timestamps: true }
);

friendRequestSchema.index({ from: 1, to: 1, status: 1 });

const privateMessageSchema = new mongoose.Schema(
  {
    from: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    to: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    text: {
      type: String,
      default: "",
      trim: true,
    },
    time: {
      type: Date,
      default: Date.now,
      index: true,
    },
    conversationKey: {
      type: String,
      required: true,
      index: true,
    },
    readBy: {
      type: [String],
      default: [],
    },
  },
  { timestamps: true }
);

privateMessageSchema.index({ conversationKey: 1, time: 1 });

const User = mongoose.model("User", userSchema);
const Friendship = mongoose.model("Friendship", friendshipSchema);
const FriendRequest = mongoose.model("FriendRequest", friendRequestSchema);
const PrivateMessage = mongoose.model("PrivateMessage", privateMessageSchema);

/* =========================
   Runtime memory
========================= */
const socketToUser = new Map();
const waitingQueue = [];
const activeMatches = new Map();

/* =========================
   Helpers
========================= */
function normalizeName(value) {
  return String(value || "").trim();
}

function pairKey(a, b) {
  const x = normalizeName(a).toLowerCase();
  const y = normalizeName(b).toLowerCase();
  return [x, y].sort().join("__");
}

function conversationKey(a, b) {
  return pairKey(a, b);
}

function removeFromQueue(userName) {
  const i = waitingQueue.indexOf(userName);
  if (i !== -1) waitingQueue.splice(i, 1);
}

function isDbReady() {
  return mongoose.connection.readyState === 1;
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

async function emitSystemMessage(userName, message) {
  await emitToUser(userName, "system_msg", message);
}

async function setUserOnline(userName, socketId) {
  await User.findOneAndUpdate(
    { userName },
    {
      $set: {
        userName,
        socketId,
        online: true,
        lastSeen: new Date(),
      },
    },
    { upsert: true, new: true }
  );
}

async function setUserOffline(userName) {
  await User.findOneAndUpdate(
    { userName },
    {
      $set: {
        socketId: null,
        online: false,
        lastSeen: new Date(),
      },
    }
  );
}

async function areFriends(userA, userB) {
  const key = pairKey(userA, userB);
  const friendship = await Friendship.findOne({ pairKey: key }).lean();
  return !!friendship;
}

async function createFriendship(userA, userB) {
  const a = normalizeName(userA);
  const b = normalizeName(userB);
  const key = pairKey(a, b);

  const existing = await Friendship.findOne({ pairKey: key }).lean();
  if (existing) return existing;

  return Friendship.create({
    userA: a,
    userB: b,
    pairKey: key,
  });
}

async function deleteFriendship(userA, userB) {
  const key = pairKey(userA, userB);
  await Friendship.deleteOne({ pairKey: key });
}

async function getFriendsList(userName) {
  const rows = await Friendship.find({
    $or: [{ userA: userName }, { userB: userName }],
  }).lean();

  return rows
    .map((row) => (row.userA === userName ? row.userB : row.userA))
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
}

async function sendFriendsStatus(requester, friends) {
  const requesterSocket = await getUserSocket(requester);
  if (!requesterSocket) return;

  for (const friend of friends) {
    const cleanFriend = normalizeName(friend);
    if (!cleanFriend) continue;

    const user = await User.findOne({ userName: cleanFriend }).lean();

    requesterSocket.emit("update_status", {
      user: cleanFriend,
      online: user?.online === true,
      lastSeen: user?.lastSeen
        ? new Date(user.lastSeen).toISOString()
        : "غير معروف",
    });
  }
}

async function startMatch(userA, userB) {
  removeFromQueue(userA);
  removeFromQueue(userB);

  activeMatches.set(userA, userB);
  activeMatches.set(userB, userA);

  await emitSystemMessage(userA, `تم العثور على صديق: ${userB}`);
  await emitSystemMessage(userB, `تم العثور على صديق: ${userA}`);

  await emitToUser(userA, "chat_started", {
    partnerId: userB,
    partnerName: userB,
  });

  await emitToUser(userB, "chat_started", {
    partnerId: userA,
    partnerName: userA,
  });

  console.log(`✅ Match started: ${userA} <-> ${userB}`);
}

async function endMatch(userName, reason = "تم إنهاء المحادثة") {
  const partner = activeMatches.get(userName);
  if (!partner) return;

  activeMatches.delete(userName);
  activeMatches.delete(partner);

  await emitSystemMessage(userName, reason);
  await emitSystemMessage(partner, "غادر الطرف الآخر");
}

async function tryMatch(userName) {
  if (!userName) return;

  if (activeMatches.has(userName)) {
    await emitSystemMessage(userName, "أنت بالفعل في محادثة");
    return;
  }

  removeFromQueue(userName);
  const partner = waitingQueue.find((u) => u !== userName);

  if (partner) {
    await startMatch(userName, partner);
  } else {
    waitingQueue.push(userName);
    await emitSystemMessage(userName, "جاري البحث عن شريك...");
  }
}

async function savePrivateMessage({ from, to, text, time }) {
  const t = time ? new Date(time) : new Date();

  return PrivateMessage.create({
    from,
    to,
    text,
    time: t,
    conversationKey: conversationKey(from, to),
    readBy: [from],
  });
}

/* =========================
   Routes
========================= */
app.get("/", (req, res) => {
  res.json({
    ok: true,
    message: "FadFad server running",
    dbReady: isDbReady(),
  });
});

app.get("/health", async (req, res) => {
  try {
    if (!isDbReady()) {
      return res.status(500).json({
        ok: false,
        dbReady: false,
        error: "MongoDB not connected",
      });
    }

    const users = await User.countDocuments();
    const friendships = await Friendship.countDocuments();
    const friendRequests = await FriendRequest.countDocuments();
    const messages = await PrivateMessage.countDocuments();

    res.json({
      ok: true,
      dbReady: true,
      users,
      friendships,
      friendRequests,
      messages,
      waitingQueueLength: waitingQueue.length,
      activeMatchesLength: activeMatches.size,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      dbReady: isDbReady(),
      error: err.message,
    });
  }
});

/* =========================
   Socket
========================= */
io.on("connection", (socket) => {
  console.log("🟢 Socket connected:", socket.id);

  socket.onAny((eventName) => {
    if (!isDbReady()) {
      console.log(`⚠️ DB not ready, skipped event: ${eventName}`);
      socket.emit("system_msg", "السيرفر غير جاهز حالياً، حاول بعد لحظات");
    }
  });

  socket.on("register_user", async (rawUserName) => {
    try {
      if (!isDbReady()) return;

      const userName = normalizeName(rawUserName);
      if (!userName) return;

      socket.data.userName = userName;
      socketToUser.set(socket.id, userName);

      await setUserOnline(userName, socket.id);

      console.log(`✅ register_user: ${userName} -> ${socket.id}`);
    } catch (err) {
      console.error("register_user error:", err);
    }
  });

  socket.on("find_partner", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;
      await tryMatch(userName);
    } catch (err) {
      console.error("find_partner error:", err);
    }
  });

  socket.on("skip_partner", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;

      const partner = activeMatches.get(userName);

      if (!partner) {
        await emitSystemMessage(userName, "لا يوجد شريك لتخطيه");
        return;
      }

      activeMatches.delete(userName);
      activeMatches.delete(partner);

      await emitSystemMessage(userName, "تم التخطي");
      await emitSystemMessage(partner, "تم التخطي من الطرف الآخر");

      await tryMatch(userName);
      await tryMatch(partner);
    } catch (err) {
      console.error("skip_partner error:", err);
    }
  });

  socket.on("leave_chat", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;
      await endMatch(userName, "تم إنهاء المحادثة");
    } catch (err) {
      console.error("leave_chat error:", err);
    }
  });

  socket.on("message", async (msg) => {
    try {
      if (!isDbReady()) return;

      const sender = socket.data.userName || socketToUser.get(socket.id);
      const partner = activeMatches.get(sender);

      if (!sender || !partner || !msg) return;

      await emitToUser(partner, "message", String(msg));
    } catch (err) {
      console.error("message error:", err);
    }
  });

  socket.on("image", async (imgBase64) => {
    try {
      if (!isDbReady()) return;

      const sender = socket.data.userName || socketToUser.get(socket.id);
      const partner = activeMatches.get(sender);

      if (!sender || !partner || !imgBase64) return;

      await emitToUser(partner, "image", imgBase64);
    } catch (err) {
      console.error("image error:", err);
    }
  });

  socket.on("typing", async () => {
    try {
      if (!isDbReady()) return;

      const sender = socket.data.userName || socketToUser.get(socket.id);
      const partner = activeMatches.get(sender);
      if (!sender || !partner) return;

      await emitToUser(partner, "typing");
    } catch (err) {
      console.error("typing error:", err);
    }
  });

  socket.on("stop_typing", async () => {
    try {
      if (!isDbReady()) return;

      const sender = socket.data.userName || socketToUser.get(socket.id);
      const partner = activeMatches.get(sender);
      if (!sender || !partner) return;

      await emitToUser(partner, "stop_typing");
    } catch (err) {
      console.error("stop_typing error:", err);
    }
  });

  socket.on("private_message", async (data) => {
    try {
      if (!isDbReady()) return;

      const from = normalizeName(data?.from || socket.data.userName);
      const to = normalizeName(data?.to);
      const text = String(data?.text || "").trim();
      const time = data?.time || new Date().toISOString();

      if (!from || !to || !text) {
        console.log("⚠️ Invalid private_message payload:", data);
        return;
      }

      const payload = {
        from,
        to,
        text,
        time: new Date(time).toISOString(),
      };

      await savePrivateMessage(payload);
      await emitToUser(to, "private_message_received", payload);

      console.log(`✅ private_message saved and forwarded: ${from} -> ${to}`);
    } catch (err) {
      console.error("private_message error:", err);
    }
  });

  socket.on("get_private_history", async (data) => {
    try {
      if (!isDbReady()) return;

      const from = normalizeName(data?.from || socket.data.userName);
      const to = normalizeName(data?.to);

      if (!from || !to) {
        socket.emit("private_history", []);
        return;
      }

      const rows = await PrivateMessage.find({
        conversationKey: conversationKey(from, to),
      })
        .sort({ time: 1, _id: 1 })
        .lean();

      const history = rows.map((m) => ({
        from: m.from,
        to: m.to,
        text: m.text,
        time:
          m.time instanceof Date
            ? m.time.toISOString()
            : new Date(m.time).toISOString(),
      }));

      socket.emit("private_history", history);
    } catch (err) {
      console.error("get_private_history error:", err);
      socket.emit("private_history", []);
    }
  });

  socket.on("mark_messages_read", async (data) => {
    try {
      if (!isDbReady()) return;

      const user = normalizeName(data?.user || socket.data.userName);
      const friend = normalizeName(data?.friend || data?.friendId);

      if (!user || !friend) return;

      await PrivateMessage.updateMany(
        {
          conversationKey: conversationKey(user, friend),
          to: user,
          readBy: { $ne: user },
        },
        {
          $addToSet: { readBy: user },
        }
      );
    } catch (err) {
      console.error("mark_messages_read error:", err);
    }
  });

  socket.on("send_friend_request", async (data) => {
    try {
      if (!isDbReady()) return;

      const from = normalizeName(data?.from || socket.data.userName);
      const to = normalizeName(data?.to);

      if (!from || !to || from === to) return;

      const alreadyFriends = await areFriends(from, to);
      if (alreadyFriends) {
        socket.emit("friend_request_response", {
          accepted: true,
          message: "هذا المستخدم موجود بالفعل في قائمة أصدقائك",
        });
        return;
      }

      const existingPending = await FriendRequest.findOne({
        from,
        to,
        status: "pending",
      }).lean();

      if (!existingPending) {
        await FriendRequest.create({
          from,
          to,
          status: "pending",
        });
      }

      socket.emit("friend_request_sent", {
        message: "تم إرسال طلب الصداقة",
      });

      await emitToUser(to, "friend_request_received", { from });
    } catch (err) {
      console.error("send_friend_request error:", err);
    }
  });

  socket.on("respond_friend_request", async (data) => {
    try {
      if (!isDbReady()) return;

      const responder = socket.data.userName || socketToUser.get(socket.id);
      const from = normalizeName(data?.from);
      const accepted = data?.accepted === true;

      if (!responder || !from) return;

      const requestDoc = await FriendRequest.findOne({
        from,
        to: responder,
        status: "pending",
      });

      if (!requestDoc) {
        socket.emit("friend_request_response", {
          accepted: false,
          message: "طلب الصداقة غير موجود",
        });
        return;
      }

      if (accepted) {
        requestDoc.status = "accepted";
        await requestDoc.save();

        await createFriendship(from, responder);

        socket.emit("friend_added_successfully", from);

        await emitToUser(from, "friend_added_successfully", responder);
        await emitToUser(from, "friend_request_response", {
          accepted: true,
          message: `وافق ${responder} على طلب الصداقة`,
        });
      } else {
        requestDoc.status = "rejected";
        await requestDoc.save();

        await emitToUser(from, "friend_request_response", {
          accepted: false,
          message: `رفض ${responder} طلب الصداقة`,
        });
      }
    } catch (err) {
      console.error("respond_friend_request error:", err);
    }
  });

  socket.on("get_my_friends", async () => {
    try {
      if (!isDbReady()) return;

      const me = socket.data.userName || socketToUser.get(socket.id);
      if (!me) {
        socket.emit("my_friends", []);
        return;
      }

      const friends = await getFriendsList(me);
      socket.emit("my_friends", friends);
    } catch (err) {
      console.error("get_my_friends error:", err);
      socket.emit("my_friends", []);
    }
  });

  socket.on("get_friends_status", async (friends) => {
    try {
      if (!isDbReady()) return;

      const requester = socket.data.userName || socketToUser.get(socket.id);
      if (!requester || !Array.isArray(friends)) return;

      await sendFriendsStatus(requester, friends);
    } catch (err) {
      console.error("get_friends_status error:", err);
    }
  });

  socket.on("delete_friend", async (data) => {
    try {
      if (!isDbReady()) return;

      const me = socket.data.userName || socketToUser.get(socket.id);
      const friend = normalizeName(data?.friend);

      if (!me || !friend) return;

      await deleteFriendship(me, friend);

      socket.emit("friend_deleted_successfully", {
        message: `تم حذف ${friend} من قائمة الأصدقاء`,
      });

      await emitToUser(friend, "friend_deleted_me", {
        from: me,
        message: `قام ${me} بحذفك من قائمة الأصدقاء`,
      });
    } catch (err) {
      console.error("delete_friend error:", err);
    }
  });

  socket.on("disconnect", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);

      if (userName) {
        await setUserOffline(userName);
        removeFromQueue(userName);

        const partner = activeMatches.get(userName);
        if (partner) {
          activeMatches.delete(userName);
          activeMatches.delete(partner);
          await emitSystemMessage(partner, "انقطع اتصال الطرف الآخر");
        }
      }

      socketToUser.delete(socket.id);
      console.log("🔴 Socket disconnected:", socket.id, "user:", userName);
    } catch (err) {
      console.error("disconnect error:", err);
    }
  });
});

/* =========================
   Start after DB connection
========================= */
async function startServer() {
  try {
    console.log("⏳ Connecting to MongoDB...");
    await mongoose.connect(DATABASE_URL, {
      serverSelectionTimeoutMS: 15000,
    });

    console.log("✅ MongoDB connected");
    server.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error("❌ MongoDB connection failed:");
    console.error(err.message);
    process.exit(1);
  }
}

startServer();
