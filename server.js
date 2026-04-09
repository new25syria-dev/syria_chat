const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");

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

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI;

if (!DATABASE_URL) {
  console.error("DATABASE_URL is missing in your environment file");
  process.exit(1);
}

mongoose.set("strictQuery", true);
mongoose.set("bufferCommands", false);

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

const randomChatMessageSchema = new mongoose.Schema(
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
    type: {
      type: String,
      enum: ["text", "image"],
      required: true,
      index: true,
    },
    text: {
      type: String,
      default: "",
      trim: true,
    },
    image: {
      type: String,
      default: "",
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
  },
  { timestamps: true }
);

randomChatMessageSchema.index({ conversationKey: 1, time: 1 });

const User = mongoose.model("User", userSchema);
const Friendship = mongoose.model("Friendship", friendshipSchema);
const FriendRequest = mongoose.model("FriendRequest", friendRequestSchema);
const PrivateMessage = mongoose.model("PrivateMessage", privateMessageSchema);
const RandomChatMessage = mongoose.model(
  "RandomChatMessage",
  randomChatMessageSchema
);

const socketToUser = new Map();
const waitingQueue = [];
const activeMatches = new Map();
const pendingMatches = new Map();
const pendingMatchByUser = new Map();

function logInfo(scope, message, extra = undefined) {
  const prefix = `[${new Date().toISOString()}] [${scope}]`;
  if (extra === undefined) {
    console.log(`${prefix} ${message}`);
    return;
  }
  console.log(`${prefix} ${message}`, extra);
}

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

function clearPendingProposal(userA, userB) {
  const key = pairKey(userA, userB);
  pendingMatches.delete(key);
  pendingMatchByUser.delete(userA);
  pendingMatchByUser.delete(userB);
}

async function getUserSocket(userName) {
  const user = await User.findOne({ userName: normalizeName(userName) }).lean();
  if (!user || !user.socketId) return null;
  return io.sockets.sockets.get(user.socketId) || null;
}

async function emitToUser(userName, event, payload) {
  const socket = await getUserSocket(userName);
  if (!socket) return;
  socket.emit(event, payload);
}

async function emitSystemMessage(userName, message) {
  await emitToUser(userName, "system_msg", message);
}

async function getPublicUserProfile(userName) {
  const cleanName = normalizeName(userName);
  const user = await User.findOne({ userName: cleanName }).lean();

  return {
    userName: user?.userName || cleanName,
    profileImage: user?.profileImage || "",
    country: user?.country || "",
    age: user?.age ?? null,
  };
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
    { upsert: true, returnDocument: "after" }
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
    },
    { returnDocument: "after" }
  );
}

async function syncUserProfile(userName, data = {}) {
  const cleanName = normalizeName(userName);
  const rawAge = data.age;
  const parsedAge =
    rawAge === null || rawAge === undefined || rawAge === ""
      ? null
      : Number(rawAge);

  await User.findOneAndUpdate(
    { userName: cleanName },
    {
      $set: {
        userName: cleanName,
        profileImage: String(data.profileImage || "").trim(),
        country: String(data.country || "").trim(),
        age: Number.isFinite(parsedAge) ? parsedAge : null,
        lastSeen: new Date(),
      },
    },
    { upsert: true, returnDocument: "after" }
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
        : "ØºÙŠØ± Ù…Ø¹Ø±ÙˆÙ",
    });
  }
}

async function sendMatchFound(userName, partnerName) {
  const partner = await getPublicUserProfile(partnerName);
  logInfo("match_found", `${userName} <= ${partner.userName}`);
  await emitSystemMessage(userName, `ØªÙ… Ø§Ù„Ø¹Ø«ÙˆØ± Ø¹Ù„Ù‰ ${partner.userName}`);
  await emitToUser(userName, "match_found", {
    partner,
  });
}

async function startMatchProposal(userA, userB) {
  userA = normalizeName(userA);
  userB = normalizeName(userB);

  if (!userA || !userB || userA === userB) return;
  if (
    activeMatches.has(userA) ||
    activeMatches.has(userB) ||
    pendingMatchByUser.has(userA) ||
    pendingMatchByUser.has(userB)
  ) {
    return;
  }

  removeFromQueue(userA);
  removeFromQueue(userB);

  const key = pairKey(userA, userB);
  pendingMatches.set(key, {
    key,
    userA,
    userB,
    acceptedBy: new Set(),
  });
  pendingMatchByUser.set(userA, key);
  pendingMatchByUser.set(userB, key);

  logInfo("match_proposal", `${userA} <-> ${userB}`, {
    pendingMatches: pendingMatches.size,
    queueLength: waitingQueue.length,
  });

  await sendMatchFound(userA, userB);
  await sendMatchFound(userB, userA);
}

async function confirmActiveMatch(userA, userB) {
  clearPendingProposal(userA, userB);

  activeMatches.set(userA, userB);
  activeMatches.set(userB, userA);

  logInfo("match_confirmed", `${userA} <-> ${userB}`, {
    activeMatches: activeMatches.size / 2,
  });

  await emitSystemMessage(userA, `ØªÙ… Ø¨Ø¯Ø¡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ù…Ø¹ ${userB}`);
  await emitSystemMessage(userB, `ØªÙ… Ø¨Ø¯Ø¡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ù…Ø¹ ${userA}`);

  await emitToUser(userA, "match_confirmed", {
    partnerName: userB,
  });
  await emitToUser(userB, "match_confirmed", {
    partnerName: userA,
  });
}

async function closePendingMatchForUser(userName, reason, partnerReason) {
  const pendingKey = pendingMatchByUser.get(userName);
  if (!pendingKey) return null;

  const proposal = pendingMatches.get(pendingKey);
  if (!proposal) {
    pendingMatchByUser.delete(userName);
    return null;
  }

  const partner = proposal.userA === userName ? proposal.userB : proposal.userA;

  clearPendingProposal(proposal.userA, proposal.userB);
  await emitSystemMessage(userName, reason);
  await emitToUser(userName, "match_closed", { reason });
  await emitSystemMessage(partner, partnerReason);
  await emitToUser(partner, "match_closed", { reason: partnerReason });

  return partner;
}

async function endMatch(userName, reason = "ØªÙ… Ø¥Ù†Ù‡Ø§Ø¡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©") {
  const partner = activeMatches.get(userName);
  if (!partner) return;

  activeMatches.delete(userName);
  activeMatches.delete(partner);

  await emitSystemMessage(userName, reason);
  await emitSystemMessage(partner, "ØºØ§Ø¯Ø± Ø§Ù„Ø·Ø±Ù Ø§Ù„Ø¢Ø®Ø±");
}

async function tryMatch(userName) {
  userName = normalizeName(userName);
  if (!userName) return;

  if (activeMatches.has(userName)) {
    await emitSystemMessage(userName, "Ø£Ù†Øª Ø¨Ø§Ù„ÙØ¹Ù„ ÙÙŠ Ù…Ø­Ø§Ø¯Ø«Ø©");
    return;
  }

  if (pendingMatchByUser.has(userName)) {
    await emitSystemMessage(userName, "ØªÙ… Ø§Ù„Ø¹Ø«ÙˆØ± Ø¹Ù„Ù‰ Ø´Ø±ÙŠÙƒ Ø¨Ø§Ù†ØªØ¸Ø§Ø± Ù‚Ø±Ø§Ø±Ùƒ");
    return;
  }

  removeFromQueue(userName);

  const partner = waitingQueue.find((candidate) => {
    const other = normalizeName(candidate);
    return (
      other &&
      other !== userName &&
      !activeMatches.has(other) &&
      !pendingMatchByUser.has(other)
    );
  });

  if (partner) {
    logInfo("match_try", `proposal candidate found for ${userName}: ${partner}`);
    await startMatchProposal(userName, partner);
  } else {
    waitingQueue.push(userName);
    logInfo("match_try", `${userName} added to queue`, {
      queueLength: waitingQueue.length,
    });
    await emitSystemMessage(userName, "Ø¬Ø§Ø±ÙŠ Ø§Ù„Ø¨Ø­Ø« Ø¹Ù† Ø´Ø±ÙŠÙƒ...");
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

async function saveRandomChatMessage({ from, to, type, text = "", image = "" }) {
  return RandomChatMessage.create({
    from,
    to,
    type,
    text,
    image,
    time: new Date(),
    conversationKey: conversationKey(from, to),
  });
}

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
    const randomMessages = await RandomChatMessage.countDocuments();

    res.json({
      ok: true,
      dbReady: true,
      users,
      friendships,
      friendRequests,
      messages,
      randomMessages,
      waitingQueueLength: waitingQueue.length,
      activeMatchesLength: activeMatches.size,
      pendingMatchesLength: pendingMatches.size,
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      dbReady: isDbReady(),
      error: err.message,
    });
  }
});

io.on("connection", (socket) => {
  logInfo("socket", `connected ${socket.id}`);
  socket.on("register_user", async (rawUserName) => {
    try {
      if (!isDbReady()) return;

      const userName = normalizeName(rawUserName);
      if (!userName) return;

      socket.data.userName = userName;
      socketToUser.set(socket.id, userName);
      await setUserOnline(userName, socket.id);
      logInfo("register_user", `${userName} -> ${socket.id}`);
    } catch (err) {
      console.error("register_user error:", err);
    }
  });

  socket.on("sync_profile", async (data) => {
    try {
      if (!isDbReady()) return;

      const userName = normalizeName(data?.userName || socket.data.userName);
      if (!userName) return;

      socket.data.userName = userName;
      socketToUser.set(socket.id, userName);
      await syncUserProfile(userName, data);
    } catch (err) {
      console.error("sync_profile error:", err);
    }
  });

  socket.on("find_partner", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;

      logInfo("find_partner", `${userName} requested match`);
      await tryMatch(userName);
    } catch (err) {
      console.error("find_partner error:", err);
    }
  });

  socket.on("accept_match", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;

      const pendingKey = pendingMatchByUser.get(userName);
      if (!pendingKey) return;

      const proposal = pendingMatches.get(pendingKey);
      if (!proposal) {
        pendingMatchByUser.delete(userName);
        return;
      }

      proposal.acceptedBy.add(userName);
      const partner = proposal.userA === userName ? proposal.userB : proposal.userA;
      logInfo("accept_match", `${userName} accepted`, {
        partner,
        acceptedBy: Array.from(proposal.acceptedBy),
      });

      if (proposal.acceptedBy.size < 2) {
        await emitSystemMessage(userName, "???? ?????????? ?????? ????????????. ?????????? ?????????? ??????????");
        await emitToUser(userName, "match_waiting", { partnerName: partner });
        await emitSystemMessage(partner, `${userName} ???????? ???????????? ?????? ????????????????`);
        await emitToUser(partner, "match_partner_ready", {
          partnerName: userName,
        });
        return;
      }

      await confirmActiveMatch(proposal.userA, proposal.userB);
    } catch (err) {
      console.error("accept_match error:", err);
    }
  });

  socket.on("cancel_find", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;

      logInfo("cancel_find", `${userName} cancelled search`);

      const partner = await closePendingMatchForUser(
        userName,
        "???? ?????????? ??????????",
        "?????????? ?????????? ???????? ??????????"
      );
      if (partner) {
        await tryMatch(partner);
        return;
      }

      removeFromQueue(userName);
      await emitSystemMessage(userName, "???? ?????????? ??????????");
    } catch (err) {
      console.error("cancel_find error:", err);
    }
  });

  socket.on("skip_partner", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;
      logInfo("skip_partner", `${userName} requested skip`);

      const pendingKey = pendingMatchByUser.get(userName);
      if (pendingKey) {
        const proposal = pendingMatches.get(pendingKey);
        if (!proposal) {
          pendingMatchByUser.delete(userName);
          return;
        }

        const partner = proposal.userA === userName ? proposal.userB : proposal.userA;

        clearPendingProposal(proposal.userA, proposal.userB);
        await emitSystemMessage(
          userName,
          "???? ????????????. ???????? ?????????? ???? ???????? ????????..."
        );
        await emitToUser(userName, "match_searching", {});
        await emitSystemMessage(
          partner,
          "???? ???????????? ???? ?????????? ??????????"
        );
        await emitToUser(partner, "match_closed", {
          reason: "???? ???????????? ???? ?????????? ??????????",
        });

        await tryMatch(userName);
        await tryMatch(partner);
        return;
      }

      const activePartner = activeMatches.get(userName);
      if (!activePartner) {
        await emitSystemMessage(userName, "???? ???????? ???????? ????????????");
        return;
      }

      activeMatches.delete(userName);
      activeMatches.delete(activePartner);

      await emitSystemMessage(
        userName,
        "???? ????????????. ???????? ?????????? ???? ???????? ????????..."
      );
      await emitToUser(userName, "match_searching", {});
      await emitSystemMessage(activePartner, "???? ???????????? ???? ?????????? ??????????");
      await emitToUser(activePartner, "match_searching", {});

      await tryMatch(userName);
      await tryMatch(activePartner);
    } catch (err) {
      console.error("skip_partner error:", err);
    }
  });

  socket.on("leave_chat", async () => {
    try {
      if (!isDbReady()) return;

      const userName = socket.data.userName || socketToUser.get(socket.id);
      if (!userName) return;

      await endMatch(userName, "ØªÙ… Ø¥Ù†Ù‡Ø§Ø¡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©");
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

      await saveRandomChatMessage({
        from: sender,
        to: partner,
        type: "text",
        text: String(msg),
      });
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

      await saveRandomChatMessage({
        from: sender,
        to: partner,
        type: "image",
        image: String(imgBase64),
      });
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

      if (!from || !to || !text) return;

      const payload = {
        from,
        to,
        text,
        time: new Date(time).toISOString(),
      };

      await savePrivateMessage(payload);
      await emitToUser(to, "private_message_received", payload);
      logInfo("private_message", `${from} -> ${to}`);
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
      const to = normalizeName(
        data?.to || activeMatches.get(from) || pendingMatches.get(pendingMatchByUser.get(from))?.userA
      );

      if (!from || !to || from === to) return;

      const alreadyFriends = await areFriends(from, to);
      if (alreadyFriends) {
        socket.emit("friend_request_response", {
          accepted: true,
          message: "Ù‡Ø°Ø§ Ø§Ù„Ù…Ø³ØªØ®Ø¯Ù… Ù…ÙˆØ¬ÙˆØ¯ Ø¨Ø§Ù„ÙØ¹Ù„ ÙÙŠ Ù‚Ø§Ø¦Ù…Ø© Ø£ØµØ¯Ù‚Ø§Ø¦Ùƒ",
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
        logInfo("friend_request", `${from} -> ${to} created`);
      } else {
        logInfo("friend_request", `${from} -> ${to} reused pending`);
      }

      socket.emit("friend_request_sent", {
        message: "ØªÙ… Ø¥Ø±Ø³Ø§Ù„ Ø·Ù„Ø¨ Ø§Ù„ØµØ¯Ø§Ù‚Ø©",
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
          message: "Ø·Ù„Ø¨ Ø§Ù„ØµØ¯Ø§Ù‚Ø© ØºÙŠØ± Ù…ÙˆØ¬ÙˆØ¯",
        });
        return;
      }

      if (accepted) {
        requestDoc.status = "accepted";
        await requestDoc.save();

        await createFriendship(from, responder);
        logInfo("friend_request", `${responder} accepted ${from}`);

        socket.emit("friend_added_successfully", from);

        await emitToUser(from, "friend_added_successfully", responder);
        await emitToUser(from, "friend_request_response", {
          accepted: true,
          message: `ÙˆØ§ÙÙ‚ ${responder} Ø¹Ù„Ù‰ Ø·Ù„Ø¨ Ø§Ù„ØµØ¯Ø§Ù‚Ø©`,
        });
      } else {
        requestDoc.status = "rejected";
        await requestDoc.save();
        logInfo("friend_request", `${responder} rejected ${from}`);

        await emitToUser(from, "friend_request_response", {
          accepted: false,
          message: `Ø±ÙØ¶ ${responder} Ø·Ù„Ø¨ Ø§Ù„ØµØ¯Ø§Ù‚Ø©`,
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
      const friend = normalizeName(
        typeof data === "string" ? data : data?.friend
      );

      if (!me || !friend) return;

      await deleteFriendship(me, friend);

      socket.emit("friend_deleted_successfully", {
        message: `ØªÙ… Ø­Ø°Ù ${friend} Ù…Ù† Ù‚Ø§Ø¦Ù…Ø© Ø§Ù„Ø£ØµØ¯Ù‚Ø§Ø¡`,
      });

      await emitToUser(friend, "friend_deleted_me", {
        from: me,
        message: `Ù‚Ø§Ù… ${me} Ø¨Ø­Ø°ÙÙƒ Ù…Ù† Ù‚Ø§Ø¦Ù…Ø© Ø§Ù„Ø£ØµØ¯Ù‚Ø§Ø¡`,
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

        const pendingKey = pendingMatchByUser.get(userName);
        if (pendingKey) {
          const proposal = pendingMatches.get(pendingKey);
          if (proposal) {
            const partner =
              proposal.userA === userName ? proposal.userB : proposal.userA;
            clearPendingProposal(proposal.userA, proposal.userB);
            await emitSystemMessage(partner, "Ø§Ù†Ù‚Ø·Ø¹ Ø§ØªØµØ§Ù„ Ø§Ù„Ø·Ø±Ù Ø§Ù„Ø¢Ø®Ø±");
            await emitToUser(partner, "match_closed", {
              reason: "partner_disconnected",
            });
            await tryMatch(partner);
          }
        }

        const partner = activeMatches.get(userName);
        if (partner) {
          activeMatches.delete(userName);
          activeMatches.delete(partner);
          await emitSystemMessage(partner, "Ø§Ù†Ù‚Ø·Ø¹ Ø§ØªØµØ§Ù„ Ø§Ù„Ø·Ø±Ù Ø§Ù„Ø¢Ø®Ø±");
        }
      }

      socketToUser.delete(socket.id);
      logInfo("socket", `disconnected ${socket.id}`, { userName });
    } catch (err) {
      console.error("disconnect error:", err);
    }
  });
});

async function startServer() {
  try {
    await mongoose.connect(DATABASE_URL, {
      serverSelectionTimeoutMS: 15000,
    });

    logInfo("startup", "MongoDB connected");
    server.listen(PORT, () => {
      logInfo("startup", `Server running on port ${PORT}`);
    });
  } catch (err) {
    console.error("MongoDB connection failed:");
    console.error(err.message);
    process.exit(1);
  }
}

startServer();

