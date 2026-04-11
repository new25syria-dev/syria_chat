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
    voiceUrl: { type: String, default: "" },
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

const userToSocket = new Map();
const matchmakingLocks = new Set();
const MATCH_PROPOSAL_TTL = 30000;

const activeCalls = new Map();
const pendingCalls = new Map();
const pendingCallByUser = new Map();
const CALL_RING_TIMEOUT = 30000;

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
  let removed = false;
  for (let i = waitingQueue.length - 1; i >= 0; i--) {
    if (waitingQueue[i] === cleanName) {
      waitingQueue.splice(i, 1);
      removed = true;
    }
  }
  if (removed) {
    logInfo("Queue", `User ${cleanName} removed from waiting list.`);
  }
}

async function getUserSocket(userName) {
  try {
    const cleanName = normalizeName(userName);

    const directSocketId = userToSocket.get(cleanName);
    if (directSocketId) {
      const directSocket = io.sockets.sockets.get(directSocketId);
      if (directSocket) return directSocket;
      logInfo("Socket", `Direct socket mapping for ${cleanName} is stale: ${directSocketId}`);
      userToSocket.delete(cleanName);
    }

    const user = await User.findOne({ userName: cleanName }).select("socketId").lean();

    if (!user) {
      logInfo("Socket", `No user found in DB for ${cleanName}`);
      return null;
    }

    if (!user.socketId) {
      logInfo("Socket", `User ${cleanName} has no socketId in DB`);
      return null;
    }

    const socket = io.sockets.sockets.get(user.socketId);
    if (!socket) {
      logInfo("Socket", `Socket ${user.socketId} for ${cleanName} not found in active sockets`);
      return null;
    }

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

async function getUserStatusSummary(userName) {
  try {
    const cleanName = normalizeName(userName);
    const user = await User.findOne({ userName: cleanName })
      .select("userName online lastSeen")
      .lean();

    if (!user) {
      return {
        user: cleanName,
        online: false,
        lastSeen: null
      };
    }

    return {
      user: user.userName,
      online: user.online === true,
      lastSeen: user.lastSeen || null
    };
  } catch (err) {
    logInfo("DB", `Error fetching status summary for ${userName}`, err);
    return {
      user: normalizeName(userName),
      online: false,
      lastSeen: null
    };
  }
}

function extractTargetName(rawValue) {
  if (!rawValue) return "";
  if (typeof rawValue === "string") {
    return normalizeName(rawValue);
  }
  if (typeof rawValue === "object") {
    return normalizeName(
      rawValue.to ||
      rawValue.userName ||
      rawValue.target ||
      rawValue.friend ||
      rawValue.name
    );
  }
  return normalizeName(rawValue);
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

async function ensureUserRegistrationState(userName, socketId = null) {
  try {
    const cleanName = normalizeName(userName);
    if (!cleanName) return false;

    const updateData = {
      online: true,
      lastSeen: new Date()
    };

    if (socketId) {
      updateData.socketId = socketId;
      userToSocket.set(cleanName, socketId);
    }

    await User.findOneAndUpdate(
      { userName: cleanName },
      updateData,
      { upsert: true, returnDocument: "after" }
    );

    return true;
  } catch (err) {
    logInfo("User", `Failed to ensure registration state for ${userName}`, err);
    return false;
  }
}

async function validateUserReadyForMatchmaking(userName, socket) {
  try {
    const me = normalizeName(userName);
    if (!me) return { ok: false, reason: "missing_user" };
    if (!socket || !socket.id) return { ok: false, reason: "missing_socket" };

    const dbUser = await User.findOne({ userName: me }).select("socketId online").lean();

    if (!dbUser) {
      await ensureUserRegistrationState(me, socket.id);
      logInfo("Matchmaking", `Auto-created missing DB user for ${me} before matchmaking.`);
      return { ok: true };
    }

    if (dbUser.socketId !== socket.id || dbUser.online !== true) {
      await ensureUserRegistrationState(me, socket.id);
      logInfo("Matchmaking", `Re-synced user ${me} socket before matchmaking.`, {
        previousSocketId: dbUser.socketId || null,
        currentSocketId: socket.id,
        previousOnline: dbUser.online === true
      });
    }

    return { ok: true };
  } catch (err) {
    logInfo("Matchmaking", `Failed to validate user readiness for ${userName}`, err);
    return { ok: false, reason: "validation_failed" };
  }
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

function isUserBusyForCall(userName) {
  const cleanName = normalizeName(userName);
  return activeCalls.has(cleanName) || pendingCallByUser.has(cleanName);
}

function createPendingCall(caller, callee) {
  const key = pairKey(caller, callee);

  const timeoutId = setTimeout(async () => {
    try {
      const pending = pendingCalls.get(key);
      if (!pending) return;

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.caller);
      pendingCallByUser.delete(pending.callee);

      await emitToUser(pending.caller, "call_ended", { reason: "no_answer" });
      await emitToUser(pending.callee, "call_ended", { reason: "no_answer" });

      logInfo("Call", `Call timed out between ${pending.caller} and ${pending.callee}`);
    } catch (err) {
      logInfo("Error", "Call timeout cleanup failed", err);
    }
  }, CALL_RING_TIMEOUT);

  pendingCalls.set(key, {
    caller,
    callee,
    timeoutId,
    createdAt: Date.now(),
  });

  pendingCallByUser.set(caller, key);
  pendingCallByUser.set(callee, key);

  return key;
}

async function clearCallStateForUser(userName, reason = "ended") {
  const me = normalizeName(userName);
  if (!me) return;

  const activePartner = activeCalls.get(me);
  if (activePartner) {
    activeCalls.delete(me);
    activeCalls.delete(activePartner);
    await emitToUser(activePartner, "call_ended", { reason });
  }

  const pendingKey = pendingCallByUser.get(me);
  if (pendingKey) {
    const pending = pendingCalls.get(pendingKey);
    if (pending) {
      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      const other = pending.caller === me ? pending.callee : pending.caller;

      pendingCalls.delete(pendingKey);
      pendingCallByUser.delete(pending.caller);
      pendingCallByUser.delete(pending.callee);

      await emitToUser(other, "call_ended", { reason });
    } else {
      pendingCallByUser.delete(me);
    }
  }
}

async function forceRefreshUserSocketState(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;

  const socket = await getUserSocket(cleanName);
  if (socket) {
    socket.emit("social_state_refresh", { success: true });
  }
}

async function clearUserBusyState(userName, reason = "state_cleared") {
  const me = normalizeName(userName);
  if (!me) return;

  logInfo("Matchmaking", `clearUserBusyState called for ${me}`, {
    reason,
    inQueue: waitingQueue.includes(me),
    activePartner: activeMatches.get(me) || null,
    pendingKey: pendingMatchByUser.get(me) || null
  });

  removeFromQueue(me);

  const activePartner = activeMatches.get(me);
  if (activePartner) {
    activeMatches.delete(me);
    activeMatches.delete(activePartner);
    await emitToUser(activePartner, "match_closed", { reason });
  }

  const pendingKey = pendingMatchByUser.get(me);
  if (pendingKey) {
    const proposal = pendingMatches.get(pendingKey);
    if (proposal) {
      if (proposal.timeoutId) {
        clearTimeout(proposal.timeoutId);
      }

      const other = proposal.userA === me ? proposal.userB : proposal.userA;

      pendingMatches.delete(pendingKey);
      pendingMatchByUser.delete(proposal.userA);
      pendingMatchByUser.delete(proposal.userB);

      if (reason === "partner_stopped_search") {
        await emitToUser(other, "match_searching", {
          status: "searching",
          message: "Searching for a new partner..."
        });
        tryMatch(other);
      } else {
        await emitToUser(other, "match_cancelled", { reason });
      }
    } else {
      pendingMatchByUser.delete(me);
    }
  }
}

async function clearRelationshipRuntimeState(userA, userB) {
  const a = normalizeName(userA);
  const b = normalizeName(userB);
  if (!a || !b) return;

  removeFromQueue(a);
  removeFromQueue(b);

  const activeA = activeMatches.get(a);
  if (activeA === b) {
    activeMatches.delete(a);
    activeMatches.delete(b);
  }

  const activeB = activeMatches.get(b);
  if (activeB === a) {
    activeMatches.delete(a);
    activeMatches.delete(b);
  }

  const pendingKeyA = pendingMatchByUser.get(a);
  if (pendingKeyA) {
    const proposalA = pendingMatches.get(pendingKeyA);
    if (proposalA) {
      const involvesPair =
        [proposalA.userA, proposalA.userB].includes(a) &&
        [proposalA.userA, proposalA.userB].includes(b);

      if (involvesPair) {
        if (proposalA.timeoutId) {
          clearTimeout(proposalA.timeoutId);
        }
        pendingMatches.delete(pendingKeyA);
        pendingMatchByUser.delete(proposalA.userA);
        pendingMatchByUser.delete(proposalA.userB);
      }
    } else {
      pendingMatchByUser.delete(a);
    }
  }

  const pendingKeyB = pendingMatchByUser.get(b);
  if (pendingKeyB) {
    const proposalB = pendingMatches.get(pendingKeyB);
    if (proposalB) {
      const involvesPair =
        [proposalB.userA, proposalB.userB].includes(a) &&
        [proposalB.userA, proposalB.userB].includes(b);

      if (involvesPair) {
        if (proposalB.timeoutId) {
          clearTimeout(proposalB.timeoutId);
        }
        pendingMatches.delete(pendingKeyB);
        pendingMatchByUser.delete(proposalB.userA);
        pendingMatchByUser.delete(proposalB.userB);
      }
    } else {
      pendingMatchByUser.delete(b);
    }
  }
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

    const mySocket = await getUserSocket(me);
    if (!mySocket) {
      logInfo("Matchmaking", `User ${me} has no active socket. Search aborted.`);
      await emitToUser(me, "error_msg", { message: "User is not fully connected yet" });
      return;
    }

    if (activeMatches.has(me) || pendingMatchByUser.has(me)) {
      logInfo("Matchmaking", `User ${me} is already busy, skipping request.`);
      return;
    }

    removeFromQueue(me);

    let partner = null;
    for (let i = 0; i < waitingQueue.length; i++) {
      const candidate = waitingQueue[i];
      if (candidate === me) continue;

      if (matchmakingLocks.has(candidate)) {
        logInfo("Matchmaking", `Candidate ${candidate} skipped because lock is active.`);
        continue;
      }

      const alreadyFriends = await Friendship.findOne({
        pairKey: pairKey(me, candidate)
      }).lean();

      if (alreadyFriends) {
        logInfo("Matchmaking", `Candidate ${candidate} skipped because already friends with ${me}.`);
        continue;
      }

      const partnerSocket = await getUserSocket(candidate);
      if (partnerSocket && !activeMatches.has(candidate) && !pendingMatchByUser.has(candidate)) {
        partner = candidate;
        waitingQueue.splice(i, 1);
        break;
      } else {
        logInfo("Matchmaking", `Candidate ${candidate} skipped because no active socket or already busy.`);
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

  socket.on("register_user", async (rawName) => {
    try {
      const userName = normalizeName(rawName);
      if (!userName) return;

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
        { upsert: true, returnDocument: "after" }
      );

      logInfo("User", `Registered & Online: ${userName}`);
      socket.emit("registration_success", { userName, timestamp: new Date() });
      socket.emit("user_ready_for_matchmaking", { success: true, userName, socketId: socket.id });
    } catch (err) {
      logInfo("Error", "Registration process failed", err);
      socket.emit("error_msg", { message: "Registration process failed" });
    }
  });

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
          gender: data?.gender ?? "unspecified",
          lastSeen: new Date()
        },
        { returnDocument: "after" }
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

  socket.on("find_partner", async () => {
    try {
      const me = socket.data.userName;
      if (!me) {
        logInfo("Matchmaking", `Socket ${socket.id} attempted matchmaking before registration.`);
        socket.emit("error_msg", { message: "Please register user before matchmaking" });
        return;
      }

      const readiness = await validateUserReadyForMatchmaking(me, socket);
      if (!readiness.ok) {
        logInfo("Matchmaking", `User ${me} is not ready for matchmaking.`, readiness);
        socket.emit("error_msg", { message: "User is not ready for matchmaking yet" });
        return;
      }

      await tryMatch(me);
    } catch (err) {
      logInfo("Error", "find_partner failed", err);
      socket.emit("error_msg", { message: "Failed to start search" });
    }
  });

  socket.on("stop_search", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const cleanMe = normalizeName(me);
      const wasInQueue = waitingQueue.includes(cleanMe);
      const hadPendingMatch = pendingMatchByUser.has(cleanMe);
      const hadActiveMatch = activeMatches.has(cleanMe);

      logInfo("Matchmaking", `stop_search requested by ${me}`, {
        wasInQueue,
        hadPendingMatch,
        hadActiveMatch,
        queueSnapshotBefore: [...waitingQueue]
      });

      if (wasInQueue && !hadPendingMatch && !hadActiveMatch) {
        removeFromQueue(me);

        socket.emit("search_stopped", {
          success: true,
          mode: "queue_only"
        });

        logInfo("Matchmaking", `User ${me} stopped queue-only matchmaking.`, {
          queueSnapshotAfter: [...waitingQueue]
        });

        return;
      }

      await clearUserBusyState(me, "partner_stopped_search");

      socket.emit("search_stopped", {
        success: true,
        mode: "relationship_state"
      });

      logInfo("Matchmaking", `User ${me} stopped matchmaking.`, {
        queueSnapshotAfter: [...waitingQueue]
      });
    } catch (err) {
      logInfo("Error", "stop_search failed", err);
      socket.emit("error_msg", { message: "Failed to stop search" });
    }
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

          await emitToUser(other, "match_searching", {
            status: "searching",
            message: "Searching for a new partner..."
          });

          logInfo("Matchmaking", `Pending match cancelled by ${me} with ${other}. Restarting search for both users.`);

          tryMatch(other);
        }

        return tryMatch(me);
      }

      const partner = activeMatches.get(me);

      if (partner) {
        activeMatches.delete(me);
        activeMatches.delete(partner);

        await emitToUser(partner, "match_searching", {
          status: "searching",
          message: "Searching for a new partner..."
        });

        tryMatch(partner);
      }

      tryMatch(me);
    } catch (err) {
      logInfo("Error", "skip_partner failed", err);
    }
  });

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

  socket.on("send_friend_request", async (targetName) => {
    const me = socket.data.userName;
    const to = extractTargetName(targetName);
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
        await emitToUser(me, "friend_added_successfully", from);
        await emitToUser(from, "friend_added_successfully", me);
        await forceRefreshUserSocketState(me);
        await forceRefreshUserSocketState(from);
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

  socket.on("get_friends_status", async (friends) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const list = Array.isArray(friends) ? friends : [];
      for (const item of list) {
        const friendName = normalizeName(item);
        if (!friendName) continue;

        const status = await getUserStatusSummary(friendName);
        socket.emit("update_status", status);
      }
    } catch (err) {
      logInfo("Error", "Failed to get friends status", err);
      socket.emit("error_msg", { message: "Failed to get friends status" });
    }
  });

  socket.on("delete_friend", async (payload) => {
    try {
      const me = socket.data.userName;
      const friendName = extractTargetName(payload);
      if (!me || !friendName || me === friendName) return;

      const currentPairKey = pairKey(me, friendName);

      const existingFriendship = await Friendship.findOne({
        pairKey: currentPairKey
      }).lean();

      if (existingFriendship) {
        await Friendship.deleteOne({ pairKey: currentPairKey });
      }

      await FriendRequest.deleteMany({
        $or: [
          { from: me, to: friendName },
          { from: friendName, to: me }
        ]
      });

      await PrivateMessage.deleteMany({
        conversationKey: currentPairKey
      });

      await clearRelationshipRuntimeState(me, friendName);

      socket.emit("friend_deleted_successfully", {
        friend: friendName,
        message: `Removed ${friendName} from friends`
      });

      await emitToUser(friendName, "friend_deleted_me", {
        from: me,
        message: `${me} removed you from friends`
      });

      await forceRefreshUserSocketState(me);
      await forceRefreshUserSocketState(friendName);

      logInfo("Social", `${me} removed ${friendName} from friends and deleted all related data.`);
    } catch (err) {
      logInfo("Error", "Delete friend failed", err);
      socket.emit("error_msg", { message: "Failed to delete friend" });
    }
  });

  socket.on("get_private_history", async (data) => {
    try {
      const me = socket.data.userName;
      const other = normalizeName(data?.to || data?.friend || data?.with || data?.userName);

      if (!me || !other) {
        return socket.emit("private_history", []);
      }

      const isFriend = await Friendship.findOne({ pairKey: pairKey(me, other) }).lean();
      if (!isFriend) {
        return socket.emit("private_history", []);
      }

      const messages = await PrivateMessage.find({
        conversationKey: pairKey(me, other),
        isDeleted: false
      })
        .sort({ time: 1, createdAt: 1 })
        .lean();

      socket.emit("private_history", messages);
    } catch (err) {
      logInfo("Error", "Failed to load private history", err);
      socket.emit("private_history", []);
    }
  });

  socket.on("mark_messages_read", async (data) => {
    try {
      const me = socket.data.userName;
      const friend = normalizeName(data?.friend || data?.from || data?.to);

      if (!me || !friend) return;

      await PrivateMessage.updateMany(
        {
          conversationKey: pairKey(me, friend),
          to: me,
          readBy: { $ne: me }
        },
        {
          $addToSet: { readBy: me }
        }
      );
    } catch (err) {
      logInfo("Error", "Failed to mark messages as read", err);
    }
  });

  socket.on("private_message", async (data) => {
    const me = socket.data.userName;
    const to = normalizeName(data?.to);
    const cleanText = String(data?.text || "").trim();
    if (!me || !to || !cleanText) return;

    try {
      const isFriend = await Friendship.findOne({ pairKey: pairKey(me, to) }).lean();
      if (!isFriend) {
        return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const msg = await PrivateMessage.create({
        from: me,
        to,
        text: cleanText,
        conversationKey: pairKey(me, to),
        time: new Date(),
        readBy: [me]
      });

      const plainMsg = msg.toObject ? msg.toObject() : msg;

      const delivered = await emitToUser(to, "private_message_received", plainMsg);

      socket.emit("pm_sent_success", {
        msgId: msg._id,
        delivered: delivered === true
      });

      logInfo("PrivateChat", `Message sent from ${me} to ${to}`, {
        delivered: delivered === true,
        conversationKey: pairKey(me, to)
      });
    } catch (err) {
      logInfo("Error", "Private message system failure", err);
      socket.emit("error_msg", { message: "Private message failed" });
    }
  });

  socket.on("start_private_call", async (data) => {
    try {
      const me = socket.data.userName;
      const to = normalizeName(data?.to);

      if (!me || !to || me === to) return;

      const isFriend = await Friendship.findOne({ pairKey: pairKey(me, to) }).lean();
      if (!isFriend) {
        return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const targetSocket = await getUserSocket(to);
      if (!targetSocket) {
        return socket.emit("call_offline", { to });
      }

      if (isUserBusyForCall(me) || isUserBusyForCall(to)) {
        return socket.emit("call_busy", { to });
      }

      createPendingCall(me, to);

      await emitToUser(to, "incoming_call", {
        from: me,
        friendId: me,
      });

      socket.emit("call_ringing", { to });

      logInfo("Call", `Outgoing call from ${me} to ${to}`);
    } catch (err) {
      logInfo("Error", "start_private_call failed", err);
      socket.emit("error_msg", { message: "Failed to start call" });
    }
  });

  socket.on("accept_private_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = normalizeName(data?.from);
      if (!me || !from) return;

      const key = pendingCallByUser.get(me);
      if (!key) return;

      const pending = pendingCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.caller === from && pending.callee === me) ||
        (pending.caller === me && pending.callee === from);

      if (!validPair) return;

      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.caller);
      pendingCallByUser.delete(pending.callee);

      activeCalls.set(pending.caller, pending.callee);
      activeCalls.set(pending.callee, pending.caller);

      await emitToUser(pending.caller, "call_accepted", { by: pending.callee });
      await emitToUser(pending.caller, "call_connected", { with: pending.callee });
      await emitToUser(pending.callee, "call_connected", { with: pending.caller });

      logInfo("Call", `Call connected between ${pending.caller} and ${pending.callee}`);
    } catch (err) {
      logInfo("Error", "accept_private_call failed", err);
    }
  });

  socket.on("reject_private_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = normalizeName(data?.from);
      if (!me || !from) return;

      const key = pendingCallByUser.get(me);
      if (!key) return;

      const pending = pendingCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.caller === from && pending.callee === me) ||
        (pending.caller === me && pending.callee === from);

      if (!validPair) return;

      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.caller);
      pendingCallByUser.delete(pending.callee);

      await emitToUser(from, "call_rejected", { by: me });
      await emitToUser(me, "call_ended", { reason: "rejected" });

      logInfo("Call", `Call rejected by ${me} from ${from}`);
    } catch (err) {
      logInfo("Error", "reject_private_call failed", err);
    }
  });

  socket.on("end_private_call", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      await clearCallStateForUser(me, "ended");
    } catch (err) {
      logInfo("Error", "end_private_call failed", err);
    }
  });

  socket.on("disconnect", async () => {
    const me = socket.data.userName;
    logInfo("Network", `Socket disconnected: ${socket.id} (User: ${me || "Guest"})`);

    if (me) {
      await clearCallStateForUser(me, "partner_disconnected");

      const partner = activeMatches.get(me);
      if (partner) {
        activeMatches.delete(me);
        activeMatches.delete(partner);
        await emitToUser(partner, "match_closed", { reason: "partner_disconnected" });
      }

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

      const typingKeysToDelete = [];
      for (const [typingKey, timeoutId] of userTypingTimeout.entries()) {
        if (typingKey.includes(me)) {
          clearTimeout(timeoutId);
          typingKeysToDelete.push(typingKey);
        }
      }
      for (const key of typingKeysToDelete) {
        userTypingTimeout.delete(key);
      }

      const currentSocketId = userToSocket.get(me);
      if (currentSocketId === socket.id) {
        userToSocket.delete(me);
        await User.findOneAndUpdate(
          { userName: me },
          {
            online: false,
            lastSeen: new Date(),
            socketId: null
          }
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

startMasterServer();

process.on("unhandledRejection", (reason, promise) => {
  logInfo("Critical", "Unhandled Promise Rejection detected", reason);
});

process.on("uncaughtException", (err) => {
  logInfo("Critical", "Uncaught Exception detected! System remains stable.", err);
});
