// ==========================================
// 1) Imports & Env
// ==========================================
const fs = require("fs");
const path = require("path");
const https = require("https");
const crypto = require("crypto");
const dotenv = require("dotenv");

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

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

if (!DATABASE_URL) {
  console.error("FATAL ERROR: DATABASE_URL is not defined in any environment file!");
  process.exit(1);
}

mongoose.set("strictQuery", true);
mongoose.set("bufferCommands", false);

// ==========================================
// 2) Schemas
// ==========================================
const userSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      required: true,
      unique: true,
      index: true,
      trim: true,
    },
    userName: {
      type: String,
      required: true,
      trim: true,
      index: true,
      lowercase: true,
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
    userAId: { type: String, required: true, trim: true, index: true },
    userBId: { type: String, required: true, trim: true, index: true },
    pairKey: { type: String, required: true, unique: true, index: true },
    userAName: { type: String, default: "", trim: true },
    userBName: { type: String, default: "", trim: true },
    friendsSince: { type: Date, default: Date.now }
  },
  { timestamps: true }
);

const friendRequestSchema = new mongoose.Schema(
  {
    fromId: { type: String, required: true, trim: true, index: true },
    toId: { type: String, required: true, trim: true, index: true },
    fromName: { type: String, default: "", trim: true },
    toName: { type: String, default: "", trim: true },
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
friendRequestSchema.index({ fromId: 1, toId: 1, status: 1 });

const privateMessageSchema = new mongoose.Schema(
  {
    fromId: { type: String, required: true, trim: true, index: true },
    toId: { type: String, required: true, trim: true, index: true },
    fromName: { type: String, default: "", trim: true },
    toName: { type: String, default: "", trim: true },
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
    fromId: { type: String, required: true, trim: true, index: true },
    toId: { type: String, required: true, trim: true, index: true },
    fromName: { type: String, default: "", trim: true },
    toName: { type: String, default: "", trim: true },
    type: {
      type: String,
      enum: ["text", "image", "voice", "system"],
      required: true,
      index: true
    },
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
// 3) Memory State
// ==========================================
const socketToUserId = new Map();      // socket.id -> userId
const userIdToSocket = new Map();      // userId -> socket.id

const waitingQueue = [];               // userIds
const activeMatches = new Map();       // userId -> partnerUserId
const pendingMatches = new Map();      // pairKey -> proposal
const pendingMatchByUser = new Map();  // userId -> pairKey
const userTypingTimeout = new Map();

const matchmakingLocks = new Set();
const MATCH_PROPOSAL_TTL = 30000;

const activeCalls = new Map();         // userId -> partnerUserId
const pendingCalls = new Map();        // pairKey -> call proposal
const pendingCallByUser = new Map();   // userId -> pairKey
const CALL_RING_TIMEOUT = 30000;

// ==========================================
// 4) Helpers
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

function normalizeId(value) {
  if (!value) return "";
  return String(value).trim();
}

function pairKeyByIds(a, b) {
  const x = normalizeId(a);
  const y = normalizeId(b);
  return [x, y].sort().join("__");
}

function safeUuid() {
  if (crypto.randomUUID) return crypto.randomUUID();
  return `${Date.now()}_${Math.random().toString(36).slice(2, 12)}`;
}

function removeFromQueue(userId) {
  const cleanId = normalizeId(userId);
  let removed = false;
  for (let i = waitingQueue.length - 1; i >= 0; i--) {
    if (waitingQueue[i] === cleanId) {
      waitingQueue.splice(i, 1);
      removed = true;
    }
  }
  if (removed) {
    logInfo("Queue", `User ${cleanId} removed from waiting list.`);
  }
}

async function getUserSocket(userId) {
  try {
    const cleanId = normalizeId(userId);

    const directSocketId = userIdToSocket.get(cleanId);
    if (directSocketId) {
      const directSocket = io.sockets.sockets.get(directSocketId);
      if (directSocket) return directSocket;
      userIdToSocket.delete(cleanId);
    }

    const user = await User.findOne({ userId: cleanId }).select("socketId").lean();
    if (!user || !user.socketId) return null;

    const socket = io.sockets.sockets.get(user.socketId);
    if (!socket) return null;

    return socket || null;
  } catch (err) {
    logInfo("Critical", `Error in getUserSocket for ${userId}`, err);
    return null;
  }
}

async function emitToUser(userId, event, payload) {
  try {
    const socket = await getUserSocket(userId);
    if (socket) {
      socket.emit(event, payload);
      return true;
    }
    return false;
  } catch (err) {
    logInfo("Socket", `Failed to emit ${event} to ${userId}`, err);
    return false;
  }
}

async function getUserById(userId) {
  const cleanId = normalizeId(userId);
  if (!cleanId) return null;
  return User.findOne({ userId: cleanId }).lean();
}

async function getUserByNameIfUnique(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return { ok: false, reason: "missing_name" };

  const users = await User.find({ userName: cleanName }).select("userId userName").lean();

  if (users.length === 0) {
    return { ok: false, reason: "not_found" };
  }

  if (users.length > 1) {
    return { ok: false, reason: "ambiguous_name" };
  }

  return { ok: true, user: users[0] };
}

async function resolveUserRef(rawValue) {
  if (!rawValue) return { ok: false, reason: "missing_target" };

  if (typeof rawValue === "string") {
    return getUserByNameIfUnique(rawValue);
  }

  if (typeof rawValue === "object") {
    const maybeId =
      rawValue.userId ||
      rawValue.friendId ||
      rawValue.toId ||
      rawValue.fromId ||
      rawValue.id;

    const maybeName =
      rawValue.userName ||
      rawValue.friendName ||
      rawValue.to ||
      rawValue.from ||
      rawValue.friend ||
      rawValue.name;

    if (maybeId) {
      const user = await getUserById(maybeId);
      if (!user) return { ok: false, reason: "not_found" };
      return { ok: true, user };
    }

    if (maybeName) {
      return getUserByNameIfUnique(maybeName);
    }
  }

  return { ok: false, reason: "invalid_target" };
}

async function getFullUserProfile(userId) {
  try {
    const cleanId = normalizeId(userId);
    const user = await User.findOne({ userId: cleanId }).lean();
    if (!user) return null;

    return {
      userId: user.userId,
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
    logInfo("DB", `Error fetching profile for ${userId}`, err);
    return null;
  }
}

async function getUserStatusSummary(userId) {
  try {
    const cleanId = normalizeId(userId);
    const user = await User.findOne({ userId: cleanId })
      .select("userId userName online lastSeen")
      .lean();

    if (!user) {
      return {
        userId: cleanId,
        userName: "",
        online: false,
        lastSeen: null
      };
    }

    return {
      userId: user.userId,
      userName: user.userName,
      online: user.online === true,
      lastSeen: user.lastSeen || null
    };
  } catch (err) {
    logInfo("DB", `Error fetching status summary for ${userId}`, err);
    return {
      userId: normalizeId(userId),
      userName: "",
      online: false,
      lastSeen: null
    };
  }
}

function acquireMatchLock(userId) {
  const cleanId = normalizeId(userId);
  if (!cleanId) return false;
  if (matchmakingLocks.has(cleanId)) return false;
  matchmakingLocks.add(cleanId);
  return true;
}

function releaseMatchLock(userId) {
  const cleanId = normalizeId(userId);
  if (!cleanId) return;
  matchmakingLocks.delete(cleanId);
}

async function ensureUserRegistrationState(userId, socketId = null) {
  try {
    const cleanId = normalizeId(userId);
    if (!cleanId) return false;

    const updateData = {
      online: true,
      lastSeen: new Date()
    };

    if (socketId) {
      updateData.socketId = socketId;
      userIdToSocket.set(cleanId, socketId);
    }

    await User.findOneAndUpdate(
      { userId: cleanId },
      updateData
    );

    return true;
  } catch (err) {
    logInfo("User", `Failed to ensure registration state for ${userId}`, err);
    return false;
  }
}

async function validateUserReadyForMatchmaking(userId, socket) {
  try {
    const me = normalizeId(userId);
    if (!me) return { ok: false, reason: "missing_user" };
    if (!socket || !socket.id) return { ok: false, reason: "missing_socket" };

    const dbUser = await User.findOne({ userId: me }).select("socketId online").lean();

    if (!dbUser) {
      return { ok: false, reason: "missing_db_user" };
    }

    if (dbUser.socketId !== socket.id || dbUser.online !== true) {
      await ensureUserRegistrationState(me, socket.id);
    }

    return { ok: true };
  } catch (err) {
    logInfo("Matchmaking", `Failed to validate user readiness for ${userId}`, err);
    return { ok: false, reason: "validation_failed" };
  }
}

function createPendingMatchEntry(userAId, userBId) {
  const key = pairKeyByIds(userAId, userBId);

  const timeoutId = setTimeout(async () => {
    try {
      const proposal = pendingMatches.get(key);
      if (!proposal) return;

      pendingMatches.delete(key);
      pendingMatchByUser.delete(proposal.userAId);
      pendingMatchByUser.delete(proposal.userBId);

      await emitToUser(proposal.userAId, "match_timeout", { reason: "no_response" });
      await emitToUser(proposal.userBId, "match_timeout", { reason: "no_response" });

      tryMatch(proposal.userAId);
      tryMatch(proposal.userBId);
    } catch (err) {
      logInfo("Error", "Pending match timeout cleanup failed", err);
    }
  }, MATCH_PROPOSAL_TTL);

  pendingMatches.set(key, {
    userAId,
    userBId,
    acceptedBy: new Set(),
    createdAt: Date.now(),
    timeoutId
  });

  pendingMatchByUser.set(userAId, key);
  pendingMatchByUser.set(userBId, key);

  return key;
}

function isUserBusyForCall(userId) {
  const cleanId = normalizeId(userId);
  return activeCalls.has(cleanId) || pendingCallByUser.has(cleanId);
}

function createPendingCall(callerId, calleeId) {
  const key = pairKeyByIds(callerId, calleeId);

  const timeoutId = setTimeout(async () => {
    try {
      const pending = pendingCalls.get(key);
      if (!pending) return;

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.callerId);
      pendingCallByUser.delete(pending.calleeId);

      await emitToUser(pending.callerId, "call_ended", { reason: "no_answer" });
      await emitToUser(pending.calleeId, "call_ended", { reason: "no_answer" });
    } catch (err) {
      logInfo("Error", "Call timeout cleanup failed", err);
    }
  }, CALL_RING_TIMEOUT);

  pendingCalls.set(key, {
    callerId,
    calleeId,
    timeoutId,
    createdAt: Date.now(),
  });

  pendingCallByUser.set(callerId, key);
  pendingCallByUser.set(calleeId, key);

  return key;
}

async function clearCallStateForUser(userId, reason = "ended") {
  const me = normalizeId(userId);
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
      if (pending.timeoutId) clearTimeout(pending.timeoutId);

      const other = pending.callerId === me ? pending.calleeId : pending.callerId;

      pendingCalls.delete(pendingKey);
      pendingCallByUser.delete(pending.callerId);
      pendingCallByUser.delete(pending.calleeId);

      await emitToUser(other, "call_ended", { reason });
    } else {
      pendingCallByUser.delete(me);
    }
  }
}

async function forceRefreshUserSocketState(userId) {
  const cleanId = normalizeId(userId);
  if (!cleanId) return;

  const socket = await getUserSocket(cleanId);
  if (socket) {
    socket.emit("social_state_refresh", { success: true });
  }
}

async function clearUserBusyState(userId, reason = "state_cleared") {
  const me = normalizeId(userId);
  if (!me) return;

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
      if (proposal.timeoutId) clearTimeout(proposal.timeoutId);

      const other = proposal.userAId === me ? proposal.userBId : proposal.userAId;

      pendingMatches.delete(pendingKey);
      pendingMatchByUser.delete(proposal.userAId);
      pendingMatchByUser.delete(proposal.userBId);

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

async function clearRelationshipRuntimeState(userAId, userBId) {
  const a = normalizeId(userAId);
  const b = normalizeId(userBId);
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
        [proposalA.userAId, proposalA.userBId].includes(a) &&
        [proposalA.userAId, proposalA.userBId].includes(b);

      if (involvesPair) {
        if (proposalA.timeoutId) clearTimeout(proposalA.timeoutId);
        pendingMatches.delete(pendingKeyA);
        pendingMatchByUser.delete(proposalA.userAId);
        pendingMatchByUser.delete(proposalA.userBId);
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
        [proposalB.userAId, proposalB.userBId].includes(a) &&
        [proposalB.userAId, proposalB.userBId].includes(b);

      if (involvesPair) {
        if (proposalB.timeoutId) clearTimeout(proposalB.timeoutId);
        pendingMatches.delete(pendingKeyB);
        pendingMatchByUser.delete(proposalB.userAId);
        pendingMatchByUser.delete(proposalB.userBId);
      }
    } else {
      pendingMatchByUser.delete(b);
    }
  }
}

function getFallbackIceServers() {
  return [{ urls: "stun:stun.l.google.com:19302" }];
}

function requestTwilioToken() {
  return new Promise((resolve) => {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
      return resolve({
        ok: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    }

    const postData = "Ttl=21600";

    const options = {
      hostname: "api.twilio.com",
      port: 443,
      path: `/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Tokens.json`,
      method: "POST",
      headers: {
        Authorization:
          "Basic " +
          Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64"),
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": Buffer.byteLength(postData),
      },
    };

    const req = https.request(options, (res) => {
      let body = "";

      res.on("data", (chunk) => {
        body += chunk.toString();
      });

      res.on("end", () => {
        try {
          const parsed = JSON.parse(body);

          if (Array.isArray(parsed.ice_servers) && parsed.ice_servers.length > 0) {
            return resolve({
              ok: true,
              iceServers: parsed.ice_servers,
              provider: "twilio",
            });
          }

          return resolve({
            ok: true,
            iceServers: getFallbackIceServers(),
            provider: "fallback_stun",
          });
        } catch (err) {
          logInfo("Twilio", "Failed to parse Twilio token response", err);
          return resolve({
            ok: true,
            iceServers: getFallbackIceServers(),
            provider: "fallback_stun",
          });
        }
      });
    });

    req.on("error", (err) => {
      logInfo("Twilio", "Twilio token request failed", err);
      return resolve({
        ok: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    });

    req.write(postData);
    req.end();
  });
}

// ==========================================
// 5) Matchmaking
// ==========================================
async function tryMatch(userId) {
  const me = normalizeId(userId);
  if (!me) return;

  if (!acquireMatchLock(me)) return;

  try {
    const mySocket = await getUserSocket(me);
    if (!mySocket) {
      await emitToUser(me, "error_msg", { message: "User is not fully connected yet" });
      return;
    }

    if (activeMatches.has(me) || pendingMatchByUser.has(me)) return;

    removeFromQueue(me);

    let partnerId = null;
    for (let i = 0; i < waitingQueue.length; i++) {
      const candidateId = waitingQueue[i];
      if (candidateId === me) continue;
      if (matchmakingLocks.has(candidateId)) continue;

      const alreadyFriends = await Friendship.findOne({
        pairKey: pairKeyByIds(me, candidateId)
      }).lean();

      if (alreadyFriends) {
        continue;
      }

      const partnerSocket = await getUserSocket(candidateId);
      if (partnerSocket && !activeMatches.has(candidateId) && !pendingMatchByUser.has(candidateId)) {
        partnerId = candidateId;
        waitingQueue.splice(i, 1);
        break;
      } else {
        waitingQueue.splice(i, 1);
        i--;
      }
    }

    if (partnerId) {
      const key = createPendingMatchEntry(me, partnerId);

      const myProfile = await getFullUserProfile(me);
      const partnerProfile = await getFullUserProfile(partnerId);

      await emitToUser(me, "match_found", { partner: partnerProfile, proposalKey: key });
      await emitToUser(partnerId, "match_found", { partner: myProfile, proposalKey: key });
    } else {
      if (!waitingQueue.includes(me)) {
        waitingQueue.push(me);
      }
      await emitToUser(me, "waiting_in_queue", {
        status: "searching",
        message: "Searching for a partner..."
      });
    }
  } catch (err) {
    logInfo("Error", `Matchmaking failure for ${me}`, err);
  } finally {
    releaseMatchLock(me);
  }
}

// ==========================================
// 6) Socket Events
// ==========================================
io.on("connection", (socket) => {
  logInfo("Network", `Socket connected: ${socket.id}`);

  socket.on("register_user", async (payload) => {
    try {
      let incomingUserId = "";
      let incomingUserName = "";

      if (typeof payload === "string") {
        incomingUserName = normalizeName(payload);
      } else if (payload && typeof payload === "object") {
        incomingUserId = normalizeId(payload.userId);
        incomingUserName = normalizeName(payload.userName || payload.name);
      }

      if (!incomingUserName) {
        socket.emit("error_msg", { message: "اسم المستخدم مطلوب" });
        return;
      }

      if (!incomingUserId) {
        incomingUserId = safeUuid();
      }

      let user = await User.findOne({ userId: incomingUserId }).lean();

      if (!user) {
        user = await User.create({
          userId: incomingUserId,
          userName: incomingUserName,
          socketId: socket.id,
          online: true,
          lastSeen: new Date()
        });
      } else {
        await User.findOneAndUpdate(
          { userId: incomingUserId },
          {
            userName: incomingUserName,
            socketId: socket.id,
            online: true,
            lastSeen: new Date()
          }
        );
      }

      socket.data.userId = incomingUserId;
      socket.data.userName = incomingUserName;

      socketToUserId.set(socket.id, incomingUserId);
      userIdToSocket.set(incomingUserId, socket.id);

      socket.emit("registration_success", {
        userId: incomingUserId,
        userName: incomingUserName,
        timestamp: new Date()
      });

      socket.emit("user_ready_for_matchmaking", {
        success: true,
        userId: incomingUserId,
        userName: incomingUserName,
        socketId: socket.id
      });

      logInfo("User", `Registered & Online`, {
        userId: incomingUserId,
        userName: incomingUserName
      });
    } catch (err) {
      logInfo("Error", "Registration process failed", err);
      socket.emit("error_msg", { message: "Registration process failed" });
    }
  });

  socket.on("update_profile", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const updatedUser = await User.findOneAndUpdate(
        { userId: me },
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
    } catch (err) {
      logInfo("Error", "Failed to update profile", err);
      socket.emit("error_msg", { message: "Failed to update profile" });
    }
  });

  socket.on("get_turn_credentials", async () => {
    try {
      const result = await requestTwilioToken();

      socket.emit("turn_credentials", {
        success: true,
        iceServers: result.iceServers,
        provider: result.provider,
      });

      logInfo("Twilio", `TURN credentials served`, {
        userId: socket.data.userId || null,
        provider: result.provider,
        count: result.iceServers.length,
      });
    } catch (err) {
      logInfo("Twilio", "Failed to provide turn credentials", err);
      socket.emit("turn_credentials", {
        success: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    }
  });

  socket.on("find_partner", async () => {
    try {
      const me = socket.data.userId;
      if (!me) {
        socket.emit("error_msg", { message: "Please register user before matchmaking" });
        return;
      }

      const readiness = await validateUserReadyForMatchmaking(me, socket);
      if (!readiness.ok) {
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
      const me = socket.data.userId;
      if (!me) return;

      const cleanMe = normalizeId(me);
      const wasInQueue = waitingQueue.includes(cleanMe);
      const hadPendingMatch = pendingMatchByUser.has(cleanMe);
      const hadActiveMatch = activeMatches.has(cleanMe);

      if (wasInQueue && !hadPendingMatch && !hadActiveMatch) {
        removeFromQueue(me);

        socket.emit("search_stopped", {
          success: true,
          mode: "queue_only"
        });

        return;
      }

      await clearUserBusyState(me, "partner_stopped_search");

      socket.emit("search_stopped", {
        success: true,
        mode: "relationship_state"
      });
    } catch (err) {
      logInfo("Error", "stop_search failed", err);
      socket.emit("error_msg", { message: "Failed to stop search" });
    }
  });

  socket.on("accept_match", async () => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const key = pendingMatchByUser.get(me);
      if (!key) return;

      const proposal = pendingMatches.get(key);
      if (!proposal) return;

      proposal.acceptedBy.add(me);
      const partnerId = proposal.userAId === me ? proposal.userBId : proposal.userAId;

      if (proposal.acceptedBy.size === 2) {
        if (proposal.timeoutId) clearTimeout(proposal.timeoutId);

        pendingMatches.delete(key);
        pendingMatchByUser.delete(proposal.userAId);
        pendingMatchByUser.delete(proposal.userBId);

        activeMatches.set(proposal.userAId, proposal.userBId);
        activeMatches.set(proposal.userBId, proposal.userAId);

        const partnerA = await getFullUserProfile(proposal.userBId);
        const partnerB = await getFullUserProfile(proposal.userAId);

        await emitToUser(proposal.userAId, "match_confirmed", { partner: partnerA });
        await emitToUser(proposal.userBId, "match_confirmed", { partner: partnerB });
      } else {
        await emitToUser(partnerId, "partner_accepted", { message: "Partner is ready" });
      }
    } catch (err) {
      logInfo("Error", "accept_match failed", err);
    }
  });

  socket.on("skip_partner", async () => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const pendingKey = pendingMatchByUser.get(me);
      if (pendingKey) {
        const proposal = pendingMatches.get(pendingKey);
        if (proposal) {
          if (proposal.timeoutId) clearTimeout(proposal.timeoutId);

          const other = proposal.userAId === me ? proposal.userBId : proposal.userAId;
          pendingMatches.delete(pendingKey);
          pendingMatchByUser.delete(me);
          pendingMatchByUser.delete(other);

          await emitToUser(other, "match_searching", {
            status: "searching",
            message: "Searching for a new partner..."
          });

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
    const me = socket.data.userId;
    const partner = activeMatches.get(me);
    const cleanText = String(msgContent || "").trim();
    if (!partner || !cleanText) return;

    try {
      const meUser = await getUserById(me);
      const partnerUser = await getUserById(partner);
      if (!meUser || !partnerUser) return;

      const msgData = {
        fromId: me,
        toId: partner,
        fromName: meUser.userName,
        toName: partnerUser.userName,
        type: "text",
        text: cleanText,
        time: new Date(),
        conversationKey: pairKeyByIds(me, partner)
      };

      await RandomChatMessage.create(msgData);
      await emitToUser(partner, "message", msgData);
    } catch (err) {
      logInfo("Error", "Random chat message failed to send", err);
    }
  });

  socket.on("typing", (isTyping) => {
    const me = socket.data.userId;
    const partner = activeMatches.get(me);
    if (!partner) return;

    const key = pairKeyByIds(me, partner);
    const oldTimeout = userTypingTimeout.get(key);
    if (oldTimeout) clearTimeout(oldTimeout);

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

  socket.on("send_friend_request", async (targetRef) => {
    const me = socket.data.userId;
    if (!me) return;

    try {
      const meUser = await getUserById(me);
      if (!meUser) return;

      const resolved = await resolveUserRef(targetRef);
      if (!resolved.ok) {
        if (resolved.reason === "ambiguous_name") {
          return socket.emit("error_msg", { message: "الاسم مكرر، يجب استخدام معرف المستخدم" });
        }
        return socket.emit("error_msg", { message: "المستخدم المطلوب غير موجود" });
      }

      const target = resolved.user;
      if (!target || me === target.userId) return;

      const alreadyFriends = await Friendship.findOne({
        pairKey: pairKeyByIds(me, target.userId)
      }).lean();

      if (alreadyFriends) {
        return socket.emit("error_msg", { message: "Already friends" });
      }

      const existingReq = await FriendRequest.findOne({
        fromId: me,
        toId: target.userId,
        status: "pending"
      }).lean();

      const reverseReq = await FriendRequest.findOne({
        fromId: target.userId,
        toId: me,
        status: "pending"
      }).lean();

      if (reverseReq) {
        return socket.emit("error_msg", {
          message: "There is already a pending request from this user"
        });
      }

      if (!existingReq) {
        await FriendRequest.create({
          fromId: me,
          toId: target.userId,
          fromName: meUser.userName,
          toName: target.userName
        });

        await emitToUser(target.userId, "new_friend_request", {
          fromId: me,
          fromName: meUser.userName
        });

        socket.emit("request_sent", {
          success: true,
          toId: target.userId,
          toName: target.userName
        });
      } else {
        socket.emit("error_msg", { message: "Friend request already pending" });
      }
    } catch (err) {
      logInfo("Error", "Friend request system failure", err);
      socket.emit("error_msg", { message: "Friend request failed" });
    }
  });

  socket.on("respond_to_request", async (data) => {
    const me = socket.data.userId;
    const accept = Boolean(data?.accept);
    if (!me) return;

    try {
      const resolved = await resolveUserRef({
        userId: data?.fromId,
        userName: data?.from
      });

      if (!resolved.ok) return;

      const fromUser = resolved.user;
      const meUser = await getUserById(me);
      if (!fromUser || !meUser) return;

      const request = await FriendRequest.findOne({
        fromId: fromUser.userId,
        toId: me,
        status: "pending"
      });

      if (!request) return;

      if (accept) {
        request.status = "accepted";
        await request.save();

        const currentPairKey = pairKeyByIds(fromUser.userId, me);

        const existingFriendship = await Friendship.findOne({ pairKey: currentPairKey }).lean();
        if (!existingFriendship) {
          await Friendship.create({
            userAId: fromUser.userId,
            userBId: me,
            pairKey: currentPairKey,
            userAName: fromUser.userName,
            userBName: meUser.userName
          });
        }

        await emitToUser(fromUser.userId, "friend_request_accepted", {
          byId: me,
          byName: meUser.userName
        });

        await emitToUser(me, "friend_added_successfully", {
          userId: fromUser.userId,
          userName: fromUser.userName
        });

        await emitToUser(fromUser.userId, "friend_added_successfully", {
          userId: me,
          userName: meUser.userName
        });

        await forceRefreshUserSocketState(me);
        await forceRefreshUserSocketState(fromUser.userId);
      } else {
        request.status = "rejected";
        await request.save();

        await emitToUser(fromUser.userId, "friend_request_rejected", {
          byId: me,
          byName: meUser.userName
        });
      }
    } catch (err) {
      logInfo("Error", "Friend response failed", err);
      socket.emit("error_msg", { message: "Failed to respond to friend request" });
    }
  });

  socket.on("get_friends_status", async (friends) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const list = Array.isArray(friends) ? friends : [];
      for (const item of list) {
        const resolved = await resolveUserRef(item);
        if (!resolved.ok) continue;

        const status = await getUserStatusSummary(resolved.user.userId);
        socket.emit("update_status", status);
      }
    } catch (err) {
      logInfo("Error", "Failed to get friends status", err);
      socket.emit("error_msg", { message: "Failed to get friends status" });
    }
  });

  socket.on("delete_friend", async (payload) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef(payload);
      if (!resolved.ok) return;

      const friend = resolved.user;
      if (!friend || me === friend.userId) return;

      const currentPairKey = pairKeyByIds(me, friend.userId);

      await Friendship.deleteOne({ pairKey: currentPairKey });

      await FriendRequest.deleteMany({
        $or: [
          { fromId: me, toId: friend.userId },
          { fromId: friend.userId, toId: me }
        ]
      });

      await PrivateMessage.deleteMany({
        conversationKey: currentPairKey
      });

      await clearRelationshipRuntimeState(me, friend.userId);

      socket.emit("friend_deleted_successfully", {
        friendId: friend.userId,
        friendName: friend.userName,
        message: `Removed ${friend.userName} from friends`
      });

      await emitToUser(friend.userId, "friend_deleted_me", {
        fromId: me,
        message: `User removed you from friends`
      });

      await forceRefreshUserSocketState(me);
      await forceRefreshUserSocketState(friend.userId);
    } catch (err) {
      logInfo("Error", "Delete friend failed", err);
      socket.emit("error_msg", { message: "Failed to delete friend" });
    }
  });

  socket.on("get_private_history", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) {
        return socket.emit("private_history", []);
      }

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to || data?.friend || data?.with || data?.userName
      });

      if (!resolved.ok) {
        return socket.emit("private_history", []);
      }

      const other = resolved.user;
      if (!other) {
        return socket.emit("private_history", []);
      }

      const isFriend = await Friendship.findOne({
        pairKey: pairKeyByIds(me, other.userId)
      }).lean();

      if (!isFriend) {
        return socket.emit("private_history", []);
      }

      const messages = await PrivateMessage.find({
        conversationKey: pairKeyByIds(me, other.userId),
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
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.friendId || data?.fromId || data?.toId,
        userName: data?.friend || data?.from || data?.to
      });

      if (!resolved.ok) return;

      const friend = resolved.user;
      if (!friend) return;

      await PrivateMessage.updateMany(
        {
          conversationKey: pairKeyByIds(me, friend.userId),
          toId: me,
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
    const me = socket.data.userId;
    if (!me) return;

    try {
      const meUser = await getUserById(me);
      if (!meUser) return;

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to
      });

      const cleanText = String(data?.text || "").trim();

      if (!resolved.ok || !cleanText) return;

      const toUser = resolved.user;
      if (!toUser) return;

      const isFriend = await Friendship.findOne({
        pairKey: pairKeyByIds(me, toUser.userId)
      }).lean();

      if (!isFriend) {
        return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const msg = await PrivateMessage.create({
        fromId: me,
        toId: toUser.userId,
        fromName: meUser.userName,
        toName: toUser.userName,
        text: cleanText,
        conversationKey: pairKeyByIds(me, toUser.userId),
        time: new Date(),
        readBy: [me]
      });

      const plainMsg = msg.toObject ? msg.toObject() : msg;

      const delivered = await emitToUser(toUser.userId, "private_message_received", plainMsg);

      socket.emit("pm_sent_success", {
        msgId: msg._id,
        delivered: delivered === true
      });
    } catch (err) {
      logInfo("Error", "Private message system failure", err);
      socket.emit("error_msg", { message: "Private message failed" });
    }
  });

  socket.on("start_private_call", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to
      });

      if (!resolved.ok) {
        return socket.emit("error_msg", { message: "Target user not found" });
      }

      const toUser = resolved.user;
      if (!toUser || me === toUser.userId) return;

      const isFriend = await Friendship.findOne({
        pairKey: pairKeyByIds(me, toUser.userId)
      }).lean();

      if (!isFriend) {
        return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const targetSocket = await getUserSocket(toUser.userId);
      if (!targetSocket) {
        return socket.emit("call_offline", {
          toId: toUser.userId,
          toName: toUser.userName
        });
      }

      if (isUserBusyForCall(me) || isUserBusyForCall(toUser.userId)) {
        return socket.emit("call_busy", {
          toId: toUser.userId,
          toName: toUser.userName
        });
      }

      createPendingCall(me, toUser.userId);

      const meUser = await getUserById(me);

      await emitToUser(toUser.userId, "incoming_call", {
        fromId: me,
        from: meUser?.userName || "",
      });

      socket.emit("call_ringing", {
        toId: toUser.userId,
        toName: toUser.userName
      });

      logInfo("Call", `Outgoing call`, {
        fromId: me,
        toId: toUser.userId
      });
    } catch (err) {
      logInfo("Error", "start_private_call failed", err);
      socket.emit("error_msg", { message: "Failed to start call" });
    }
  });

  socket.on("accept_private_call", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.fromId || data?.friendId,
        userName: data?.from
      });

      if (!resolved.ok) return;

      const fromUser = resolved.user;
      if (!fromUser) return;

      const key = pendingCallByUser.get(me);
      if (!key) return;

      const pending = pendingCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.callerId === fromUser.userId && pending.calleeId === me) ||
        (pending.callerId === me && pending.calleeId === fromUser.userId);

      if (!validPair) return;

      if (pending.timeoutId) clearTimeout(pending.timeoutId);

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.callerId);
      pendingCallByUser.delete(pending.calleeId);

      activeCalls.set(pending.callerId, pending.calleeId);
      activeCalls.set(pending.calleeId, pending.callerId);

      const calleeProfile = await getFullUserProfile(pending.calleeId);
      const callerProfile = await getFullUserProfile(pending.callerId);

      await emitToUser(pending.callerId, "call_accepted", {
        byId: pending.calleeId,
        byName: calleeProfile?.userName || ""
      });

      await emitToUser(pending.callerId, "call_connected", { with: calleeProfile });
      await emitToUser(pending.calleeId, "call_connected", { with: callerProfile });

      logInfo("Call", `Call connected`, {
        callerId: pending.callerId,
        calleeId: pending.calleeId
      });
    } catch (err) {
      logInfo("Error", "accept_private_call failed", err);
    }
  });

  socket.on("reject_private_call", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.fromId || data?.friendId,
        userName: data?.from
      });

      if (!resolved.ok) return;

      const fromUser = resolved.user;
      if (!fromUser) return;

      const key = pendingCallByUser.get(me);
      if (!key) return;

      const pending = pendingCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.callerId === fromUser.userId && pending.calleeId === me) ||
        (pending.callerId === me && pending.calleeId === fromUser.userId);

      if (!validPair) return;

      if (pending.timeoutId) clearTimeout(pending.timeoutId);

      pendingCalls.delete(key);
      pendingCallByUser.delete(pending.callerId);
      pendingCallByUser.delete(pending.calleeId);

      await emitToUser(fromUser.userId, "call_rejected", { byId: me });
      await emitToUser(me, "call_ended", { reason: "rejected" });
    } catch (err) {
      logInfo("Error", "reject_private_call failed", err);
    }
  });

  socket.on("end_private_call", async () => {
    try {
      const me = socket.data.userId;
      if (!me) return;
      await clearCallStateForUser(me, "ended");
    } catch (err) {
      logInfo("Error", "end_private_call failed", err);
    }
  });

  socket.on("webrtc_offer", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to
      });

      if (!resolved.ok) return;

      const toUser = resolved.user;
      const sdp = data?.sdp;
      const type = data?.type;

      if (!toUser || !sdp || !type) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== toUser.userId) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(toUser.userId, "webrtc_offer", {
        fromId: me,
        sdp,
        type,
      });

      logInfo("WebRTC", `Offer relayed`, { fromId: me, toId: toUser.userId });
    } catch (err) {
      logInfo("Error", "webrtc_offer failed", err);
      socket.emit("error_msg", { message: "Failed to relay offer" });
    }
  });

  socket.on("webrtc_answer", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to
      });

      if (!resolved.ok) return;

      const toUser = resolved.user;
      const sdp = data?.sdp;
      const type = data?.type;

      if (!toUser || !sdp || !type) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== toUser.userId) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(toUser.userId, "webrtc_answer", {
        fromId: me,
        sdp,
        type,
      });

      logInfo("WebRTC", `Answer relayed`, { fromId: me, toId: toUser.userId });
    } catch (err) {
      logInfo("Error", "webrtc_answer failed", err);
      socket.emit("error_msg", { message: "Failed to relay answer" });
    }
  });

  socket.on("webrtc_ice_candidate", async (data) => {
    try {
      const me = socket.data.userId;
      if (!me) return;

      const resolved = await resolveUserRef({
        userId: data?.toId || data?.friendId,
        userName: data?.to
      });

      if (!resolved.ok) return;

      const toUser = resolved.user;
      const candidate = data?.candidate;

      if (!toUser || !candidate) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== toUser.userId) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(toUser.userId, "webrtc_ice_candidate", {
        fromId: me,
        candidate,
      });

      logInfo("WebRTC", `ICE candidate relayed`, { fromId: me, toId: toUser.userId });
    } catch (err) {
      logInfo("Error", "webrtc_ice_candidate failed", err);
      socket.emit("error_msg", { message: "Failed to relay ICE candidate" });
    }
  });

  socket.on("disconnect", async () => {
    const me = socket.data.userId;
    logInfo("Network", `Socket disconnected`, {
      socketId: socket.id,
      userId: me || null
    });

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
          if (prop.timeoutId) clearTimeout(prop.timeoutId);
          const other = prop.userAId === me ? prop.userBId : prop.userAId;
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

      const currentSocketId = userIdToSocket.get(me);
      if (currentSocketId === socket.id) {
        userIdToSocket.delete(me);
        await User.findOneAndUpdate(
          { userId: me },
          {
            online: false,
            lastSeen: new Date(),
            socketId: null
          }
        );
      }
    }

    socketToUserId.delete(socket.id);
  });
});

// ==========================================
// 7) API
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
      onlineUsers: userIdToSocket.size,
      queueSize: waitingQueue.length,
      ongoingChats: activeMatches.size / 2,
      pendingProposals: pendingMatches.size,
      activeCalls: activeCalls.size / 2,
      pendingCalls: pendingCalls.size
    },
    system: {
      uptime: process.uptime(),
      memory: process.memoryUsage().heapUsed / 1024 / 1024 + " MB",
      platform: process.platform
    }
  });
});

// ==========================================
// 8) Start Server
// ==========================================
async function startMasterServer() {
  try {
    logInfo("System", "Initializing database connection...");
    await mongoose.connect(DATABASE_URL, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 10000
    });
    logInfo("System", "Database connection established.");

    if (TWILIO_ACCOUNT_SID && TWILIO_AUTH_TOKEN) {
      logInfo("Twilio", "Twilio credentials detected. TURN/STUN via Twilio is enabled.");
    } else {
      logInfo("Twilio", "Twilio credentials not found. Using fallback STUN only.");
    }

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

process.on("unhandledRejection", (reason) => {
  logInfo("Critical", "Unhandled Promise Rejection detected", reason);
});

process.on("uncaughtException", (err) => {
  logInfo("Critical", "Uncaught Exception detected! System remains stable.", err);
});
