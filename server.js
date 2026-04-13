// ==========================================
// 1. استدعاء المكتبات وإعدادات البيئة
// ==========================================
const fs = require("fs");
const path = require("path");
const https = require("https");
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
// 2. تعريف مخططات قاعدة البيانات
// ==========================================

const userSchema = new mongoose.Schema(
  {
    userId: {
      type: String,
      unique: true,
      sparse: true,
      trim: true,
      index: true,
      lowercase: true
    },
    userName: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      index: true,
      lowercase: true
    },
    displayName: { type: String, default: "", trim: true, index: true },
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
// 3. إدارة الحالة في الذاكرة
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
// 4. دوال مساعدة
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

function normalizeDisplayName(value) {
  if (!value) return "";
  return String(value).trim();
}

function normalizeClientId(value) {
  if (!value) return "";
  return String(value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "");
}

function normalizeSecretValue(value) {
  if (!value) return "";
  return String(value).trim().replace(/^['"]+|['"]+$/g, "");
}

function buildStableUserId(clientId, fallbackName) {
  const cleanClientId = normalizeClientId(clientId);
  if (cleanClientId) {
    return normalizeName(`uid_${cleanClientId}`);
  }
  return normalizeName(fallbackName);
}

function parseRegistrationPayload(rawPayload) {
  if (rawPayload && typeof rawPayload === "object") {
    const displayName = normalizeDisplayName(
      rawPayload.userName ||
      rawPayload.displayName ||
      rawPayload.name
    );

    const clientId = normalizeClientId(
      rawPayload.clientId ||
      rawPayload.deviceId ||
      rawPayload.accountId
    );

    return {
      displayName,
      clientId
    };
  }

  return {
    displayName: normalizeDisplayName(rawPayload),
    clientId: ""
  };
}

function publicDisplayName(userDoc) {
  const display = normalizeDisplayName(userDoc?.displayName);
  if (display) return display;
  return normalizeDisplayName(userDoc?.userName);
}

function publicUserId(userDoc) {
  return normalizeName(userDoc?.userId || userDoc?.userName);
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
      userToSocket.delete(cleanName);
    }

    const user = await User.findOne({
      $or: [
        { userName: cleanName },
        { userId: cleanName }
      ]
    }).select("socketId userName userId").lean();
    if (!user || !user.socketId) return null;

    const socket = io.sockets.sockets.get(user.socketId);
    if (!socket) return null;

    const canonicalId = publicUserId(user);
    if (canonicalId) {
      userToSocket.set(canonicalId, user.socketId);
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
    const user = await User.findOne({
      $or: [
        { userName: cleanName },
        { userId: cleanName }
      ]
    }).lean();
    if (!user) return null;

    const canonicalId = publicUserId(user);
    const displayName = publicDisplayName(user);

    return {
      userId: canonicalId,
      userName: displayName || canonicalId,
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

async function resolveUserByAnyIdentifier(userName) {
  try {
    const cleanName = normalizeName(userName);
    const cleanDisplayName = normalizeDisplayName(userName);

    if (!cleanName && !cleanDisplayName) return null;

    let user = await User.findOne({
      $or: [
        { userName: cleanName },
        { userId: cleanName }
      ]
    })
      .select("userName userId displayName online lastSeen")
      .lean();

    if (user) return user;

    if (cleanDisplayName) {
      user = await User.findOne({
        displayName: cleanDisplayName
      })
        .select("userName userId displayName online lastSeen")
        .lean();
    }

    if (user) return user;

    if (cleanDisplayName) {
      user = await User.findOne({
        displayName: { $regex: `^${cleanDisplayName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, $options: "i" }
      })
        .select("userName userId displayName online lastSeen")
        .lean();
    }

    return user || null;
  } catch (err) {
    logInfo("DB", `Error resolving user by identifier for ${userName}`, err);
    return null;
  }
}

async function getUserStatusSummary(userName) {
  try {
    const cleanName = normalizeName(userName);
    const user = await resolveUserByAnyIdentifier(userName);

    if (!user) {
      return {
        user: cleanName,
        userName: normalizeDisplayName(userName) || cleanName,
        online: false,
        lastSeen: null
      };
    }

    const canonicalId = publicUserId(user);
    const displayName = publicDisplayName(user);

    return {
      user: canonicalId,
      userName: displayName || canonicalId,
      online: user.online === true,
      lastSeen: user.lastSeen || null
    };
  } catch (err) {
    logInfo("DB", `Error fetching status summary for ${userName}`, err);
    return {
      user: normalizeName(userName),
      userName: normalizeDisplayName(userName) || normalizeName(userName),
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
      rawValue.toId ||
      rawValue.userId ||
      rawValue.friendId ||
      rawValue.to ||
      rawValue.target ||
      rawValue.friend ||
      rawValue.userName ||
      rawValue.name ||
      rawValue.fromId
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
      {
        $or: [
          { userName: cleanName },
          { userId: cleanName }
        ]
      },
      {
        $set: updateData,
        $setOnInsert: {
          userName: cleanName,
          userId: cleanName,
          displayName: cleanName
        }
      },
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

    const dbUser = await User.findOne({
      $or: [
        { userName: me },
        { userId: me }
      ]
    }).select("socketId online").lean();

    if (!dbUser) {
      await ensureUserRegistrationState(me, socket.id);
      return { ok: true };
    }

    if (dbUser.socketId !== socket.id || dbUser.online !== true) {
      await ensureUserRegistrationState(me, socket.id);
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

function getFallbackIceServers() {
  return [{ urls: "stun:stun.l.google.com:19302" }];
}

function requestTwilioToken() {
  return new Promise((resolve) => {
    const sidNormalized = normalizeSecretValue(TWILIO_ACCOUNT_SID);
    const tokenNormalized = normalizeSecretValue(TWILIO_AUTH_TOKEN);

    if (!sidNormalized || !tokenNormalized) {
      logInfo("Twilio", "Twilio credentials are missing in runtime env", {
        sidPresent: Boolean(sidNormalized),
        tokenPresent: Boolean(tokenNormalized)
      });
      return resolve({
        ok: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    }

    if (!sidNormalized.startsWith("AC")) {
      logInfo("Twilio", "TWILIO_ACCOUNT_SID format is invalid for token API", {
        expectedPrefix: "AC",
        actualPrefix: sidNormalized.slice(0, 2)
      });
      return resolve({
        ok: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    }

    const normalizedPostData = "Ttl=21600";
    const normalizedOptions = {
      hostname: "api.twilio.com",
      port: 443,
      path: `/2010-04-01/Accounts/${sidNormalized}/Tokens.json`,
      method: "POST",
      headers: {
        "Authorization":
          "Basic " +
          Buffer.from(`${sidNormalized}:${tokenNormalized}`).toString("base64"),
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": Buffer.byteLength(normalizedPostData),
      },
    };

    const normalizedReq = https.request(normalizedOptions, (res) => {
      let body = "";

      res.on("data", (chunk) => {
        body += chunk.toString();
      });

      res.on("end", () => {
        if (!res.statusCode || res.statusCode < 200 || res.statusCode >= 300) {
          logInfo("Twilio", "Twilio token request returned non-success status", {
            statusCode: res.statusCode,
            bodyPreview: body.slice(0, 500)
          });
          return resolve({
            ok: true,
            iceServers: getFallbackIceServers(),
            provider: "fallback_stun",
          });
        }

        try {
          const parsed = JSON.parse(body);

          if (Array.isArray(parsed.ice_servers) && parsed.ice_servers.length > 0) {
            return resolve({
              ok: true,
              iceServers: parsed.ice_servers,
              provider: "twilio",
            });
          }

          logInfo("Twilio", "Twilio response has no usable ice_servers", {
            statusCode: res.statusCode,
            bodyPreview: body.slice(0, 500)
          });
          return resolve({
            ok: true,
            iceServers: getFallbackIceServers(),
            provider: "fallback_stun",
          });
        } catch (err) {
          logInfo("Twilio", "Failed to parse Twilio token response", {
            statusCode: res.statusCode,
            bodyPreview: body.slice(0, 500)
          });
          return resolve({
            ok: true,
            iceServers: getFallbackIceServers(),
            provider: "fallback_stun",
          });
        }
      });
    });

    normalizedReq.on("error", (err) => {
      logInfo("Twilio", "Twilio token request failed", err);
      return resolve({
        ok: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    });

    normalizedReq.write(normalizedPostData);
    normalizedReq.end();
  });
}

// ==========================================
// 5. منطق المطابقة
// ==========================================

async function tryMatch(userName) {
  const me = normalizeName(userName);
  if (!me) return;

  if (!acquireMatchLock(me)) {
    return;
  }

  try {
    const mySocket = await getUserSocket(me);
    if (!mySocket) {
      await emitToUser(me, "error_msg", { message: "User is not fully connected yet" });
      return;
    }

    if (activeMatches.has(me) || pendingMatchByUser.has(me)) {
      return;
    }

    removeFromQueue(me);

    let partner = null;
    for (let i = 0; i < waitingQueue.length; i++) {
      const candidate = waitingQueue[i];
      if (candidate === me) continue;

      if (matchmakingLocks.has(candidate)) continue;

      const alreadyFriends = await Friendship.findOne({
        pairKey: pairKey(me, candidate)
      }).lean();

      if (alreadyFriends) continue;

      const partnerSocket = await getUserSocket(candidate);
      if (partnerSocket && !activeMatches.has(candidate) && !pendingMatchByUser.has(candidate)) {
        partner = candidate;
        waitingQueue.splice(i, 1);
        break;
      } else {
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
// 6. أحداث السوكيت
// ==========================================

io.on("connection", (socket) => {
  logInfo("Network", `Socket connected: ${socket.id}`);

  socket.on("register_user", async (rawName) => {
    try {
      const registration = parseRegistrationPayload(rawName);
      const displayName = registration.displayName;
      if (!displayName) return;

      const userName = buildStableUserId(registration.clientId, displayName);
      if (!userName) return;

      const existingUser = await User.findOne({
        $or: [
          { userName },
          { userId: userName }
        ]
      }).select("socketId").lean();
      if (existingUser?.socketId && existingUser.socketId !== socket.id) {
        const oldSocket = io.sockets.sockets.get(existingUser.socketId);
        if (oldSocket) {
          oldSocket.emit("session_replaced", { message: "Logged in from another device" });
          oldSocket.disconnect(true);
        }
        socketToUser.delete(existingUser.socketId);
      }

      socket.data.userName = userName;
      socket.data.userId = userName;
      socket.data.displayName = displayName;
      socketToUser.set(socket.id, userName);
      userToSocket.set(userName, socket.id);

      const updatedUser = await User.findOneAndUpdate(
        {
          $or: [
            { userName },
            { userId: userName }
          ]
        },
        {
          $set: {
            userName,
            userId: userName,
            displayName,
            socketId: socket.id,
            online: true,
            lastSeen: new Date()
          },
          $setOnInsert: {
            profileImage: "",
            country: "",
            age: null,
            bio: "",
            gender: "unspecified"
          }
        },
        { upsert: true, returnDocument: "after" }
      );

      // --- دمج حل "آخر ظهور": إبلاغ الأصدقاء بالدخول ---
      const friends = await Friendship.find({
        $or: [{ userA: userName }, { userB: userName }]
      });
      friends.forEach(f => {
        const target = f.userA === userName ? f.userB : f.userA;
        emitToUser(target, "update_status", {
          userName: userName,
          online: true,
          lastSeen: updatedUser.lastSeen
        });
      });
      // ------------------------------------------

      socket.emit("registration_success", {
        userName: displayName,
        displayName,
        userId: userName,
        canonicalUserName: userName,
        timestamp: new Date()
      });
      socket.emit("user_ready_for_matchmaking", {
        success: true,
        userName: displayName,
        userId: userName,
        socketId: socket.id
      });
    } catch (err) {
      logInfo("Error", "Registration process failed", err);
      socket.emit("error_msg", { message: "Registration process failed" });
    }
  });

  socket.on("update_profile", async (data) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedDisplayName = normalizeDisplayName(data?.userName);
      const updateFields = {
        profileImage: data?.profileImage ?? "",
        country: data?.country ?? "",
        age: data?.age ?? null,
        bio: data?.bio ?? "",
        gender: data?.gender ?? "unspecified",
        lastSeen: new Date(),
      };

      if (requestedDisplayName) {
        updateFields.displayName = requestedDisplayName;
      }

      const updatedUser = await User.findOneAndUpdate(
        {
          $or: [
            { userName: me },
            { userId: me }
          ]
        },
        updateFields,
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

      logInfo("Twilio", `TURN credentials served to ${socket.data.userName || socket.id}`, {
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

  // --- دمج حل "آخر ظهور": معالجة انقطاع الاتصال ---
  socket.on("disconnect", async () => {
    const userName = socket.data.userName;
    if (userName) {
      const now = new Date();
      await User.findOneAndUpdate({ userName }, { online: false, lastSeen: now });
      
      const friends = await Friendship.find({
        $or: [{ userA: userName }, { userB: userName }]
      });
      friends.forEach(f => {
        const target = f.userA === userName ? f.userB : f.userA;
        emitToUser(target, "update_status", {
          userName: userName,
          online: false,
          lastSeen: now
        });
      });
      
      userToSocket.delete(userName);
      socketToUser.delete(socket.id);
      await clearUserBusyState(userName, "disconnected");
      await clearCallStateForUser(userName, "disconnected");
    }
    logInfo("Network", `Socket disconnected: ${socket.id}`);
  });
  // ------------------------------------------

  // ... (بقية الأحداث الأصلية الخاصة بك كما هي)
});

// ==========================================
// 7. تشغيل الخادم
// ==========================================

mongoose.connect(DATABASE_URL).then(() => {
  logInfo("DB", "Connected to MongoDB");
  server.listen(PORT, () => {
    logInfo("App", `Server listening on port ${PORT}`);
  });
}).catch(err => {
  console.error("MongoDB connection error:", err);
});
