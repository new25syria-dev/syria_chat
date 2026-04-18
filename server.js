// ==========================================
// 1. استدعاء المكتبات وإعدادات البيئة
// ==========================================
const fs = require("fs");
const path = require("path");
const https = require("https");
const dotenv = require("dotenv");
const express = require("express");
const http = require("http");
const cors = require("cors");
const mongoose = require("mongoose");
const { Server } = require("socket.io");

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

const PORT = Number(process.env.PORT || 10000);
const DATABASE_URL = process.env.DATABASE_URL || process.env.MONGO_URI || "";

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

const ALLOWED_ORIGINS = String(process.env.ALLOWED_ORIGINS || "*")
  .split(",")
  .map((v) => v.trim())
  .filter(Boolean);

const app = express();

const corsOptions = {
  origin(origin, callback) {
    if (ALLOWED_ORIGINS.includes("*")) {
      return callback(null, true);
    }
    if (!origin) {
      return callback(null, true);
    }
    if (ALLOWED_ORIGINS.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error("Not allowed by CORS"));
  },
  methods: ["GET", "POST"],
  credentials: !ALLOWED_ORIGINS.includes("*"),
};

app.use(cors(corsOptions));
app.use(express.json({ limit: "8mb" }));

const server = http.createServer(app);

const io = new Server(server, {
  pingTimeout: 15000,
  pingInterval: 8000,
  connectTimeout: 45000,
  allowEIO3: true,
  cors: {
    origin: ALLOWED_ORIGINS.includes("*") ? true : ALLOWED_ORIGINS,
    methods: ["GET", "POST"],
    credentials: !ALLOWED_ORIGINS.includes("*"),
  },
});

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
      lowercase: true,
    },
    userName: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      index: true,
      lowercase: true,
    },
    displayName: { type: String, default: "", trim: true, index: true },

    // ✅ تمت الإضافة هنا
    deviceId: {
      type: String,
      default: "",
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
    reports: { type: Number, default: 0 },
  },
  { timestamps: true }
);

const friendshipSchema = new mongoose.Schema(
  {
    userA: { type: String, required: true, trim: true, index: true },
    userB: { type: String, required: true, trim: true, index: true },
    pairKey: { type: String, required: true, unique: true, index: true },
    friendsSince: { type: Date, default: Date.now },
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
      index: true,
    },
    sentAt: { type: Date, default: Date.now },
  },
  { timestamps: true }
);
friendRequestSchema.index({ from: 1, to: 1, status: 1 });

const privateMessageSchema = new mongoose.Schema(
  {
    from: { type: String, required: true, trim: true, index: true },
    to: { type: String, required: true, trim: true, index: true },

    text: { type: String, default: "", trim: true },

    type: {
      type: String,
      enum: ["text", "image", "voice"],
      default: "text",
      index: true
    },
    image: { type: String, default: "" },
    audio: { type: String, default: "" },
    durationSeconds: { type: Number, default: 0 },

    imageUrl: { type: String, default: "" },
    voiceUrl: { type: String, default: "" },
    messageType: { type: String, default: "text" },

    time: { type: Date, default: Date.now, index: true },
    conversationKey: { type: String, required: true, index: true },
    readBy: { type: [String], default: [] },
    isDeleted: { type: Boolean, default: false },
  },
  { timestamps: true }
);
privateMessageSchema.index({ conversationKey: 1, time: 1 });

const randomChatMessageSchema = new mongoose.Schema(
  {
    from: { type: String, required: true, trim: true, index: true },
    to: { type: String, required: true, trim: true, index: true },
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
// 3. إدارة الحالة في الذاكرة
// ==========================================
const socketToUser = new Map();
const waitingQueues = {
  text: [],
  voice: []
};
const activeMatches = new Map();
const pendingMatches = new Map();
const pendingMatchByUser = new Map();
const userTypingTimeout = new Map();
const userSearchPreferences = new Map();
const userSearchLocks = new Set();

const userToSocket = new Map();
const matchmakingLocks = new Set();
const MATCH_PROPOSAL_TTL = 30000;

const activeCalls = new Map();
const pendingCalls = new Map();
const pendingCallByUser = new Map();
const CALL_RING_TIMEOUT = 30000;

const activeRandomCalls = new Map();
const pendingRandomCalls = new Map();
const pendingRandomCallByUser = new Map();

const voiceMatchGrace = new Map();
const VOICE_MATCH_GRACE_MS = 20000;

// ==========================================
// 4. دوال مساعدة
// ==========================================

function logInfo(scope, message, extra = undefined) {
  const prefix = `[${new Date().toISOString()}] [${scope.toUpperCase()}]`;
  if (extra === undefined) {
    console.log(`${prefix} ${message}`);
  } else {
    try {
      console.log(`${prefix} ${message}`, JSON.stringify(extra, null, 2));
    } catch {
      console.log(`${prefix} ${message}`, extra);
    }
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

function normalizeChatType(value) {
  const raw = String(value || "").trim().toLowerCase();

  if (
    raw === "voice" ||
    raw === "audio" ||
    raw === "call" ||
    raw === "audio_chat"
  ) {
    return "voice";
  }

  return "text";
}

function getUserPreferredChatType(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return "text";
  return normalizeChatType(userSearchPreferences.get(cleanName));
}

function setUserPreferredChatType(userName, chatType, socket = null) {
  const cleanName = normalizeName(userName);
  const safeChatType = normalizeChatType(chatType);

  if (!cleanName) return safeChatType;

  userSearchPreferences.set(cleanName, safeChatType);

  if (socket) {
    socket.data.chatType = safeChatType;
  }

  return safeChatType;
}

function lockUserSearch(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;
  userSearchLocks.add(cleanName);
}

function unlockUserSearch(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;
  userSearchLocks.delete(cleanName);
}

function isUserLocked(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;
  return userSearchLocks.has(cleanName);
}

function setVoiceMatchGrace(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;
  voiceMatchGrace.set(cleanName, Date.now() + VOICE_MATCH_GRACE_MS);
}

function clearVoiceMatchGrace(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;
  voiceMatchGrace.delete(cleanName);
}

function hasVoiceMatchGrace(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;

  const expiresAt = voiceMatchGrace.get(cleanName);
  if (!expiresAt) return false;

  if (Date.now() > expiresAt) {
    voiceMatchGrace.delete(cleanName);
    return false;
  }

  return true;
}

function sanitizeText(value, maxLength = 2000) {
  if (value === undefined || value === null) return "";
  return String(value).trim().slice(0, maxLength);
}

function sanitizeUrl(value, maxLength = 4000) {
  if (!value) return "";
  return String(value).trim().slice(0, maxLength);
}

function sanitizeOptionalString(value, maxLength = 300) {
  if (value === undefined || value === null) return "";
  return String(value).trim().slice(0, maxLength);
}

function sanitizeAge(value) {
  if (value === undefined || value === null || value === "") return null;
  const age = Number(value);
  if (!Number.isFinite(age)) return null;
  if (age < 0 || age > 120) return null;
  return Math.floor(age);
}

function sanitizeProfileImage(value, maxLength = 2000000) {
  if (!value) return "";
  let clean = String(value).trim();

  if (!clean) return "";

  const lower = clean.toLowerCase();
  if (lower.startsWith("http://") || lower.startsWith("https://")) {
    return clean.slice(0, maxLength);
  }

  if (clean.startsWith("data:image")) {
    const commaIndex = clean.indexOf(",");
    if (commaIndex !== -1 && commaIndex + 1 < clean.length) {
      clean = clean.substring(commaIndex + 1).trim();
    }
  }

  clean = clean.replace(/\s+/g, "");

  if (!clean) return "";
  return clean.slice(0, maxLength);
}

function sanitizeAudioPayload(value, maxLength = 6000000) {
  if (!value) return "";
  let clean = String(value).trim();

  if (!clean) return "";

  if (clean.startsWith("data:audio")) {
    const commaIndex = clean.indexOf(",");
    if (commaIndex !== -1 && commaIndex + 1 < clean.length) {
      clean = clean.substring(commaIndex + 1).trim();
    }
  }

  clean = clean.replace(/\s+/g, "");
  if (!clean) return "";

  return clean.slice(0, maxLength);
}

function sanitizeDurationSeconds(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 0;
  if (parsed < 0) return 0;
  if (parsed > 60) return 60;
  return Math.floor(parsed);
}

function buildStableUserId(clientId) {
  const cleanClientId = normalizeClientId(clientId);
  if (!cleanClientId) return "";
  return normalizeName(`uid_${cleanClientId}`);
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

function getQueueByChatType(chatType) {
  return waitingQueues[normalizeChatType(chatType)] || waitingQueues.text;
}

function getAllQueueSizes() {
  return Object.values(waitingQueues).reduce((sum, queue) => sum + queue.length, 0);
}

function isUserBusyForCall(userName) {
  const cleanName = normalizeName(userName);
  return (
    activeCalls.has(cleanName) ||
    pendingCallByUser.has(cleanName) ||
    activeRandomCalls.has(cleanName) ||
    pendingRandomCallByUser.has(cleanName)
  );
}

function isUserUnavailableForMatch(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return true;

  return (
    activeMatches.has(cleanName) ||
    pendingMatchByUser.has(cleanName) ||
    isUserBusyForCall(cleanName) ||
    isUserLocked(cleanName)
  );
}

function isUserInVoiceTransition(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;
  if (!hasVoiceMatchGrace(cleanName)) return false;
  if (!activeMatches.has(cleanName)) return false;
  if (activeRandomCalls.has(cleanName) || pendingRandomCallByUser.has(cleanName)) {
    return false;
  }
  return getUserPreferredChatType(cleanName) === "voice";
}

function removeFromQueue(userName, chatType = null) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;

  const targetTypes = chatType
    ? [normalizeChatType(chatType)]
    : Object.keys(waitingQueues);

  let removed = false;

  for (const type of targetTypes) {
    const queue = getQueueByChatType(type);
    for (let i = queue.length - 1; i >= 0; i--) {
      if (queue[i] === cleanName) {
        queue.splice(i, 1);
        removed = true;
      }
    }
  }

  if (removed) {
    logInfo("Queue", `User ${cleanName} removed from waiting list.`, {
      chatType: chatType ? normalizeChatType(chatType) : "all",
      textQueueSize: waitingQueues.text.length,
      voiceQueueSize: waitingQueues.voice.length
    });
  }

  return removed;
}

function isUserInQueue(userName, chatType = null) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return false;

  if (chatType) {
    return getQueueByChatType(chatType).includes(cleanName);
  }

  return Object.values(waitingQueues).some((queue) => queue.includes(cleanName));
}

function addToQueue(userName, chatType = "text") {
  const cleanName = normalizeName(userName);
  const safeChatType = normalizeChatType(chatType);

  if (!cleanName) return false;
  if (isUserUnavailableForMatch(cleanName)) return false;
  if (isUserInVoiceTransition(cleanName)) return false;
  if (isUserInQueue(cleanName)) return false;

  getQueueByChatType(safeChatType).push(cleanName);

  logInfo("Queue", `User ${cleanName} added to waiting list.`, {
    chatType: safeChatType,
    textQueueSize: waitingQueues.text.length,
    voiceQueueSize: waitingQueues.voice.length
  });

  return true;
}

async function getUserSocket(userName) {
  try {
    const cleanName = normalizeName(userName);
    if (!cleanName) return null;

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
    if (!cleanName) return null;

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
      profileImage: sanitizeProfileImage(user.profileImage),
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
      .select("userName userId displayName online lastSeen socketId profileImage")
      .lean();

    if (user) return user;

    if (cleanDisplayName) {
      user = await User.findOne({
        displayName: cleanDisplayName
      })
        .select("userName userId displayName online lastSeen socketId profileImage")
        .lean();
    }

    if (user) return user;

    if (cleanDisplayName) {
      user = await User.findOne({
        displayName: {
          $regex: `^${cleanDisplayName.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`,
          $options: "i"
        }
      })
        .select("userName userId displayName online lastSeen socketId profileImage")
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
        userId: cleanName,
        friendId: cleanName,
        userName: normalizeDisplayName(userName) || cleanName,
        displayName: normalizeDisplayName(userName) || cleanName,
        profileImage: "",
        online: false,
        lastSeen: null
      };
    }

    const canonicalId = publicUserId(user);
    const displayName = publicDisplayName(user);

    const liveSocket = await getUserSocket(canonicalId);
    const isOnlineNow = Boolean(liveSocket);

    if (isOnlineNow && (user.online !== true || user.socketId !== liveSocket.id)) {
      User.updateOne(
        {
          $or: [
            { userName: canonicalId },
            { userId: canonicalId }
          ]
        },
        {
          $set: {
            online: true,
            socketId: liveSocket.id,
            lastSeen: user.lastSeen || new Date()
          }
        }
      ).catch((err) => {
        logInfo("Status", `Failed to sync online state for ${canonicalId}`, err);
      });
    }

    if (!isOnlineNow && user.online === true) {
      User.updateOne(
        {
          $or: [
            { userName: canonicalId },
            { userId: canonicalId }
          ]
        },
        {
          $set: {
            online: false,
            socketId: null,
            lastSeen: user.lastSeen || new Date()
          }
        }
      ).catch((err) => {
        logInfo("Status", `Failed to sync offline state for ${canonicalId}`, err);
      });
    }

    return {
      user: canonicalId,
      userId: canonicalId,
      friendId: canonicalId,
      userName: displayName || canonicalId,
      displayName: displayName || canonicalId,
      profileImage: sanitizeProfileImage(user.profileImage),
      online: isOnlineNow,
      lastSeen: user.lastSeen || null
    };
  } catch (err) {
    logInfo("DB", `Error fetching status summary for ${userName}`, err);
    const fallback = normalizeName(userName);
    const fallbackDisplay = normalizeDisplayName(userName) || fallback;

    return {
      user: fallback,
      userId: fallback,
      friendId: fallback,
      userName: fallbackDisplay,
      displayName: fallbackDisplay,
      profileImage: "",
      online: false,
      lastSeen: null
    };
  }
}

async function notifyFriendsStatusChanged(userName) {
  try {
    const me = normalizeName(userName);
    if (!me) return;

    const friendships = await Friendship.find({
      $or: [
        { userA: me },
        { userB: me }
      ]
    })
      .select("userA userB")
      .lean();

    if (!friendships.length) return;

    const myStatus = await getUserStatusSummary(me);

    for (const item of friendships) {
      const friend = item.userA === me ? item.userB : item.userA;
      await emitToUser(friend, "update_status", {
        user: myStatus.user,
        userId: myStatus.userId,
        friendId: myStatus.friendId,
        userName: myStatus.userName,
        displayName: myStatus.displayName,
        profileImage: myStatus.profileImage || "",
        online: myStatus.online,
        lastSeen: myStatus.lastSeen
      });
    }
  } catch (err) {
    logInfo("Status", `Failed to notify friends status change for ${userName}`, err);
  }
}

async function getFriendIdsForUser(userName) {
  try {
    const me = normalizeName(userName);
    if (!me) return [];

    const friendships = await Friendship.find({
      $or: [
        { userA: me },
        { userB: me }
      ]
    })
      .select("userA userB")
      .lean();

    const ids = [];

    for (const item of friendships) {
      const friend = item.userA === me ? item.userB : item.userA;
      if (friend) ids.push(friend);
    }

    return [...new Set(ids)];
  } catch (err) {
    logInfo("DB", `Failed to get friend ids for ${userName}`, err);
    return [];
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
          displayName: cleanName,
          deviceId: ""
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
    }).select("socketId online isBanned").lean();

    if (!dbUser) {
      await ensureUserRegistrationState(me, socket.id);
      return { ok: true };
    }

    if (dbUser.isBanned === true) {
      return { ok: false, reason: "banned" };
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

async function deletePendingFriendRequestsBetween(userA, userB) {
  try {
    const a = normalizeName(userA);
    const b = normalizeName(userB);
    if (!a || !b || a === b) return;

    await FriendRequest.deleteMany({
      status: "pending",
      $or: [
        { from: a, to: b },
        { from: b, to: a }
      ]
    });
  } catch (err) {
    logInfo("FriendRequest", `Failed deleting pending requests between ${userA} and ${userB}`, err);
  }
}

function createPendingMatchEntry(userA, userB, chatType = "text") {
  const a = normalizeName(userA);
  const b = normalizeName(userB);
  const safeChatType = normalizeChatType(chatType);
  const key = pairKey(a, b);

  const timeoutId = setTimeout(async () => {
    try {
      const proposal = pendingMatches.get(key);
      if (!proposal) return;

      pendingMatches.delete(key);
      pendingMatchByUser.delete(proposal.userA);
      pendingMatchByUser.delete(proposal.userB);
      unlockUserSearch(proposal.userA);
      unlockUserSearch(proposal.userB);
      clearVoiceMatchGrace(proposal.userA);
      clearVoiceMatchGrace(proposal.userB);

      await deletePendingFriendRequestsBetween(proposal.userA, proposal.userB);

      await emitToUser(proposal.userA, "match_timeout", {
        reason: "no_response",
        chatType: proposal.chatType
      });
      await emitToUser(proposal.userB, "match_timeout", {
        reason: "no_response",
        chatType: proposal.chatType
      });

      await tryMatch(proposal.userA, proposal.chatType);
      await tryMatch(proposal.userB, proposal.chatType);
    } catch (err) {
      logInfo("Error", "Pending match timeout cleanup failed", err);
    }
  }, MATCH_PROPOSAL_TTL);

  pendingMatches.set(key, {
    userA: a,
    userB: b,
    chatType: safeChatType,
    acceptedBy: new Set(),
    createdAt: Date.now(),
    timeoutId
  });

  pendingMatchByUser.set(a, key);
  pendingMatchByUser.set(b, key);

  return key;
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
      unlockUserSearch(pending.caller);
      unlockUserSearch(pending.callee);

      await emitToUser(pending.caller, "call_ended", { reason: "no_answer" });
      await emitToUser(pending.callee, "call_ended", { reason: "no_answer" });
    } catch (err) {
      logInfo("Error", "Call timeout cleanup failed", err);
    }
  }, CALL_RING_TIMEOUT);

  pendingCalls.set(key, {
    caller: normalizeName(caller),
    callee: normalizeName(callee),
    timeoutId,
    createdAt: Date.now(),
  });

  pendingCallByUser.set(normalizeName(caller), key);
  pendingCallByUser.set(normalizeName(callee), key);

  return key;
}

function createPendingRandomCall(caller, callee) {
  const key = pairKey(caller, callee);

  const timeoutId = setTimeout(async () => {
    try {
      const pending = pendingRandomCalls.get(key);
      if (!pending) return;

      pendingRandomCalls.delete(key);
      pendingRandomCallByUser.delete(pending.caller);
      pendingRandomCallByUser.delete(pending.callee);
      unlockUserSearch(pending.caller);
      unlockUserSearch(pending.callee);
      clearVoiceMatchGrace(pending.caller);
      clearVoiceMatchGrace(pending.callee);

      await emitToUser(pending.caller, "random_call_ended", {
        reason: "no_answer"
      });
      await emitToUser(pending.callee, "random_call_ended", {
        reason: "no_answer"
      });
    } catch (err) {
      logInfo("Error", "Random call timeout cleanup failed", err);
    }
  }, CALL_RING_TIMEOUT);

  pendingRandomCalls.set(key, {
    caller: normalizeName(caller),
    callee: normalizeName(callee),
    timeoutId,
    createdAt: Date.now(),
  });

  pendingRandomCallByUser.set(normalizeName(caller), key);
  pendingRandomCallByUser.set(normalizeName(callee), key);

  return key;
}

async function clearCallStateForUser(userName, reason = "ended") {
  const me = normalizeName(userName);
  if (!me) return;

  const activePartner = activeCalls.get(me);
  if (activePartner) {
    activeCalls.delete(me);
    activeCalls.delete(activePartner);
    unlockUserSearch(me);
    unlockUserSearch(activePartner);
    await emitToUser(activePartner, "call_ended", { reason });
    await emitToUser(me, "call_ended", { reason });
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
      unlockUserSearch(me);
      unlockUserSearch(other);

      await emitToUser(other, "call_ended", { reason });
      await emitToUser(me, "call_ended", { reason });
    } else {
      pendingCallByUser.delete(me);
      unlockUserSearch(me);
    }
  } else {
    unlockUserSearch(me);
  }
}

async function clearRandomCallStateForUser(userName, reason = "ended") {
  const me = normalizeName(userName);
  if (!me) return;

  const activePartner = activeRandomCalls.get(me);
  if (activePartner) {
    activeRandomCalls.delete(me);
    activeRandomCalls.delete(activePartner);
    unlockUserSearch(me);
    unlockUserSearch(activePartner);
    clearVoiceMatchGrace(me);
    clearVoiceMatchGrace(activePartner);
    await emitToUser(activePartner, "random_call_ended", { reason });
    await emitToUser(me, "random_call_ended", { reason });
  }

  const pendingKey = pendingRandomCallByUser.get(me);
  if (pendingKey) {
    const pending = pendingRandomCalls.get(pendingKey);
    if (pending) {
      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      const other = pending.caller === me ? pending.callee : pending.caller;

      pendingRandomCalls.delete(pendingKey);
      pendingRandomCallByUser.delete(pending.caller);
      pendingRandomCallByUser.delete(pending.callee);
      unlockUserSearch(me);
      unlockUserSearch(other);
      clearVoiceMatchGrace(me);
      clearVoiceMatchGrace(other);

      await emitToUser(other, "random_call_ended", { reason });
      await emitToUser(me, "random_call_ended", { reason });
    } else {
      pendingRandomCallByUser.delete(me);
      unlockUserSearch(me);
      clearVoiceMatchGrace(me);
    }
  } else if (!activeRandomCalls.has(me)) {
    clearVoiceMatchGrace(me);
    unlockUserSearch(me);
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

async function restartSearchForUser(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;

  if (
    isUserUnavailableForMatch(cleanName) ||
    isUserInQueue(cleanName) ||
    isUserInVoiceTransition(cleanName)
  ) {
    return;
  }

  const preferredChatType = getUserPreferredChatType(cleanName);

  await emitToUser(cleanName, "match_searching", {
    status: "searching",
    message: "Searching for a new partner...",
    chatType: preferredChatType
  });

  await tryMatch(cleanName, preferredChatType);
}

async function clearUserBusyState(userName, reason = "state_cleared", chatType = null) {
  const me = normalizeName(userName);
  if (!me) return;

  const safeChatType = chatType ? normalizeChatType(chatType) : null;

  logInfo("DEBUG", "clearUserBusyState called", {
    user: me,
    reason,
    chatType: safeChatType,
    activePartner: activeMatches.get(me) || null,
    pendingKey: pendingMatchByUser.get(me) || null,
    inQueue: isUserInQueue(me, safeChatType),
    locked: isUserLocked(me),
  });

  if (
    (reason === "left_chat" || reason === "partner_stopped_search") &&
    isUserInVoiceTransition(me)
  ) {
    logInfo("DEBUG", "clearUserBusyState ignored during voice transition", {
      user: me,
      reason,
      chatType: safeChatType,
    });
    return;
  }

  const wasInQueueOnly =
    isUserInQueue(me, safeChatType) &&
    !pendingMatchByUser.has(me) &&
    !activeMatches.has(me);

  if (wasInQueueOnly) {
    removeFromQueue(me, safeChatType);
    unlockUserSearch(me);
    return;
  }

  removeFromQueue(me, safeChatType);

  const activePartner = activeMatches.get(me);
  if (activePartner) {
    logInfo("DEBUG", "clearUserBusyState clearing active match", {
      user: me,
      partner: activePartner,
      reason,
    });

    activeMatches.delete(me);
    activeMatches.delete(activePartner);
    unlockUserSearch(me);
    unlockUserSearch(activePartner);
    clearVoiceMatchGrace(me);
    clearVoiceMatchGrace(activePartner);

    await deletePendingFriendRequestsBetween(me, activePartner);

    await emitToUser(activePartner, "match_closed", { reason });

    if (
      reason === "partner_stopped_search" ||
      reason === "partner_left" ||
      reason === "partner_disconnected" ||
      reason === "left_chat"
    ) {
      await restartSearchForUser(activePartner);
    }

    return;
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
      unlockUserSearch(proposal.userA);
      unlockUserSearch(proposal.userB);
      clearVoiceMatchGrace(proposal.userA);
      clearVoiceMatchGrace(proposal.userB);

      await deletePendingFriendRequestsBetween(proposal.userA, proposal.userB);

      await emitToUser(other, "match_cancelled", { reason });

      if (
        reason === "partner_stopped_search" ||
        reason === "partner_left" ||
        reason === "partner_disconnected" ||
        reason === "left_chat"
      ) {
        await restartSearchForUser(other);
      }
    } else {
      pendingMatchByUser.delete(me);
      unlockUserSearch(me);
      clearVoiceMatchGrace(me);
    }
    return;
  }

  unlockUserSearch(me);
}

async function clearRelationshipRuntimeState(userA, userB) {
  const a = normalizeName(userA);
  const b = normalizeName(userB);
  if (!a || !b) return;

  removeFromQueue(a);
  removeFromQueue(b);
  unlockUserSearch(a);
  unlockUserSearch(b);
  clearVoiceMatchGrace(a);
  clearVoiceMatchGrace(b);

  await deletePendingFriendRequestsBetween(a, b);

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

  const activeCallA = activeCalls.get(a);
  if (activeCallA === b) {
    activeCalls.delete(a);
    activeCalls.delete(b);
  }

  const activeRandomCallA = activeRandomCalls.get(a);
  if (activeRandomCallA === b) {
    activeRandomCalls.delete(a);
    activeRandomCalls.delete(b);
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
        unlockUserSearch(proposalA.userA);
        unlockUserSearch(proposalA.userB);
        clearVoiceMatchGrace(proposalA.userA);
        clearVoiceMatchGrace(proposalA.userB);
      }
    } else {
      pendingMatchByUser.delete(a);
      unlockUserSearch(a);
      clearVoiceMatchGrace(a);
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
        unlockUserSearch(proposalB.userA);
        unlockUserSearch(proposalB.userB);
        clearVoiceMatchGrace(proposalB.userA);
        clearVoiceMatchGrace(proposalB.userB);
      }
    } else {
      pendingMatchByUser.delete(b);
      unlockUserSearch(b);
      clearVoiceMatchGrace(b);
    }
  }

  const pendingPrivateCallKey = pendingCallByUser.get(a);
  if (pendingPrivateCallKey) {
    const pendingPrivateCall = pendingCalls.get(pendingPrivateCallKey);
    if (pendingPrivateCall) {
      const involvesPair =
        [pendingPrivateCall.caller, pendingPrivateCall.callee].includes(a) &&
        [pendingPrivateCall.caller, pendingPrivateCall.callee].includes(b);

      if (involvesPair) {
        if (pendingPrivateCall.timeoutId) {
          clearTimeout(pendingPrivateCall.timeoutId);
        }
        pendingCalls.delete(pendingPrivateCallKey);
        pendingCallByUser.delete(pendingPrivateCall.caller);
        pendingCallByUser.delete(pendingPrivateCall.callee);
        unlockUserSearch(pendingPrivateCall.caller);
        unlockUserSearch(pendingPrivateCall.callee);
      }
    }
  }

  const pendingRandomCallKey = pendingRandomCallByUser.get(a);
  if (pendingRandomCallKey) {
    const pendingRandomCall = pendingRandomCalls.get(pendingRandomCallKey);
    if (pendingRandomCall) {
      const involvesPair =
        [pendingRandomCall.caller, pendingRandomCall.callee].includes(a) &&
        [pendingRandomCall.caller, pendingRandomCall.callee].includes(b);

      if (involvesPair) {
        if (pendingRandomCall.timeoutId) {
          clearTimeout(pendingRandomCall.timeoutId);
        }
        pendingRandomCalls.delete(pendingRandomCallKey);
        pendingRandomCallByUser.delete(pendingRandomCall.caller);
        pendingRandomCallByUser.delete(pendingRandomCall.callee);
        unlockUserSearch(pendingRandomCall.caller);
        unlockUserSearch(pendingRandomCall.callee);
        clearVoiceMatchGrace(pendingRandomCall.caller);
        clearVoiceMatchGrace(pendingRandomCall.callee);
      }
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

    const postData = "Ttl=21600";
    const options = {
      hostname: "api.twilio.com",
      port: 443,
      path: `/2010-04-01/Accounts/${sidNormalized}/Tokens.json`,
      method: "POST",
      headers: {
        Authorization:
          "Basic " +
          Buffer.from(`${sidNormalized}:${tokenNormalized}`).toString("base64"),
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
// 5. منطق المطابقة
// ==========================================

async function tryMatch(userName, requestedChatType = null) {
  const me = normalizeName(userName);
  if (!me) return;

  if (!acquireMatchLock(me)) {
    return;
  }

  let lockedPartner = null;

  try {
    const mySocket = await getUserSocket(me);
    if (!mySocket) {
      await emitToUser(me, "error_msg", { message: "User is not fully connected yet" });
      return;
    }

    if (isUserUnavailableForMatch(me) || isUserInVoiceTransition(me)) {
      return;
    }

    const chatType = normalizeChatType(
      requestedChatType || socketChatTypeFromUser(mySocket, me)
    );

    setUserPreferredChatType(me, chatType, mySocket);

    logInfo("Matchmaking", `Trying to match user ${me}`, {
      chatType,
      textQueueSize: waitingQueues.text.length,
      voiceQueueSize: waitingQueues.voice.length
    });

    if (isUserInQueue(me)) {
      removeFromQueue(me, chatType);
    }

    let partner = null;
    const queue = getQueueByChatType(chatType);

    for (let i = 0; i < queue.length; i++) {
      const candidate = normalizeName(queue[i]);
      if (!candidate || candidate === me) continue;

      if (!acquireMatchLock(candidate)) {
        continue;
      }

      lockedPartner = candidate;

      try {
        const alreadyFriends = await Friendship.findOne({
          pairKey: pairKey(me, candidate)
        }).lean();

        if (alreadyFriends) {
          releaseMatchLock(candidate);
          lockedPartner = null;
          continue;
        }

        const partnerSocket = await getUserSocket(candidate);
        const candidateChatType = getUserPreferredChatType(candidate);

        if (
          partnerSocket &&
          candidateChatType === chatType &&
          !isUserUnavailableForMatch(candidate) &&
          !isUserInVoiceTransition(candidate)
        ) {
          partner = candidate;
          queue.splice(i, 1);

          logInfo("Matchmaking", `Partner found`, {
            userA: me,
            userB: partner,
            chatType
          });

          break;
        }

        queue.splice(i, 1);
        i--;

        releaseMatchLock(candidate);
        lockedPartner = null;
      } catch (candidateErr) {
        logInfo("Matchmaking", `Candidate validation failed for ${candidate}`, candidateErr);
        releaseMatchLock(candidate);
        lockedPartner = null;
      }
    }

    if (partner) {
      const key = createPendingMatchEntry(me, partner, chatType);

      const myProfile = await getFullUserProfile(me);
      const partnerProfile = await getFullUserProfile(partner);

      await emitToUser(me, "match_found", {
        partner: partnerProfile,
        proposalKey: key,
        chatType
      });
      await emitToUser(partner, "match_found", {
        partner: myProfile,
        proposalKey: key,
        chatType
      });

      if (lockedPartner === partner) {
        releaseMatchLock(partner);
        lockedPartner = null;
      }
    } else {
      addToQueue(me, chatType);
      await emitToUser(me, "waiting_in_queue", {
        status: "searching",
        message: "Searching for a partner...",
        chatType
      });
    }
  } catch (err) {
    logInfo("Error", `Matchmaking failure for ${me}`, err);
  } finally {
    if (lockedPartner) {
      releaseMatchLock(lockedPartner);
    }
    releaseMatchLock(me);
  }
}

function socketChatTypeFromUser(socket, userName) {
  if (socket?.data?.chatType) {
    return socket.data.chatType;
  }
  return getUserPreferredChatType(userName);
}

// ==========================================
// 6. أحداث السوكيت
// ==========================================

io.on("connection", (socket) => {
  logInfo("Network", `Socket connected: ${socket.id}`);

  socket.on("register_user", async (rawName) => {
    try {
      const registration = parseRegistrationPayload(rawName);
      const displayName = sanitizeOptionalString(registration.displayName, 60);

      if (!displayName) {
        socket.emit("error_msg", { message: "Display name is required" });
        return;
      }

      const cleanClientId = normalizeClientId(registration.clientId);
      if (!cleanClientId) {
        socket.emit("error_msg", { message: "Invalid client identity" });
        return;
      }

      const userName = buildStableUserId(cleanClientId);
      if (!userName) {
        socket.emit("error_msg", { message: "Invalid user identity" });
        return;
      }

      const existingUser = await User.findOne({
        $or: [
          { userName },
          { userId: userName }
        ]
      }).select("socketId isBanned deviceId").lean();

      if (existingUser?.isBanned === true) {
        socket.emit("error_msg", { message: "This account is banned" });
        return;
      }

      if (existingUser?.socketId && existingUser.socketId !== socket.id) {
        const oldSocket = io.sockets.sockets.get(existingUser.socketId);
        if (oldSocket) {
          oldSocket.emit("session_replaced", { message: "Logged in from another device" });
          oldSocket.disconnect(true);
        }
        socketToUser.delete(existingUser.socketId);
      }

      const preservedChatType = getUserPreferredChatType(userName);

      socket.data.userName = userName;
      socket.data.userId = userName;
      socket.data.displayName = displayName;
      socket.data.clientId = cleanClientId;
      socket.data.deviceId = cleanClientId; // ✅ تمت الإضافة
      socket.data.chatType = preservedChatType;

      socketToUser.set(socket.id, userName);
      userToSocket.set(userName, socket.id);
      userSearchPreferences.set(userName, preservedChatType);

      await User.findOneAndUpdate(
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
            deviceId: cleanClientId, // ✅ تمت الإضافة
            socketId: socket.id,
            online: true,
            lastSeen: new Date()
          },
          $setOnInsert: {
            profileImage: "",
            country: "",
            age: null,
            bio: "",
            gender: "unspecified",
            deviceId: cleanClientId // ✅ تمت الإضافة
          }
        },
        { upsert: true, returnDocument: "after" }
      );

      logInfo("Auth", `User registered`, {
        userName,
        displayName,
        deviceId: cleanClientId,
        chatType: preservedChatType,
        socketId: socket.id
      });

      socket.emit("registration_success", {
        userName: displayName,
        displayName,
        userId: userName,
        deviceId: cleanClientId, // ✅ تمت الإضافة
        canonicalUserName: userName,
        timestamp: new Date()
      });

      socket.emit("user_ready_for_matchmaking", {
        success: true,
        userName: displayName,
        userId: userName,
        socketId: socket.id
      });

      await notifyFriendsStatusChanged(userName);
    } catch (err) {
      logInfo("Error", "Registration process failed", err);
      socket.emit("error_msg", { message: "Registration process failed" });
    }
  });

  socket.on("update_profile", async (data) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedDisplayName = sanitizeOptionalString(
        normalizeDisplayName(data?.userName),
        60
      );

      const updateFields = {
        profileImage: sanitizeProfileImage(data?.profileImage),
        country: sanitizeOptionalString(data?.country, 100),
        age: sanitizeAge(data?.age),
        bio: sanitizeOptionalString(data?.bio, 500),
        gender:
          sanitizeOptionalString(data?.gender || "unspecified", 40) ||
          "unspecified",
        lastSeen: new Date(),
      };

      if (requestedDisplayName) {
        updateFields.displayName = requestedDisplayName;
        socket.data.displayName = requestedDisplayName;
      }

      const updatedUser = await User.findOneAndUpdate(
        {
          $or: [
            { userName: me },
            { userId: me }
          ]
        },
        { $set: updateFields },
        { returnDocument: "after" }
      ).lean();

      socket.emit("profile_updated", {
        success: true,
        user: updatedUser
      });

      await notifyFriendsStatusChanged(me);
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

      logInfo(
        "Twilio",
        `TURN credentials served to ${socket.data.userName || socket.id}`,
        {
          provider: result.provider,
          count: result.iceServers.length,
        }
      );
    } catch (err) {
      logInfo("Twilio", "Failed to provide turn credentials", err);
      socket.emit("turn_credentials", {
        success: true,
        iceServers: getFallbackIceServers(),
        provider: "fallback_stun",
      });
    }
  });

  socket.on("find_partner", async (payload) => {
    try {
      const me = socket.data.userName;
      if (!me) {
        socket.emit("error_msg", {
          message: "Please register user before matchmaking"
        });
        return;
      }

      const cleanMe = normalizeName(me);
      const chatType = normalizeChatType(payload?.chatType);

      setUserPreferredChatType(cleanMe, chatType, socket);

      logInfo("Matchmaking", `find_partner received`, {
        user: cleanMe,
        requestedChatType: payload?.chatType,
        normalizedChatType: chatType,
        socketId: socket.id
      });

      const activeSocketId = userToSocket.get(cleanMe);
      if (activeSocketId && activeSocketId !== socket.id) {
        return;
      }

      if (
        isUserInQueue(cleanMe) ||
        isUserUnavailableForMatch(cleanMe) ||
        isUserInVoiceTransition(cleanMe)
      ) {
        return;
      }

      const readiness = await validateUserReadyForMatchmaking(me, socket);
      if (!readiness.ok) {
        const msg =
          readiness.reason === "banned"
            ? "This account is banned"
            : "User is not ready for matchmaking yet";
        socket.emit("error_msg", { message: msg });
        return;
      }

      await tryMatch(me, chatType);
    } catch (err) {
      logInfo("Error", "find_partner failed", err);
      socket.emit("error_msg", { message: "Failed to start search" });
    }
  });

  socket.on("stop_search", async (payload) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedChatType = normalizeChatType(
        payload?.chatType || socket.data.chatType || getUserPreferredChatType(me)
      );
      setUserPreferredChatType(me, requestedChatType, socket);

      const cleanMe = normalizeName(me);
      const wasInQueue = isUserInQueue(cleanMe, requestedChatType);
      const hadPendingMatch = pendingMatchByUser.has(cleanMe);
      const hadActiveMatch = activeMatches.has(cleanMe);

      if (wasInQueue && !hadPendingMatch && !hadActiveMatch) {
        removeFromQueue(cleanMe, requestedChatType);
        unlockUserSearch(cleanMe);

        socket.emit("search_stopped", {
          success: true,
          mode: "queue_only",
          chatType: requestedChatType
        });

        return;
      }

      if (!hadPendingMatch && !hadActiveMatch) {
        unlockUserSearch(cleanMe);

        socket.emit("search_stopped", {
          success: true,
          mode: "self_only",
          chatType: requestedChatType
        });
        return;
      }

      await clearUserBusyState(me, "partner_stopped_search", requestedChatType);

      socket.emit("search_stopped", {
        success: true,
        mode: "relationship_state",
        chatType: requestedChatType
      });
    } catch (err) {
      logInfo("Error", "stop_search failed", err);
      socket.emit("error_msg", { message: "Failed to stop search" });
    }
  });

  socket.on("leave_chat", async (payload) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedChatType = normalizeChatType(
        payload?.chatType || socket.data.chatType || getUserPreferredChatType(me)
      );
      setUserPreferredChatType(me, requestedChatType, socket);

      logInfo("DEBUG", "leave_chat received", {
        user: me,
        socketId: socket.id,
        chatType: requestedChatType,
      });

      if (requestedChatType === "voice" && isUserInVoiceTransition(me)) {
        logInfo("DEBUG", "leave_chat ignored during voice transition", {
          user: me,
          socketId: socket.id,
          chatType: requestedChatType,
        });
        return;
      }

      await clearUserBusyState(me, "left_chat", requestedChatType);
      await clearRandomCallStateForUser(me, "left_chat");

      socket.emit("search_stopped", {
        success: true,
        mode: "leave_chat",
        chatType: requestedChatType
      });
    } catch (err) {
      logInfo("Error", "leave_chat failed", err);
      socket.emit("error_msg", { message: "Failed to leave chat" });
    }
  });

  socket.on("accept_match", async (payload) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedChatType = normalizeChatType(
        payload?.chatType || socket.data.chatType || getUserPreferredChatType(me)
      );
      setUserPreferredChatType(me, requestedChatType, socket);

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
        lockUserSearch(proposal.userA);
        lockUserSearch(proposal.userB);

        if (proposal.chatType === "voice") {
          setVoiceMatchGrace(proposal.userA);
          setVoiceMatchGrace(proposal.userB);
        } else {
          clearVoiceMatchGrace(proposal.userA);
          clearVoiceMatchGrace(proposal.userB);
        }

        logInfo("DEBUG", "match state stored", {
          userA: proposal.userA,
          userB: proposal.userB,
          activeA: activeMatches.get(proposal.userA),
          activeB: activeMatches.get(proposal.userB),
          lockedA: isUserLocked(proposal.userA),
          lockedB: isUserLocked(proposal.userB),
          graceA: hasVoiceMatchGrace(proposal.userA),
          graceB: hasVoiceMatchGrace(proposal.userB),
        });

        const profileA = await getFullUserProfile(proposal.userA);
        const profileB = await getFullUserProfile(proposal.userB);

        await emitToUser(proposal.userA, "match_confirmed", {
          partnerName: profileB?.userName || proposal.userB,
          partnerId: profileB?.userId || proposal.userB,
          age: profileB?.age ?? null,
          country: profileB?.country ?? "",
          bio: profileB?.bio ?? "",
          gender: profileB?.gender ?? "unspecified",
          profileImage: profileB?.profileImage ?? "",
          lastSeen: profileB?.lastSeen ?? null,
          online: profileB?.online === true,
          chatType: proposal.chatType
        });

        await emitToUser(proposal.userB, "match_confirmed", {
          partnerName: profileA?.userName || proposal.userA,
          partnerId: profileA?.userId || proposal.userA,
          age: profileA?.age ?? null,
          country: profileA?.country ?? "",
          bio: profileA?.bio ?? "",
          gender: profileA?.gender ?? "unspecified",
          profileImage: profileA?.profileImage ?? "",
          lastSeen: profileA?.lastSeen ?? null,
          online: profileA?.online === true,
          chatType: proposal.chatType
        });

        logInfo("Matchmaking", `Match confirmed`, {
          userA: proposal.userA,
          userB: proposal.userB,
          chatType: proposal.chatType
        });
      } else {
        const meProfile = await getFullUserProfile(me);
        await emitToUser(partner, "partner_accepted", {
          message: "Partner is ready",
          partnerName: meProfile?.userName || me,
          partnerId: meProfile?.userId || me,
          profileImage: meProfile?.profileImage || "",
          chatType: proposal.chatType
        });
      }
    } catch (err) {
      logInfo("Error", "accept_match failed", err);
    }
  });

  socket.on("skip_partner", async (payload) => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const requestedChatType = normalizeChatType(
        payload?.chatType || socket.data.chatType || getUserPreferredChatType(me)
      );
      const myChatType = setUserPreferredChatType(me, requestedChatType, socket);

      const pendingKey = pendingMatchByUser.get(me);

      if (pendingKey) {
        const proposal = pendingMatches.get(pendingKey);
        if (proposal) {
          if (proposal.timeoutId) {
            clearTimeout(proposal.timeoutId);
          }

          const other = proposal.userA === me ? proposal.userB : proposal.userA;
          const proposalChatType = proposal.chatType || myChatType;

          pendingMatches.delete(pendingKey);
          pendingMatchByUser.delete(me);
          pendingMatchByUser.delete(other);
          unlockUserSearch(me);
          unlockUserSearch(other);
          clearVoiceMatchGrace(me);
          clearVoiceMatchGrace(other);

          await deletePendingFriendRequestsBetween(me, other);

          await emitToUser(other, "match_searching", {
            status: "searching",
            message: "Searching for a new partner...",
            chatType: proposalChatType
          });

          await clearRandomCallStateForUser(me, "ended");
          await clearRandomCallStateForUser(other, "ended");

          await tryMatch(other, proposalChatType);
        }

        await tryMatch(me, myChatType);
        return;
      }

      const partner = activeMatches.get(me);

      if (partner) {
        activeMatches.delete(me);
        activeMatches.delete(partner);
        unlockUserSearch(me);
        unlockUserSearch(partner);
        clearVoiceMatchGrace(me);
        clearVoiceMatchGrace(partner);

        await deletePendingFriendRequestsBetween(me, partner);
        await clearRandomCallStateForUser(me, "ended");
        await clearRandomCallStateForUser(partner, "ended");

        await emitToUser(partner, "match_searching", {
          status: "searching",
          message: "Searching for a new partner...",
          chatType: getUserPreferredChatType(partner)
        });

        await tryMatch(partner, getUserPreferredChatType(partner));
      }

      await tryMatch(me, myChatType);
    } catch (err) {
      logInfo("Error", "skip_partner failed", err);
    }
  });

  socket.on("message", async (msgContent) => {
    const me = socket.data.userName;
    const partner = activeMatches.get(me);
    const cleanText = sanitizeText(msgContent, 2000);

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
    const imageUrl = sanitizeProfileImage(imgData?.url);

    if (!partner || !imageUrl) return;

    try {
      const data = {
        from: me,
        to: partner,
        type: "image",
        image: imageUrl,
        time: new Date(),
        conversationKey: pairKey(me, partner)
      };
      await RandomChatMessage.create(data);
      await emitToUser(partner, "image_received", data);
    } catch (err) {
      logInfo("Error", "Image transmission failed", err);
    }
  });

  socket.on("typing", async (isTyping) => {
    try {
      const me = socket.data.userName;
      const partner = activeMatches.get(me);
      if (!partner) return;

      const key = `${pairKey(me, partner)}__${me}`;
      const oldTimeout = userTypingTimeout.get(key);

      if (oldTimeout) {
        clearTimeout(oldTimeout);
      }

      await emitToUser(partner, "partner_typing", {
        from: me,
        isTyping: Boolean(isTyping)
      });

      if (isTyping) {
        const timeoutId = setTimeout(() => {
          emitToUser(partner, "partner_typing", {
            from: me,
            isTyping: false
          });
          userTypingTimeout.delete(key);
        }, 2500);

        userTypingTimeout.set(key, timeoutId);
      } else {
        userTypingTimeout.delete(key);
      }
    } catch (err) {
      logInfo("Error", "typing event failed", err);
    }
  });

  socket.on("send_friend_request", async (targetName) => {
    const me = socket.data.userName;
    let to = extractTargetName(targetName);

    if (!to) {
      to = activeMatches.get(me) || "";
    }

    if (!me || !to || me === to) return;

    try {
      const targetUser = await User.findOne({
        $or: [
          { userName: to },
          { userId: to }
        ]
      }).select("_id userName userId displayName").lean();

      if (!targetUser) {
        return socket.emit("error_msg", {
          message: "Target user does not exist"
        });
      }

      const targetId = publicUserId(targetUser);
      if (!targetId || me === targetId) return;

      const alreadyFriends = await Friendship.findOne({
        pairKey: pairKey(me, targetId)
      }).lean();

      if (alreadyFriends) {
        return socket.emit("error_msg", { message: "Already friends" });
      }

      const existingReq = await FriendRequest.findOne({
        from: me,
        to: targetId,
        status: "pending"
      }).lean();

      const reverseReq = await FriendRequest.findOne({
        from: targetId,
        to: me,
        status: "pending"
      }).lean();

      if (reverseReq) {
        return socket.emit("error_msg", {
          message: "There is already a pending request from this user"
        });
      }

      if (!existingReq) {
        const myProfile = await getFullUserProfile(me);
        await FriendRequest.create({ from: me, to: targetId });
        await emitToUser(targetId, "new_friend_request", {
          from: myProfile?.userName || me,
          fromId: me,
          profileImage: myProfile?.profileImage || ""
        });
        socket.emit("request_sent", {
          success: true,
          to: publicDisplayName(targetUser) || targetId,
          toId: targetId
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
    const me = socket.data.userName;
    const from = extractTargetName(data?.fromId || data?.from || data);
    const accept = Boolean(data?.accept);

    try {
      const request = await FriendRequest.findOne({
        from,
        to: me,
        status: "pending"
      });
      if (!request) return;

      if (accept) {
        request.status = "accepted";
        await request.save();

        const existingFriendship = await Friendship.findOne({
          pairKey: pairKey(from, me)
        }).lean();

        if (!existingFriendship) {
          await Friendship.create({
            userA: from,
            userB: me,
            pairKey: pairKey(from, me)
          });
        }

        const myProfile = await getFullUserProfile(me);
        const fromProfile = await getFullUserProfile(from);

        await emitToUser(from, "friend_request_accepted", {
          by: myProfile?.userName || me,
          byId: me,
          profileImage: myProfile?.profileImage || ""
        });

        await emitToUser(me, "friend_added_successfully", {
          userName: fromProfile?.userName || from,
          userId: from,
          profileImage: fromProfile?.profileImage || ""
        });

        await emitToUser(from, "friend_added_successfully", {
          userName: myProfile?.userName || me,
          userId: me,
          profileImage: myProfile?.profileImage || ""
        });

        await forceRefreshUserSocketState(me);
        await forceRefreshUserSocketState(from);

        await notifyFriendsStatusChanged(me);
        await notifyFriendsStatusChanged(from);
      } else {
        request.status = "rejected";
        await request.save();
        await emitToUser(from, "friend_request_rejected", { by: me });
      }
    } catch (err) {
      logInfo("Error", "Friend response failed", err);
      socket.emit("error_msg", {
        message: "Failed to respond to friend request"
      });
    }
  });

  socket.on("get_friends_status", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const friendIds = await getFriendIdsForUser(me);

      for (const friendId of friendIds) {
        const status = await getUserStatusSummary(friendId);

        socket.emit("update_status", {
          user: status.userId,
          userId: status.userId,
          friendId: status.friendId,
          userName: status.userName,
          displayName: status.displayName,
          profileImage: status.profileImage || "",
          online: status.online,
          lastSeen: status.lastSeen
        });
      }
    } catch (err) {
      logInfo("Error", "Failed to get friends status", err);
      socket.emit("error_msg", { message: "Failed to get friends status" });
    }
  });

  socket.on("get_my_friends_status", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      const friendIds = await getFriendIdsForUser(me);

      for (const friendId of friendIds) {
        const status = await getUserStatusSummary(friendId);

        socket.emit("update_status", {
          user: status.userId,
          userId: status.userId,
          friendId: status.friendId,
          userName: status.userName,
          displayName: status.displayName,
          profileImage: status.profileImage || "",
          online: status.online,
          lastSeen: status.lastSeen
        });
      }
    } catch (err) {
      logInfo("Error", "Failed to get_my_friends_status", err);
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

      const myProfile = await getFullUserProfile(me);
      const friendProfile = await getFullUserProfile(friendName);

      socket.emit("friend_deleted_successfully", {
        friend: friendProfile?.userName || friendName,
        friendId: friendName,
        message: `Removed ${friendProfile?.userName || friendName} from friends`
      });

      await emitToUser(friendName, "friend_deleted_me", {
        from: myProfile?.userName || me,
        fromId: me,
        message: `${myProfile?.userName || me} removed you from friends`
      });

      await forceRefreshUserSocketState(me);
      await forceRefreshUserSocketState(friendName);
    } catch (err) {
      logInfo("Error", "Delete friend failed", err);
      socket.emit("error_msg", { message: "Failed to delete friend" });
    }
  });

  socket.on("get_private_history", async (data) => {
    try {
      const me = socket.data.userName;
      const other = extractTargetName(
        data?.toId ||
          data?.friendId ||
          data?.to ||
          data?.friend ||
          data?.with ||
          data?.userName
      );

      if (!me || !other) {
        return socket.emit("private_history", []);
      }

      const isFriend = await Friendship.findOne({
        pairKey: pairKey(me, other)
      }).lean();

      if (!isFriend) {
        return socket.emit("private_history", []);
      }

      const messages = await PrivateMessage.find({
        conversationKey: pairKey(me, other),
        isDeleted: false
      })
        .sort({ time: 1, createdAt: 1 })
        .lean();

      const normalizedMessages = messages.map((msg) => {
        const normalizedType =
          msg.type ||
          msg.messageType ||
          (msg.audio || msg.voiceUrl ? "voice" : msg.image || msg.imageUrl ? "image" : "text");

        return {
          ...msg,
          type: normalizedType,
          image: msg.image || msg.imageUrl || "",
          audio: msg.audio || msg.voiceUrl || "",
          text: msg.text || "",
          durationSeconds: Number.isFinite(Number(msg.durationSeconds))
            ? Number(msg.durationSeconds)
            : 0,
          fromId: msg.from,
          toId: msg.to
        };
      });

      socket.emit("private_history", normalizedMessages);
    } catch (err) {
      logInfo("Error", "Failed to load private history", err);
      socket.emit("private_history", []);
    }
  });

  socket.on("mark_messages_read", async (data) => {
    try {
      const me = socket.data.userName;
      const friend = extractTargetName(
        data?.friendId || data?.friend || data?.from || data?.to
      );

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

  socket.on("private_typing", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const isTyping = Boolean(data?.isTyping);

      if (!me || !to || me === to) return;

      const isFriend = await Friendship.findOne({
        pairKey: pairKey(me, to)
      }).lean();

      if (!isFriend) return;

      const typingKey = `${pairKey(me, to)}__${me}`;
      const oldTimeout = userTypingTimeout.get(typingKey);

      if (oldTimeout) {
        clearTimeout(oldTimeout);
        userTypingTimeout.delete(typingKey);
      }

      await emitToUser(to, "private_typing", {
        from: me,
        fromId: me,
        to,
        toId: to,
        isTyping
      });

      if (isTyping) {
        const timeoutId = setTimeout(async () => {
          try {
            await emitToUser(to, "private_typing", {
              from: me,
              fromId: me,
              to,
              toId: to,
              isTyping: false
            });
          } catch (err) {
            logInfo("Typing", `Failed auto-stop typing for ${me} -> ${to}`, err);
          } finally {
            userTypingTimeout.delete(typingKey);
          }
        }, 3000);

        userTypingTimeout.set(typingKey, timeoutId);
      }
    } catch (err) {
      logInfo("Error", "private_typing failed", err);
    }
  });

  socket.on("private_message", async (data) => {
    const me = socket.data.userName;
    const to = extractTargetName(data);

    const cleanText = sanitizeText(data?.text, 2000);
    const image = sanitizeProfileImage(data?.image);
    const audio = sanitizeAudioPayload(data?.audio);
    const durationSeconds = sanitizeDurationSeconds(data?.durationSeconds);

    const hasText = !!cleanText;
    const hasImage = !!image;
    const hasAudio = !!audio;

    if (!me || !to) return;
    if (!hasText && !hasImage && !hasAudio) return;

    try {
      const isFriend = await Friendship.findOne({
        pairKey: pairKey(me, to)
      }).lean();

      if (!isFriend) {
        return socket.emit("error_msg", { message: "Not friends yet" });
      }

      const typingKey = `${pairKey(me, to)}__${me}`;
      const oldTypingTimeout = userTypingTimeout.get(typingKey);
      if (oldTypingTimeout) {
        clearTimeout(oldTypingTimeout);
        userTypingTimeout.delete(typingKey);
      }

      await emitToUser(to, "private_typing", {
        from: me,
        fromId: me,
        to,
        toId: to,
        isTyping: false
      });

      const normalizedType =
        hasAudio ? "voice" : hasImage ? "image" : "text";

      const msg = await PrivateMessage.create({
        from: me,
        to,
        type: normalizedType,
        messageType: normalizedType,
        text: cleanText || "",
        image: hasImage ? image : "",
        imageUrl: hasImage ? image : "",
        audio: hasAudio ? audio : "",
        voiceUrl: hasAudio ? audio : "",
        durationSeconds: hasAudio ? durationSeconds : 0,
        conversationKey: pairKey(me, to),
        time: new Date(),
        readBy: [me]
      });

      const plainMsg = msg.toObject ? msg.toObject() : msg;
      const myProfile = await getFullUserProfile(me);
      const targetProfile = await getFullUserProfile(to);

      plainMsg.fromId = me;
      plainMsg.toId = to;
      plainMsg.fromName = myProfile?.userName || me;
      plainMsg.toName = targetProfile?.userName || to;
      plainMsg.profileImage = myProfile?.profileImage || "";
      plainMsg.fromProfileImage = myProfile?.profileImage || "";
      plainMsg.type = normalizedType;
      plainMsg.messageType = normalizedType;
      plainMsg.text = cleanText || "";
      plainMsg.image = hasImage ? image : "";
      plainMsg.imageUrl = hasImage ? image : "";
      plainMsg.audio = hasAudio ? audio : "";
      plainMsg.voiceUrl = hasAudio ? audio : "";
      plainMsg.durationSeconds = hasAudio ? durationSeconds : 0;

      const delivered = await emitToUser(
        to,
        "private_message_received",
        plainMsg
      );

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
      const me = socket.data.userName;
      const to = extractTargetName(data);

      if (!me || !to || me === to) return;

      const isFriend = await Friendship.findOne({
        pairKey: pairKey(me, to)
      }).lean();

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
      lockUserSearch(me);
      lockUserSearch(to);

      const myProfile = await getFullUserProfile(me);

      const incomingCallPayload = {
        from: myProfile?.userName || me,
        fromId: me,
        friendId: me,
        friendName: myProfile?.userName || me,
      };

      if (targetSocket?.id) {
        userToSocket.set(to, targetSocket.id);
        await User.findOneAndUpdate(
          {
            $or: [
              { userName: to },
              { userId: to }
            ]
          },
          {
            $set: {
              socketId: targetSocket.id,
              online: true,
              lastSeen: new Date()
            }
          },
          { returnDocument: "after" }
        );
      }

      targetSocket.emit("incoming_call", incomingCallPayload);
      socket.emit("call_ringing", { to });

      logInfo("Call", `Outgoing private call from ${me} to ${to}`);
    } catch (err) {
      logInfo("Error", "start_private_call failed", err);
      socket.emit("error_msg", { message: "Failed to start call" });
    }
  });

  socket.on("accept_private_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = extractTargetName(data);
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

      logInfo(
        "Call",
        `Private call connected between ${pending.caller} and ${pending.callee}`
      );
    } catch (err) {
      logInfo("Error", "accept_private_call failed", err);
    }
  });

  socket.on("reject_private_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = extractTargetName(data);
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
      unlockUserSearch(pending.caller);
      unlockUserSearch(pending.callee);

      await emitToUser(from, "call_rejected", { by: me });
      await emitToUser(me, "call_ended", { reason: "rejected" });
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

  socket.on("webrtc_offer", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const sdp = data?.sdp;
      const type = data?.type;

      if (!me || !to || !sdp || !type) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(to, "webrtc_offer", {
        from: me,
        sdp,
        type,
      });
    } catch (err) {
      logInfo("Error", "webrtc_offer failed", err);
      socket.emit("error_msg", { message: "Failed to relay offer" });
    }
  });

  socket.on("webrtc_answer", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const sdp = data?.sdp;
      const type = data?.type;

      if (!me || !to || !sdp || !type) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(to, "webrtc_answer", {
        from: me,
        sdp,
        type,
      });
    } catch (err) {
      logInfo("Error", "webrtc_answer failed", err);
      socket.emit("error_msg", { message: "Failed to relay answer" });
    }
  });

  socket.on("webrtc_ice_candidate", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const candidate = data?.candidate;

      if (!me || !to || !candidate) return;

      const activePartner = activeCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", { message: "Call is not active" });
      }

      await emitToUser(to, "webrtc_ice_candidate", {
        from: me,
        candidate,
      });
    } catch (err) {
      logInfo("Error", "webrtc_ice_candidate failed", err);
      socket.emit("error_msg", { message: "Failed to relay ICE candidate" });
    }
  });

  socket.on("start_random_call", async (data) => {
    try {
      const me = socket.data.userName;
      let to = extractTargetName(data);

      if (!me) return;

      const matchedPartner = activeMatches.get(me);
      if (!to) {
        to = matchedPartner || "";
      }

      if (!to || me === to) return;

      if (matchedPartner !== to || activeMatches.get(to) !== me) {
        return socket.emit("error_msg", {
          message: "Random voice call is not available for this user"
        });
      }

      const myChatType = getUserPreferredChatType(me);
      const partnerChatType = getUserPreferredChatType(to);

      if (myChatType !== "voice" || partnerChatType !== "voice") {
        return socket.emit("error_msg", {
          message: "Random voice call requires voice match"
        });
      }

      const targetSocket = await getUserSocket(to);
      if (!targetSocket) {
        return socket.emit("random_call_offline", { to });
      }

      if (isUserBusyForCall(me) || isUserBusyForCall(to)) {
        return socket.emit("random_call_busy", { to });
      }

      removeFromQueue(me);
      removeFromQueue(to);
      lockUserSearch(me);
      lockUserSearch(to);
      clearVoiceMatchGrace(me);
      clearVoiceMatchGrace(to);
      createPendingRandomCall(me, to);

      const myProfile = await getFullUserProfile(me);

      const incomingCallPayload = {
        from: myProfile?.userName || me,
        fromId: me,
        friendId: me,
        friendName: myProfile?.userName || me,
      };

      targetSocket.emit("incoming_random_call", incomingCallPayload);
      socket.emit("random_call_ringing", { to });

      logInfo("Call", `Outgoing random call from ${me} to ${to}`, {
        chatType: "voice"
      });
    } catch (err) {
      logInfo("Error", "start_random_call failed", err);
      socket.emit("error_msg", { message: "Failed to start random call" });
    }
  });

  socket.on("accept_random_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = extractTargetName(data);
      if (!me || !from) return;

      const key = pendingRandomCallByUser.get(me);
      if (!key) return;

      const pending = pendingRandomCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.caller === from && pending.callee === me) ||
        (pending.caller === me && pending.callee === from);

      if (!validPair) return;

      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      pendingRandomCalls.delete(key);
      pendingRandomCallByUser.delete(pending.caller);
      pendingRandomCallByUser.delete(pending.callee);
      clearVoiceMatchGrace(pending.caller);
      clearVoiceMatchGrace(pending.callee);

      activeRandomCalls.set(pending.caller, pending.callee);
      activeRandomCalls.set(pending.callee, pending.caller);

      await emitToUser(pending.caller, "random_call_accepted", {
        by: pending.callee
      });
      await emitToUser(pending.caller, "random_call_connected", {
        with: pending.callee
      });
      await emitToUser(pending.callee, "random_call_connected", {
        with: pending.caller
      });

      logInfo("Call", `Random call connected between ${pending.caller} and ${pending.callee}`);
    } catch (err) {
      logInfo("Error", "accept_random_call failed", err);
    }
  });

  socket.on("reject_random_call", async (data) => {
    try {
      const me = socket.data.userName;
      const from = extractTargetName(data);
      if (!me || !from) return;

      const key = pendingRandomCallByUser.get(me);
      if (!key) return;

      const pending = pendingRandomCalls.get(key);
      if (!pending) return;

      const validPair =
        (pending.caller === from && pending.callee === me) ||
        (pending.caller === me && pending.callee === from);

      if (!validPair) return;

      if (pending.timeoutId) {
        clearTimeout(pending.timeoutId);
      }

      pendingRandomCalls.delete(key);
      pendingRandomCallByUser.delete(pending.caller);
      pendingRandomCallByUser.delete(pending.callee);
      unlockUserSearch(pending.caller);
      unlockUserSearch(pending.callee);
      clearVoiceMatchGrace(pending.caller);
      clearVoiceMatchGrace(pending.callee);

      await emitToUser(from, "random_call_rejected", { by: me });
      await emitToUser(me, "random_call_ended", { reason: "rejected" });
    } catch (err) {
      logInfo("Error", "reject_random_call failed", err);
    }
  });

  socket.on("end_random_call", async () => {
    try {
      const me = socket.data.userName;
      if (!me) return;

      await clearRandomCallStateForUser(me, "ended");
    } catch (err) {
      logInfo("Error", "end_random_call failed", err);
    }
  });

  socket.on("random_webrtc_offer", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const sdp = data?.sdp;
      const type = data?.type;

      if (!me || !to || !sdp || !type) return;

      const activePartner = activeRandomCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", {
          message: "Random call is not active"
        });
      }

      await emitToUser(to, "random_webrtc_offer", {
        from: me,
        sdp,
        type,
      });
    } catch (err) {
      logInfo("Error", "random_webrtc_offer failed", err);
      socket.emit("error_msg", { message: "Failed to relay random offer" });
    }
  });

  socket.on("random_webrtc_answer", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const sdp = data?.sdp;
      const type = data?.type;

      if (!me || !to || !sdp || !type) return;

      const activePartner = activeRandomCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", {
          message: "Random call is not active"
        });
      }

      await emitToUser(to, "random_webrtc_answer", {
        from: me,
        sdp,
        type,
      });
    } catch (err) {
      logInfo("Error", "random_webrtc_answer failed", err);
      socket.emit("error_msg", { message: "Failed to relay random answer" });
    }
  });

  socket.on("random_webrtc_ice_candidate", async (data) => {
    try {
      const me = socket.data.userName;
      const to = extractTargetName(data);
      const candidate = data?.candidate;

      if (!me || !to || !candidate) return;

      const activePartner = activeRandomCalls.get(me);
      if (activePartner !== to) {
        return socket.emit("error_msg", {
          message: "Random call is not active"
        });
      }

      await emitToUser(to, "random_webrtc_ice_candidate", {
        from: me,
        candidate,
      });
    } catch (err) {
      logInfo("Error", "random_webrtc_ice_candidate failed", err);
      socket.emit("error_msg", { message: "Failed to relay random ICE candidate" });
    }
  });

  socket.on("disconnect", async () => {
    const me = socket.data.userName;
    logInfo("Network", `Socket disconnected: ${socket.id} (User: ${me || "Guest"})`);

    logInfo("DEBUG", "disconnect cleanup starting", {
      user: me,
      socketId: socket.id,
      chatType: socket.data.chatType || null,
      activePartner: me ? activeMatches.get(me) || null : null,
      pendingKey: me ? pendingMatchByUser.get(me) || null : null,
      locked: me ? isUserLocked(me) : false,
    });

    if (me) {
      const preservedChatType = normalizeChatType(
        socket.data.chatType || getUserPreferredChatType(me)
      );

      await clearCallStateForUser(me, "partner_disconnected");
      await clearRandomCallStateForUser(me, "partner_disconnected");
      await clearUserBusyState(me, "partner_disconnected", preservedChatType);

      const typingKeysToDelete = [];
      for (const [typingKey, timeoutId] of userTypingTimeout.entries()) {
        if (typingKey.includes(`__${me}`)) {
          clearTimeout(timeoutId);
          typingKeysToDelete.push(typingKey);

          const pairWithoutSuffix = typingKey.split("__")[0] || "";
          const users = pairWithoutSuffix.split("__").map(normalizeName);
          const otherUser = users.find((u) => u && u !== normalizeName(me));

          if (otherUser) {
            await emitToUser(otherUser, "private_typing", {
              from: me,
              fromId: me,
              to: otherUser,
              toId: otherUser,
              isTyping: false
            });
          }
        }
      }
      for (const key of typingKeysToDelete) {
        userTypingTimeout.delete(key);
      }

      const currentSocketId = userToSocket.get(me);
      if (currentSocketId === socket.id) {
        userToSocket.delete(me);

        await User.findOneAndUpdate(
          {
            $or: [
              { userName: me },
              { userId: me }
            ]
          },
          {
            $set: {
              online: false,
              lastSeen: new Date(),
              socketId: null
            }
          },
          { returnDocument: "after" }
        );

        await notifyFriendsStatusChanged(me);
      }
    }

    socketToUser.delete(socket.id);
  });
});

// ==========================================
// 7. API
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
      queueSize: getAllQueueSizes(),
      textQueueSize: waitingQueues.text.length,
      voiceQueueSize: waitingQueues.voice.length,
      ongoingChats: activeMatches.size / 2,
      pendingProposals: pendingMatches.size,
      activeCalls: activeCalls.size / 2,
      pendingCalls: pendingCalls.size,
      activeRandomCalls: activeRandomCalls.size / 2,
      pendingRandomCalls: pendingRandomCalls.size
    },
    system: {
      uptime: process.uptime(),
      memory: `${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2)} MB`,
      platform: process.platform
    }
  });
});

// ==========================================
// 8. تشغيل السيرفر
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

process.on("unhandledRejection", (reason) => {
  logInfo("Critical", "Unhandled Promise Rejection detected", reason);
});

process.on("uncaughtException", (err) => {
  logInfo("Critical", "Uncaught Exception detected! System remains stable.", err);
});
