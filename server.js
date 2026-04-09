const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

app.get("/", (req, res) => {
  res.send("Socket server is running");
});

/**
 * usersByName:
 * {
 *   "sam": { socketId: "...", online: true, lastSeen: "..." }
 * }
 */
const usersByName = new Map();

/**
 * nameBySocketId:
 * socket.id => userName
 */
const nameBySocketId = new Map();

/**
 * waitingQueue:
 * users waiting for random match
 */
const waitingQueue = [];

/**
 * activeMatches:
 * userName => partnerUserName
 */
const activeMatches = new Map();

/**
 * privateHistories:
 * conversationKey => [{ from, to, text, time }]
 */
const privateHistories = new Map();

/**
 * friendRequests:
 * toUser => [{ from, time }]
 */
const friendRequests = new Map();

/**
 * friendsMap:
 * userName => Set(friendNames)
 */
const friendsMap = new Map();

function nowIso() {
  return new Date().toISOString();
}

function normalizeName(value) {
  return String(value || "").trim();
}

function ensureFriendSet(user) {
  if (!friendsMap.has(user)) {
    friendsMap.set(user, new Set());
  }
  return friendsMap.get(user);
}

function conversationKey(a, b) {
  const x = normalizeName(a);
  const y = normalizeName(b);
  return [x, y].sort().join("__");
}

function getUserSocket(userName) {
  const record = usersByName.get(normalizeName(userName));
  if (!record || !record.socketId) return null;
  return io.sockets.sockets.get(record.socketId) || null;
}

function removeFromWaitingQueue(userName) {
  const index = waitingQueue.indexOf(userName);
  if (index !== -1) {
    waitingQueue.splice(index, 1);
  }
}

function emitSystemMessage(userName, message) {
  const socket = getUserSocket(userName);
  if (socket) {
    socket.emit("system_msg", message);
  }
}

function endMatchFor(userName, reason = "تم إنهاء المحادثة") {
  const partner = activeMatches.get(userName);
  if (!partner) return;

  activeMatches.delete(userName);
  activeMatches.delete(partner);

  emitSystemMessage(userName, reason);
  emitSystemMessage(partner, "غادر الطرف الآخر");

  console.log(`❌ Match ended: ${userName} <-> ${partner}`);
}

function startMatch(userA, userB) {
  removeFromWaitingQueue(userA);
  removeFromWaitingQueue(userB);

  activeMatches.set(userA, userB);
  activeMatches.set(userB, userA);

  emitSystemMessage(userA, `تم العثور على صديق: ${userB}`);
  emitSystemMessage(userB, `تم العثور على صديق: ${userA}`);

  const socketA = getUserSocket(userA);
  const socketB = getUserSocket(userB);

  if (socketA) {
    socketA.emit("chat_started", {
      partnerId: userB,
      partnerName: userB,
    });
  }

  if (socketB) {
    socketB.emit("chat_started", {
      partnerId: userA,
      partnerName: userA,
    });
  }

  console.log(`✅ Match started: ${userA} <-> ${userB}`);
}

function tryMatchUser(userName) {
  const cleanName = normalizeName(userName);
  if (!cleanName) return;

  if (activeMatches.has(cleanName)) {
    emitSystemMessage(cleanName, "أنت بالفعل في محادثة");
    return;
  }

  removeFromWaitingQueue(cleanName);

  const partner = waitingQueue.find((u) => u !== cleanName);

  if (partner) {
    startMatch(cleanName, partner);
  } else {
    waitingQueue.push(cleanName);
    emitSystemMessage(cleanName, "جاري البحث عن شريك...");
    console.log(`🔍 ${cleanName} added to waiting queue`);
  }
}

function savePrivateMessage(from, to, text, time) {
  const key = conversationKey(from, to);
  if (!privateHistories.has(key)) {
    privateHistories.set(key, []);
  }

  privateHistories.get(key).push({
    from,
    to,
    text,
    time,
  });
}

function sendFriendsStatus(requester, friends) {
  const requesterSocket = getUserSocket(requester);
  if (!requesterSocket) return;

  for (const friend of friends) {
    const cleanFriend = normalizeName(friend);
    if (!cleanFriend) continue;

    const friendRecord = usersByName.get(cleanFriend);
    requesterSocket.emit("update_status", {
      user: cleanFriend,
      online: !!friendRecord?.online,
      lastSeen: friendRecord?.lastSeen || "غير معروف",
    });
  }
}

io.on("connection", (socket) => {
  console.log("🟢 New socket connected:", socket.id);

  socket.on("register_user", (userNameRaw) => {
    const userName = normalizeName(userNameRaw);
    if (!userName) return;

    nameBySocketId.set(socket.id, userName);
    usersByName.set(userName, {
      socketId: socket.id,
      online: true,
      lastSeen: nowIso(),
    });

    socket.data.userName = userName;

    console.log(`✅ register_user: ${userName} -> ${socket.id}`);
  });

  socket.on("find_partner", () => {
    const userName = socket.data.userName || nameBySocketId.get(socket.id);
    if (!userName) return;

    tryMatchUser(userName);
  });

  socket.on("skip_partner", () => {
    const userName = socket.data.userName || nameBySocketId.get(socket.id);
    if (!userName) return;

    const partner = activeMatches.get(userName);
    if (partner) {
      activeMatches.delete(userName);
      activeMatches.delete(partner);

      emitSystemMessage(userName, "تم التخطي");
      emitSystemMessage(partner, "تم التخطي من الطرف الآخر");

      tryMatchUser(userName);
      tryMatchUser(partner);
    } else {
      emitSystemMessage(userName, "لا يوجد شريك لتخطيه");
    }
  });

  socket.on("leave_chat", () => {
    const userName = socket.data.userName || nameBySocketId.get(socket.id);
    if (!userName) return;
    endMatchFor(userName, "تم إنهاء المحادثة");
  });

  socket.on("message", (msg) => {
    const sender = socket.data.userName || nameBySocketId.get(socket.id);
    const partner = activeMatches.get(sender);

    if (!sender || !partner) return;
    if (!msg) return;

    const partnerSocket = getUserSocket(partner);
    if (partnerSocket) {
      partnerSocket.emit("message", String(msg));
    }

    console.log(`💬 message: ${sender} -> ${partner}: ${msg}`);
  });

  socket.on("image", (imgBase64) => {
    const sender = socket.data.userName || nameBySocketId.get(socket.id);
    const partner = activeMatches.get(sender);

    if (!sender || !partner) return;
    if (!imgBase64) return;

    const partnerSocket = getUserSocket(partner);
    if (partnerSocket) {
      partnerSocket.emit("image", imgBase64);
    }

    console.log(`🖼️ image: ${sender} -> ${partner}`);
  });

  socket.on("typing", () => {
    const sender = socket.data.userName || nameBySocketId.get(socket.id);
    const partner = activeMatches.get(sender);

    if (!sender || !partner) return;

    const partnerSocket = getUserSocket(partner);
    if (partnerSocket) {
      partnerSocket.emit("typing");
    }
  });

  socket.on("stop_typing", () => {
    const sender = socket.data.userName || nameBySocketId.get(socket.id);
    const partner = activeMatches.get(sender);

    if (!sender || !partner) return;

    const partnerSocket = getUserSocket(partner);
    if (partnerSocket) {
      partnerSocket.emit("stop_typing");
    }
  });

  socket.on("private_message", (data) => {
    try {
      const from = normalizeName(data?.from || socket.data.userName);
      const to = normalizeName(data?.to);
      const text = String(data?.text || "").trim();
      const time = data?.time || nowIso();

      if (!from || !to || !text) {
        console.log("⚠️ private_message invalid payload:", data);
        return;
      }

      const payload = { from, to, text, time };

      console.log("Incoming event: private_message", payload);

      savePrivateMessage(from, to, text, time);

      const targetSocket = getUserSocket(to);
      if (targetSocket) {
        targetSocket.emit("private_message_received", payload);
        console.log(`private_message forwarded to ${targetSocket.id}`);
      } else {
        console.log(`⚠️ target offline: ${to}`);
      }
    } catch (err) {
      console.error("private_message error:", err);
    }
  });

  socket.on("get_private_history", (data) => {
    const from = normalizeName(data?.from || socket.data.userName);
    const to = normalizeName(data?.to);

    if (!from || !to) {
      socket.emit("private_history", []);
      return;
    }

    const key = conversationKey(from, to);
    const history = privateHistories.get(key) || [];
    socket.emit("private_history", history);

    console.log(`📜 private_history sent for ${from} <-> ${to}`);
  });

  socket.on("mark_messages_read", (data) => {
    const user = normalizeName(data?.user || socket.data.userName);
    const friend = normalizeName(data?.friend || data?.friendId);

    console.log(`✅ mark_messages_read: user=${user}, friend=${friend}`);
  });

  socket.on("send_friend_request", (data) => {
    const from = normalizeName(data?.from || socket.data.userName);
    const to = normalizeName(data?.to);

    if (!from || !to || from === to) return;

    const myFriends = ensureFriendSet(from);
    if (myFriends.has(to)) {
      socket.emit("friend_request_response", {
        accepted: true,
        message: "هذا المستخدم موجود بالفعل في قائمة أصدقائك",
      });
      return;
    }

    if (!friendRequests.has(to)) {
      friendRequests.set(to, []);
    }

    const requests = friendRequests.get(to);
    const alreadySent = requests.some((r) => r.from === from);
    if (!alreadySent) {
      requests.push({ from, time: nowIso() });
    }

    socket.emit("friend_request_sent", {
      message: "تم إرسال طلب الصداقة",
    });

    const targetSocket = getUserSocket(to);
    if (targetSocket) {
      targetSocket.emit("friend_request_received", {
        from,
      });
    }

    console.log(`🤝 friend request: ${from} -> ${to}`);
  });

  socket.on("respond_friend_request", (data) => {
    const responder = socket.data.userName || nameBySocketId.get(socket.id);
    const from = normalizeName(data?.from);
    const accepted = data?.accepted === true;

    if (!responder || !from) return;

    const requests = friendRequests.get(responder) || [];
    friendRequests.set(
      responder,
      requests.filter((r) => r.from !== from)
    );

    const requesterSocket = getUserSocket(from);

    if (accepted) {
      ensureFriendSet(responder).add(from);
      ensureFriendSet(from).add(responder);

      socket.emit("friend_added_successfully", from);
      if (requesterSocket) {
        requesterSocket.emit("friend_added_successfully", responder);
        requesterSocket.emit("friend_request_response", {
          accepted: true,
          message: `وافق ${responder} على طلب الصداقة`,
        });
      }

      console.log(`✅ friendship created: ${from} <-> ${responder}`);
    } else {
      if (requesterSocket) {
        requesterSocket.emit("friend_request_response", {
          accepted: false,
          message: `رفض ${responder} طلب الصداقة`,
        });
      }

      console.log(`❌ friend request rejected: ${from} -> ${responder}`);
    }
  });

  socket.on("delete_friend", (data) => {
    const me = socket.data.userName || nameBySocketId.get(socket.id);
    const friend = normalizeName(data?.friend);

    if (!me || !friend) return;

    ensureFriendSet(me).delete(friend);
    ensureFriendSet(friend).delete(me);

    socket.emit("friend_deleted_successfully", {
      message: `تم حذف ${friend} من قائمة الأصدقاء`,
    });

    const friendSocket = getUserSocket(friend);
    if (friendSocket) {
      friendSocket.emit("friend_deleted_me", {
        from: me,
        message: `قام ${me} بحذفك من قائمة الأصدقاء`,
      });
    }

    console.log(`🗑️ friendship removed: ${me} x ${friend}`);
  });

  socket.on("get_friends_status", (friends) => {
    const requester = socket.data.userName || nameBySocketId.get(socket.id);
    if (!requester || !Array.isArray(friends)) return;

    sendFriendsStatus(requester, friends);
  });

  socket.on("disconnect", () => {
    const userName = socket.data.userName || nameBySocketId.get(socket.id);

    console.log("🔴 Disconnected socket:", socket.id, "user:", userName);

    if (userName) {
      usersByName.set(userName, {
        socketId: null,
        online: false,
        lastSeen: nowIso(),
      });

      removeFromWaitingQueue(userName);

      const partner = activeMatches.get(userName);
      if (partner) {
        activeMatches.delete(userName);
        activeMatches.delete(partner);
        emitSystemMessage(partner, "انقطع اتصال الطرف الآخر");
      }
    }

    nameBySocketId.delete(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
