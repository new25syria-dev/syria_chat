const io = require("socket.io")(process.env.PORT || 3000, {
  cors: { origin: "*" },
});

// =========================
// التخزين داخل الذاكرة
// =========================

let waitingUsers = [];
const activeChats = new Map(); // socketId => partnerSocketId

// username => Set(socketId)
const onlineUsers = new Map();

// username => ISO string
const lastSeenMap = new Map();

// username => Set(friendName)
const friendsMap = new Map();

// targetUsername => Set(senderUsername)
const pendingFriendRequests = new Map();

// =========================
// Helpers
// =========================

function getSafeName(socket, providedName) {
  if (typeof providedName === "string" && providedName.trim()) {
    return providedName.trim();
  }
  if (socket.username && socket.username.trim()) {
    return socket.username.trim();
  }
  return `مستخدم_${socket.id.slice(0, 5)}`;
}

function ensureSet(map, key) {
  if (!map.has(key)) {
    map.set(key, new Set());
  }
  return map.get(key);
}

function removeFromWaiting(socketId) {
  waitingUsers = waitingUsers.filter((id) => id !== socketId);
}

function addSocketForUser(username, socketId) {
  ensureSet(onlineUsers, username).add(socketId);
}

function removeSocketForUser(username, socketId) {
  if (!onlineUsers.has(username)) return;

  const set = onlineUsers.get(username);
  set.delete(socketId);

  if (set.size === 0) {
    onlineUsers.delete(username);
    lastSeenMap.set(username, new Date().toISOString());
  }
}

function emitToUser(username, event, data) {
  const sockets = onlineUsers.get(username);
  if (!sockets) return false;

  sockets.forEach((socketId) => {
    io.to(socketId).emit(event, data);
  });

  return true;
}

function isUserOnline(username) {
  return onlineUsers.has(username) && onlineUsers.get(username).size > 0;
}

function sendFriendStatusToSocket(socket, friendName) {
  socket.emit("update_status", {
    user: friendName,
    online: isUserOnline(friendName),
    lastSeen: lastSeenMap.get(friendName) || "غير معروف",
  });
}

function notifyFriendsStatusChange(username) {
  const myFriends = friendsMap.get(username);
  if (!myFriends) return;

  myFriends.forEach((friendName) => {
    emitToUser(friendName, "update_status", {
      user: username,
      online: isUserOnline(username),
      lastSeen: lastSeenMap.get(username) || "غير معروف",
    });
  });
}

function addFriendship(userA, userB) {
  ensureSet(friendsMap, userA).add(userB);
  ensureSet(friendsMap, userB).add(userA);
}

function removeFriendship(userA, userB) {
  if (friendsMap.has(userA)) friendsMap.get(userA).delete(userB);
  if (friendsMap.has(userB)) friendsMap.get(userB).delete(userA);
}

function getPartnerSocketId(socketId) {
  return activeChats.get(socketId) || null;
}

function endChatForSocket(socket, options = {}) {
  const {
    notifyPartner = null,
    notifySelf = null,
  } = options;

  const partnerId = getPartnerSocketId(socket.id);
  if (!partnerId) {
    if (notifySelf) socket.emit("system_msg", notifySelf);
    return null;
  }

  activeChats.delete(socket.id);
  activeChats.delete(partnerId);

  if (notifyPartner) {
    io.to(partnerId).emit("system_msg", notifyPartner);
    io.to(partnerId).emit("stop_typing");
  }

  socket.emit("stop_typing");

  if (notifySelf) {
    socket.emit("system_msg", notifySelf);
  }

  return partnerId;
}

// =========================
// Socket.io
// =========================

io.on("connection", (socket) => {
  console.log("New connection:", socket.id);

  // 1) تسجيل المستخدم
  socket.on("register_user", (username) => {
    const safeName = getSafeName(socket, username);
    socket.username = safeName;

    addSocketForUser(safeName, socket.id);
    ensureSet(friendsMap, safeName);
    ensureSet(pendingFriendRequests, safeName);

    console.log(`User registered: ${safeName} -> ${socket.id}`);

    notifyFriendsStatusChange(safeName);
  });

  // 2) بدء المطابقة
  socket.on("find_partner", () => {
    if (!socket.username) {
      socket.username = getSafeName(socket);
      addSocketForUser(socket.username, socket.id);
    }

    endChatForSocket(socket, {
      notifyPartner: "قام الطرف الآخر بإنهاء المحادثة وتخطيك.",
      notifySelf: "تم إنهاء المحادثة الحالية. جاري البحث عن لاعب جديد...",
    });

    removeFromWaiting(socket.id);

    while (waitingUsers.length > 0) {
      const partnerId = waitingUsers.shift();
      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (!partnerSocket) continue;
      if (partnerSocket.id === socket.id) continue;
      if (activeChats.has(partnerSocket.id)) continue;

      activeChats.set(socket.id, partnerSocket.id);
      activeChats.set(partnerSocket.id, socket.id);

      socket.emit(
        "system_msg",
        `تم العثور على صديق: ${partnerSocket.username || "مستخدم مجهول"}`
      );
      partnerSocket.emit(
        "system_msg",
        `تم العثور على صديق: ${socket.username || "مستخدم مجهول"}`
      );
      return;
    }

    waitingUsers.push(socket.id);
    socket.emit("system_msg", "جاري البحث عن لاعب...");
  });

  // 3) التخطي
  socket.on("skip_partner", () => {
    endChatForSocket(socket, {
      notifyPartner: "قام الطرف الآخر بتخطيك. تم إنهاء المحادثة.",
      notifySelf: "تم التخطي والخروج من المحادثة.",
    });

    removeFromWaiting(socket.id);
    socket.emit("system_msg", "تم التخطي. اضغط بحث عن لاعب لبدء مطابقة جديدة.");
  });

  // 4) رسالة نصية
  socket.on("message", (msg) => {
    const partnerId = getPartnerSocketId(socket.id);
    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("message", msg);
  });

  // 5) صورة
  socket.on("image", (base64Image) => {
    const partnerId = getPartnerSocketId(socket.id);
    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("image", base64Image);
  });

  // 6) typing
  socket.on("typing", () => {
    const partnerId = getPartnerSocketId(socket.id);
    if (partnerId) io.to(partnerId).emit("typing");
  });

  socket.on("stop_typing", () => {
    const partnerId = getPartnerSocketId(socket.id);
    if (partnerId) io.to(partnerId).emit("stop_typing");
  });

  // 7) طلب صداقة
  socket.on("send_friend_request", (senderName) => {
    const partnerId = getPartnerSocketId(socket.id);
    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب لإرسال طلب صداقة له.");
      return;
    }

    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (!partnerSocket) return;

    const fromName = getSafeName(socket, senderName);
    const toName = getSafeName(partnerSocket);

    if (fromName === toName) return;

    if (friendsMap.has(fromName) && friendsMap.get(fromName).has(toName)) {
      socket.emit("system_msg", `أنت و ${toName} أصدقاء بالفعل.`);
      return;
    }

    ensureSet(pendingFriendRequests, toName).add(fromName);

    emitToUser(toName, "friend_request_received", {
      from: fromName,
      message: `${fromName} أرسل لك طلب صداقة`,
    });

    socket.emit("friend_request_sent", {
      to: toName,
      message: `تم إرسال طلب الصداقة إلى ${toName}`,
    });
  });

  // 8) قبول / رفض طلب الصداقة
  socket.on("respond_friend_request", (data) => {
    const myName = getSafeName(socket);
    const fromName =
      data && typeof data.from === "string" ? data.from.trim() : "";
    const accepted = !!(data && data.accepted);

    if (!fromName) return;

    const myPending = ensureSet(pendingFriendRequests, myName);

    if (!myPending.has(fromName)) {
      socket.emit("system_msg", "هذا الطلب لم يعد موجودًا.");
      return;
    }

    myPending.delete(fromName);

    if (accepted) {
      addFriendship(myName, fromName);

      emitToUser(myName, "friend_added_successfully", fromName);
      emitToUser(fromName, "friend_added_successfully", myName);

      emitToUser(myName, "friend_request_response", {
        from: fromName,
        accepted: true,
        message: `أنت الآن صديق لـ ${fromName}`,
      });

      emitToUser(fromName, "friend_request_response", {
        from: myName,
        accepted: true,
        message: `تم قبول طلب صداقتك من ${myName}`,
      });

      emitToUser(myName, "update_status", {
        user: fromName,
        online: isUserOnline(fromName),
        lastSeen: lastSeenMap.get(fromName) || "غير معروف",
      });

      emitToUser(fromName, "update_status", {
        user: myName,
        online: isUserOnline(myName),
        lastSeen: lastSeenMap.get(myName) || "غير معروف",
      });
    } else {
      emitToUser(myName, "friend_request_response", {
        from: fromName,
        accepted: false,
        message: `تم رفض طلب الصداقة من ${fromName}`,
      });

      emitToUser(fromName, "friend_request_response", {
        from: myName,
        accepted: false,
        message: `${myName} رفض طلب الصداقة`,
      });
    }
  });

  // 9) حالات الأصدقاء
  socket.on("get_friends_status", (friendsList) => {
    if (!Array.isArray(friendsList)) return;

    friendsList.forEach((friendName) => {
      if (typeof friendName !== "string" || !friendName.trim()) return;
      sendFriendStatusToSocket(socket, friendName.trim());
    });
  });

  // 10) الرسائل الخاصة
  socket.on("private_message", (data) => {
    if (!data || typeof data !== "object") return;

    const to = typeof data.to === "string" ? data.to.trim() : "";
    const from =
      typeof data.from === "string" && data.from.trim()
        ? data.from.trim()
        : getSafeName(socket);
    const text = typeof data.text === "string" ? data.text.trim() : "";

    if (!to || !text) return;

    if (!friendsMap.has(from) || !friendsMap.get(from).has(to)) {
      socket.emit("system_msg", "لا يمكنك إرسال رسالة خاصة قبل إضافة المستخدم كصديق.");
      return;
    }

    const delivered = emitToUser(to, "private_message_received", {
      from,
      text,
      time: new Date().toISOString(),
    });

    if (!delivered) {
      socket.emit("system_msg", `${to} غير متصل الآن.`);
    }
  });

  // 11) حذف صديق
  socket.on("delete_friend", (friendNameRaw) => {
    const myName = getSafeName(socket);
    const friendName =
      typeof friendNameRaw === "string" ? friendNameRaw.trim() : "";

    if (!friendName) return;

    removeFriendship(myName, friendName);

    emitToUser(myName, "friend_deleted_successfully", {
      name: friendName,
      message: `تم حذف ${friendName} من قائمة الأصدقاء`,
    });

    emitToUser(friendName, "friend_deleted_me", {
      from: myName,
      message: `${myName} حذفك من قائمة أصدقائه`,
    });

    emitToUser(friendName, "update_status", {
      user: myName,
      online: false,
      lastSeen: lastSeenMap.get(myName) || "غير معروف",
    });
  });

  // 12) disconnect
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);

    removeFromWaiting(socket.id);

    const partnerId = getPartnerSocketId(socket.id);
    if (partnerId) {
      activeChats.delete(socket.id);
      activeChats.delete(partnerId);

      io.to(partnerId).emit(
        "system_msg",
        "انقطع اتصال الطرف الآخر. تم إنهاء المحادثة."
      );
      io.to(partnerId).emit("stop_typing");
    }

    if (socket.username) {
      removeSocketForUser(socket.username, socket.id);
      notifyFriendsStatusChange(socket.username);
    }
  });
});
