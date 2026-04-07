const io = require("socket.io")(process.env.PORT || 3000, {
  cors: { origin: "*" },
});

// =========================
// Debug helpers
// =========================
const DEBUG = true;

function log(...args) {
  if (DEBUG) {
    console.log(new Date().toISOString(), ...args);
  }
}

// =========================
// التخزين داخل الذاكرة
// =========================

// المستخدمون المنتظرون للمطابقة
let waitingUsers = [];

// socketId => partnerSocketId
const activeChats = new Map();

// username => Set(socketId)
const onlineUsers = new Map();

// username => ISO string
const lastSeenMap = new Map();

// username => Set(friendName)
const friendsMap = new Map();

// targetUsername => Set(senderUsername)
const pendingFriendRequests = new Map();

// socketId => username
const socketToUsername = new Map();

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
  const before = waitingUsers.length;
  waitingUsers = waitingUsers.filter((id) => id !== socketId);
  const after = waitingUsers.length;
  if (before !== after) {
    log("[WAITING] Removed socket from waiting list:", socketId);
  }
}

function addSocketForUser(username, socketId) {
  ensureSet(onlineUsers, username).add(socketId);
  socketToUsername.set(socketId, username);
  log(
    "[ONLINE] addSocketForUser:",
    username,
    "socket:",
    socketId,
    "total sockets:",
    onlineUsers.get(username).size
  );
}

function removeSocketForUser(username, socketId) {
  if (!onlineUsers.has(username)) return;

  const userSockets = onlineUsers.get(username);
  userSockets.delete(socketId);
  socketToUsername.delete(socketId);

  log(
    "[ONLINE] removeSocketForUser:",
    username,
    "socket:",
    socketId,
    "remaining sockets:",
    userSockets.size
  );

  if (userSockets.size === 0) {
    onlineUsers.delete(username);
    lastSeenMap.set(username, new Date().toISOString());
    log("[ONLINE] user offline بالكامل:", username);
  }
}

function isUserOnline(username) {
  return onlineUsers.has(username) && onlineUsers.get(username).size > 0;
}

function emitToUser(username, event, data) {
  const userSockets = onlineUsers.get(username);

  if (!userSockets || userSockets.size === 0) {
    log("[EMIT] user offline, cannot emit:", event, "to", username);
    return false;
  }

  log(
    "[EMIT] sending event:",
    event,
    "to user:",
    username,
    "socket count:",
    userSockets.size
  );

  userSockets.forEach((socketId) => {
    io.to(socketId).emit(event, data);
    log("[EMIT] -> socket:", socketId, "event:", event);
  });

  return true;
}

function addFriendship(userA, userB) {
  ensureSet(friendsMap, userA).add(userB);
  ensureSet(friendsMap, userB).add(userA);
  log("[FRIENDS] addFriendship:", userA, "<->", userB);
}

function removeFriendship(userA, userB) {
  if (friendsMap.has(userA)) friendsMap.get(userA).delete(userB);
  if (friendsMap.has(userB)) friendsMap.get(userB).delete(userA);
  log("[FRIENDS] removeFriendship:", userA, "X", userB);
}

function areFriends(userA, userB) {
  return friendsMap.has(userA) && friendsMap.get(userA).has(userB);
}

function sendFriendStatusToSocket(socket, friendName) {
  const payload = {
    user: friendName,
    online: isUserOnline(friendName),
    lastSeen: lastSeenMap.get(friendName) || "غير معروف",
  };

  log("[STATUS] sendFriendStatusToSocket:", socket.username, "gets", payload);
  socket.emit("update_status", payload);
}

function notifyFriendsStatusChange(username) {
  const myFriends = friendsMap.get(username);
  if (!myFriends) return;

  const payload = {
    user: username,
    online: isUserOnline(username),
    lastSeen: lastSeenMap.get(username) || "غير معروف",
  };

  log("[STATUS] notifyFriendsStatusChange:", username, payload);

  myFriends.forEach((friendName) => {
    emitToUser(friendName, "update_status", payload);
  });
}

function getPartnerSocketId(socketId) {
  return activeChats.get(socketId) || null;
}

function clearChatForSocket(socketId) {
  const partnerId = getPartnerSocketId(socketId);
  if (!partnerId) return null;

  activeChats.delete(socketId);
  activeChats.delete(partnerId);

  log("[CHAT] clearChatForSocket:", socketId, "partner:", partnerId);
  return partnerId;
}

function endChatForSocket(socket, options = {}) {
  const { notifyPartner = null, notifySelf = null } = options;

  const partnerId = clearChatForSocket(socket.id);

  if (!partnerId) {
    if (notifySelf) socket.emit("system_msg", notifySelf);
    return null;
  }

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

function cleanupPendingRequestsWith(username) {
  pendingFriendRequests.forEach((senders, target) => {
    if (senders.has(username)) {
      senders.delete(username);
      log("[REQUESTS] cleanup pending request from", username, "for target", target);
    }
  });
}

// ✅ المطابقة الآن تمنع لقاء الأصدقاء
function matchUser(socket) {
  removeFromWaiting(socket.id);

  log("[MATCH] trying to match socket:", socket.id, "user:", socket.username);

  while (waitingUsers.length > 0) {
    const partnerId = waitingUsers.shift();
    const partnerSocket = io.sockets.sockets.get(partnerId);

    if (!partnerSocket) {
      log("[MATCH] skipped missing partner socket:", partnerId);
      continue;
    }

    if (partnerSocket.id === socket.id) {
      log("[MATCH] skipped self partner:", partnerId);
      continue;
    }

    if (activeChats.has(partnerSocket.id)) {
      log("[MATCH] skipped busy partner:", partnerId);
      continue;
    }

    const myName = socket.username || getSafeName(socket);
    const partnerName = partnerSocket.username || getSafeName(partnerSocket);

    // ✅ لا تسمح بمطابقة الأصدقاء
    if (areFriends(myName, partnerName)) {
      log("[MATCH] skipped friend pair:", myName, "<->", partnerName);

      // نعيد الشريك لقائمة الانتظار ليتم إيجاد شخص آخر له لاحقًا
      if (!waitingUsers.includes(partnerSocket.id)) {
        waitingUsers.push(partnerSocket.id);
      }
      continue;
    }

    activeChats.set(socket.id, partnerSocket.id);
    activeChats.set(partnerSocket.id, socket.id);

    log(
      "[MATCH] success:",
      socket.username,
      "(",
      socket.id,
      ") <->",
      partnerSocket.username,
      "(",
      partnerSocket.id,
      ")"
    );

    socket.emit(
      "system_msg",
      `تم العثور على صديق: ${partnerSocket.username || "مستخدم مجهول"}`
    );
    partnerSocket.emit(
      "system_msg",
      `تم العثور على صديق: ${socket.username || "مستخدم مجهول"}`
    );
    return true;
  }

  waitingUsers.push(socket.id);
  log("[MATCH] no partner found, added to waiting:", socket.id, socket.username);
  socket.emit("system_msg", "جاري البحث عن لاعب...");
  return false;
}

// =========================
// socket.io
// =========================

io.on("connection", (socket) => {
  log("[CONNECT] New connection:", socket.id);

  socket.on("register_user", (username) => {
    const safeName = getSafeName(socket, username);
    socket.username = safeName;

    ensureSet(friendsMap, safeName);
    ensureSet(pendingFriendRequests, safeName);

    addSocketForUser(safeName, socket.id);

    log("[REGISTER] user:", safeName, "socket:", socket.id);

    notifyFriendsStatusChange(safeName);
  });

  socket.on("find_partner", () => {
    if (!socket.username) {
      socket.username = getSafeName(socket);
      addSocketForUser(socket.username, socket.id);
    }

    log("[MATCH] find_partner from:", socket.username, socket.id);

    endChatForSocket(socket, {
      notifyPartner: "قام الطرف الآخر بإنهاء المحادثة وتخطيك.",
      notifySelf: "تم إنهاء المحادثة الحالية. جاري البحث عن لاعب جديد...",
    });

    matchUser(socket);
  });

  socket.on("skip_partner", () => {
    log("[CHAT] skip_partner from:", socket.username, socket.id);

    endChatForSocket(socket, {
      notifyPartner: "قام الطرف الآخر بتخطيك. تم إنهاء المحادثة.",
      notifySelf: "تم التخطي والخروج من المحادثة.",
    });

    removeFromWaiting(socket.id);
    socket.emit("system_msg", "تم التخطي. اضغط بحث عن لاعب لبدء مطابقة جديدة.");
  });

  socket.on("message", (msg) => {
    const partnerId = getPartnerSocketId(socket.id);

    log("[CHAT] message from:", socket.username, "partner socket:", partnerId);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("message", msg);
  });

  socket.on("image", (base64Image) => {
    const partnerId = getPartnerSocketId(socket.id);

    log("[CHAT] image from:", socket.username, "partner socket:", partnerId);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("image", base64Image);
  });

  socket.on("typing", () => {
    const partnerId = getPartnerSocketId(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("typing");
      log("[CHAT] typing from:", socket.username, "to partner socket:", partnerId);
    }
  });

  socket.on("stop_typing", () => {
    const partnerId = getPartnerSocketId(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("stop_typing");
      log("[CHAT] stop_typing from:", socket.username, "to partner socket:", partnerId);
    }
  });

  socket.on("send_friend_request", (senderName) => {
    const partnerId = getPartnerSocketId(socket.id);

    log("[FRIEND REQUEST] send request from:", socket.username, "partnerId:", partnerId);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب لإرسال طلب صداقة له.");
      return;
    }

    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (!partnerSocket) {
      socket.emit("system_msg", "تعذر الوصول إلى الطرف الآخر.");
      return;
    }

    const fromName = getSafeName(socket, senderName);
    const toName = getSafeName(partnerSocket);

    if (fromName === toName) return;

    if (areFriends(fromName, toName)) {
      log("[FRIEND REQUEST] already friends:", fromName, toName);
      socket.emit("system_msg", `أنت و ${toName} أصدقاء بالفعل.`);
      return;
    }

    const targetRequests = ensureSet(pendingFriendRequests, toName);

    if (targetRequests.has(fromName)) {
      log("[FRIEND REQUEST] duplicate request:", fromName, "->", toName);
      socket.emit("friend_request_sent", {
        to: toName,
        message: `طلب الصداقة إلى ${toName} مُرسل بالفعل`,
      });
      return;
    }

    targetRequests.add(fromName);
    log("[FRIEND REQUEST] stored request:", fromName, "->", toName);

    emitToUser(toName, "friend_request_received", {
      from: fromName,
      message: `${fromName} أرسل لك طلب صداقة`,
    });

    socket.emit("friend_request_sent", {
      to: toName,
      message: `تم إرسال طلب الصداقة إلى ${toName}`,
    });
  });

  socket.on("respond_friend_request", (data) => {
    const myName = getSafeName(socket);
    const fromName =
      data && typeof data.from === "string" ? data.from.trim() : "";
    const accepted = !!(data && data.accepted);

    log(
      "[FRIEND REQUEST] response:",
      "from target:",
      myName,
      "original sender:",
      fromName,
      "accepted:",
      accepted
    );

    if (!fromName) return;

    const myPending = ensureSet(pendingFriendRequests, myName);

    if (!myPending.has(fromName)) {
      log("[FRIEND REQUEST] request missing for:", fromName, "->", myName);
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

  socket.on("get_friends_status", (friendsList) => {
    log("[STATUS] get_friends_status from:", socket.username, friendsList);

    if (!Array.isArray(friendsList)) return;

    friendsList.forEach((friendName) => {
      if (typeof friendName !== "string" || !friendName.trim()) return;
      sendFriendStatusToSocket(socket, friendName.trim());
    });
  });

  socket.on("private_message", (data) => {
    if (!data || typeof data !== "object") return;

    const to = typeof data.to === "string" ? data.to.trim() : "";
    const from =
      typeof data.from === "string" && data.from.trim()
        ? data.from.trim()
        : getSafeName(socket);
    const text = typeof data.text === "string" ? data.text.trim() : "";

    log("[PRIVATE MESSAGE] incoming:", {
      from,
      to,
      text,
      fromSocket: socket.id,
      targetOnline: isUserOnline(to),
      targetSocketsCount: onlineUsers.has(to) ? onlineUsers.get(to).size : 0,
      areFriends: areFriends(from, to),
    });

    if (!to || !text) return;

    if (!areFriends(from, to)) {
      log("[PRIVATE MESSAGE] blocked, users are not friends:", from, to);
      socket.emit("system_msg", "لا يمكنك إرسال رسالة خاصة قبل إضافة المستخدم كصديق.");
      return;
    }

    const payload = {
      from,
      text,
      time: new Date().toISOString(),
    };

    const delivered = emitToUser(to, "private_message_received", payload);

    log("[PRIVATE MESSAGE] delivered:", delivered, "payload:", payload);

    if (!delivered) {
      socket.emit("system_msg", `${to} غير متصل الآن.`);
    }
  });

  socket.on("delete_friend", (friendNameRaw) => {
    const myName = getSafeName(socket);
    const friendName =
      typeof friendNameRaw === "string" ? friendNameRaw.trim() : "";

    log("[FRIENDS] delete_friend request:", myName, "->", friendName);

    if (!friendName) return;

    if (!areFriends(myName, friendName)) {
      socket.emit("friend_deleted_successfully", {
        name: friendName,
        message: `${friendName} غير موجود أصلًا في قائمة أصدقائك`,
      });
      return;
    }

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
      online: isUserOnline(myName),
      lastSeen: lastSeenMap.get(myName) || "غير معروف",
    });
  });

  socket.on("disconnect", () => {
    log("[DISCONNECT] socket:", socket.id, "username:", socket.username);

    removeFromWaiting(socket.id);

    const partnerId = clearChatForSocket(socket.id);
    if (partnerId) {
      io.to(partnerId).emit(
        "system_msg",
        "انقطع اتصال الطرف الآخر. تم إنهاء المحادثة."
      );
      io.to(partnerId).emit("stop_typing");
      log("[DISCONNECT] chat cleared with partner:", partnerId);
    }

    const username = socketToUsername.get(socket.id) || socket.username;
    if (username) {
      removeSocketForUser(username, socket.id);
      cleanupPendingRequestsWith(username);
      notifyFriendsStatusChange(username);
    }
  });
});
