const io = require("socket.io")(process.env.PORT || 3000, {
  cors: { origin: "*" },
});

// =========================
// التخزين داخل الذاكرة
// =========================

// المستخدمون المنتظرون للمطابقة العشوائية
let waitingUsers = [];

// ربط كل socket بشريكه في الدردشة العشوائية
// socketId => partnerSocketId
const activeChats = new Map();

// المستخدمون المتصلون الآن
// username => socketId
const onlineUsers = new Map();

// آخر ظهور
// username => ISO string
const lastSeenMap = new Map();

// قائمة الأصدقاء
// username => Set(friendName)
const friendsMap = new Map();

// طلبات الصداقة المعلقة
// targetUsername => Set(senderUsername)
const pendingFriendRequests = new Map();

// =========================
// دوال مساعدة
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

function ensureUserSet(map, username) {
  if (!map.has(username)) {
    map.set(username, new Set());
  }
  return map.get(username);
}

function removeFromWaiting(socketId) {
  waitingUsers = waitingUsers.filter((id) => id !== socketId);
}

function endActiveChatFor(socket, reasonForPartner = null, reasonForSelf = null) {
  const partnerId = activeChats.get(socket.id);

  if (!partnerId) {
    if (reasonForSelf) {
      socket.emit("system_msg", reasonForSelf);
    }
    return null;
  }

  const partnerSocket = io.sockets.sockets.get(partnerId);

  activeChats.delete(socket.id);
  activeChats.delete(partnerId);

  if (partnerSocket && reasonForPartner) {
    partnerSocket.emit("system_msg", reasonForPartner);
    partnerSocket.emit("stop_typing");
  }

  socket.emit("stop_typing");

  if (reasonForSelf) {
    socket.emit("system_msg", reasonForSelf);
  }

  return partnerSocket || null;
}

function sendFriendStatusTo(socket, friendName) {
  const online = onlineUsers.has(friendName);
  const lastSeen = lastSeenMap.get(friendName) || "غير معروف";

  socket.emit("update_status", {
    user: friendName,
    online,
    lastSeen,
  });
}

function notifyFriendStatusChange(username) {
  const myFriends = friendsMap.get(username);
  if (!myFriends) return;

  myFriends.forEach((friendName) => {
    const friendSocketId = onlineUsers.get(friendName);
    if (!friendSocketId) return;

    io.to(friendSocketId).emit("update_status", {
      user: username,
      online: onlineUsers.has(username),
      lastSeen: lastSeenMap.get(username) || "غير معروف",
    });
  });
}

function addFriendship(userA, userB) {
  ensureUserSet(friendsMap, userA).add(userB);
  ensureUserSet(friendsMap, userB).add(userA);
}

function removeFriendship(userA, userB) {
  if (friendsMap.has(userA)) {
    friendsMap.get(userA).delete(userB);
  }
  if (friendsMap.has(userB)) {
    friendsMap.get(userB).delete(userA);
  }
}

function getPartnerSocket(socket) {
  const partnerId = activeChats.get(socket.id);
  if (!partnerId) return null;
  return io.sockets.sockets.get(partnerId) || null;
}

// =========================
// socket.io
// =========================

io.on("connection", (socket) => {
  console.log("New connection:", socket.id);

  // 1) تسجيل المستخدم
  socket.on("register_user", (username) => {
    const safeName = getSafeName(socket, username);

    // إذا كان الاسم متصل سابقًا من جهاز آخر، نستبدله بآخر اتصال
    const oldSocketId = onlineUsers.get(safeName);
    if (oldSocketId && oldSocketId !== socket.id) {
      const oldSocket = io.sockets.sockets.get(oldSocketId);
      if (oldSocket) {
        oldSocket.emit("system_msg", "تم تسجيل الدخول من جهاز آخر.");
        oldSocket.disconnect(true);
      }
    }

    socket.username = safeName;
    onlineUsers.set(safeName, socket.id);

    ensureUserSet(friendsMap, safeName);
    ensureUserSet(pendingFriendRequests, safeName);

    console.log(`User registered: ${safeName} -> ${socket.id}`);

    // تحديث أصدقائه أنه أصبح متصلاً
    notifyFriendStatusChange(safeName);
  });

  // 2) البحث عن شريك / بدء المطابقة
  socket.on("find_partner", () => {
    if (!socket.username) {
      socket.username = getSafeName(socket);
      onlineUsers.set(socket.username, socket.id);
    }

    // إذا كان في محادثة حالية، أنهها للطرفين
    const previousPartner = endActiveChatFor(
      socket,
      "قام الطرف الآخر بإنهاء المحادثة وتخطيك.",
      "تم إنهاء المحادثة الحالية. جاري البحث عن لاعب جديد..."
    );

    // الطرف الآخر لا نعيده تلقائيًا للانتظار إلا إذا أردت ذلك لاحقًا
    // الآن التخطي يعني الخروج من المحادثة الحالية فقط للطرفين

    removeFromWaiting(socket.id);

    // ابحث عن شريك صالح
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
    const partnerSocket = endActiveChatFor(
      socket,
      "قام الطرف الآخر بتخطيك. تم إنهاء المحادثة.",
      "تم التخطي والخروج من المحادثة."
    );

    removeFromWaiting(socket.id);
    socket.emit("system_msg", "تم التخطي. اضغط بحث عن لاعب لبدء مطابقة جديدة.");
  });

  // 4) الرسائل النصية في الدردشة العشوائية
  socket.on("message", (msg) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("message", msg);
  });

  // 5) الصور في الدردشة العشوائية
  socket.on("image", (base64Image) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد لاعب متصل معك حاليًا.");
      return;
    }

    io.to(partnerId).emit("image", base64Image);
  });

  // 6) حالة الكتابة
  socket.on("typing", () => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("typing");
    }
  });

  socket.on("stop_typing", () => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("stop_typing");
    }
  });

  // 7) إرسال طلب صداقة أثناء الدردشة العشوائية
  socket.on("send_friend_request", (senderName) => {
    const partnerSocket = getPartnerSocket(socket);

    if (!partnerSocket) {
      socket.emit("system_msg", "لا يوجد لاعب لإرسال طلب صداقة له.");
      return;
    }

    const fromName = getSafeName(socket, senderName);
    const toName = getSafeName(partnerSocket);

    if (fromName === toName) return;

    // إذا كانوا أصدقاء أصلًا
    if (friendsMap.has(fromName) && friendsMap.get(fromName).has(toName)) {
      socket.emit("system_msg", `أنت و ${toName} أصدقاء بالفعل.`);
      return;
    }

    // سجل الطلب كمعلق
    ensureUserSet(pendingFriendRequests, toName).add(fromName);

    // أرسل إشعار للطرف الآخر
    partnerSocket.emit("friend_request_received", {
      from: fromName,
      message: `${fromName} أرسل لك طلب صداقة`,
    });

    socket.emit("friend_request_sent", {
      to: toName,
      message: `تم إرسال طلب الصداقة إلى ${toName}`,
    });
  });

  // 8) الرد على طلب الصداقة: قبول / رفض
  socket.on("respond_friend_request", (data) => {
    const myName = getSafeName(socket);
    const fromName =
      data && typeof data.from === "string" ? data.from.trim() : "";
    const accepted = !!(data && data.accepted);

    if (!fromName) return;

    const myPending = ensureUserSet(pendingFriendRequests, myName);

    if (!myPending.has(fromName)) {
      socket.emit("system_msg", "هذا الطلب لم يعد موجودًا.");
      return;
    }

    myPending.delete(fromName);

    const senderSocketId = onlineUsers.get(fromName);

    if (accepted) {
      addFriendship(myName, fromName);

      socket.emit("friend_added_successfully", fromName);

      if (senderSocketId) {
        io.to(senderSocketId).emit("friend_added_successfully", myName);
      }

      socket.emit("friend_request_response", {
        from: fromName,
        accepted: true,
        message: `أنت الآن صديق لـ ${fromName}`,
      });

      if (senderSocketId) {
        io.to(senderSocketId).emit("friend_request_response", {
          from: myName,
          accepted: true,
          message: `تم قبول طلب صداقتك من ${myName}`,
        });
      }

      // تحديث الحالة للطرفين إذا كانوا متصلين
      sendFriendStatusTo(socket, fromName);
      if (senderSocketId) {
        io.to(senderSocketId).emit("update_status", {
          user: myName,
          online: true,
          lastSeen: lastSeenMap.get(myName) || "غير معروف",
        });
      }
    } else {
      socket.emit("friend_request_response", {
        from: fromName,
        accepted: false,
        message: `تم رفض طلب الصداقة من ${fromName}`,
      });

      if (senderSocketId) {
        io.to(senderSocketId).emit("friend_request_response", {
          from: myName,
          accepted: false,
          message: `${myName} رفض طلب الصداقة`,
        });
      }
    }
  });

  // 9) جلب قائمة الأصدقاء
  socket.on("get_friends_list", () => {
    const myName = getSafeName(socket);
    const list = Array.from(friendsMap.get(myName) || []);

    socket.emit("friends_list", list);
  });

  // 10) جلب حالات الأصدقاء
  socket.on("get_friends_status", (friendsList) => {
    if (!Array.isArray(friendsList)) return;

    friendsList.forEach((friendName) => {
      if (typeof friendName !== "string" || !friendName.trim()) return;
      sendFriendStatusTo(socket, friendName.trim());
    });
  });

  // 11) الرسائل الخاصة بين الأصدقاء
  socket.on("private_message", (data) => {
    if (!data || typeof data !== "object") return;

    const to = typeof data.to === "string" ? data.to.trim() : "";
    const from =
      typeof data.from === "string" && data.from.trim()
        ? data.from.trim()
        : getSafeName(socket);
    const text = typeof data.text === "string" ? data.text.trim() : "";

    if (!to || !text) return;

    // اسمح فقط إذا كانوا أصدقاء
    if (!friendsMap.has(from) || !friendsMap.get(from).has(to)) {
      socket.emit("system_msg", "لا يمكنك إرسال رسالة خاصة قبل إضافة المستخدم كصديق.");
      return;
    }

    const targetSocketId = onlineUsers.get(to);

    if (targetSocketId) {
      io.to(targetSocketId).emit("private_message_received", {
        from,
        text,
      });
    } else {
      socket.emit("system_msg", `${to} غير متصل الآن.`);
    }
  });

  // 12) حذف صديق
  socket.on("delete_friend", (friendNameRaw) => {
    const myName = getSafeName(socket);
    const friendName =
      typeof friendNameRaw === "string" ? friendNameRaw.trim() : "";

    if (!friendName) return;

    removeFriendship(myName, friendName);

    socket.emit("friend_deleted_successfully", {
      name: friendName,
      message: `تم حذف ${friendName} من قائمة الأصدقاء`,
    });

    const friendSocketId = onlineUsers.get(friendName);
    if (friendSocketId) {
      io.to(friendSocketId).emit("friend_deleted_me", {
        from: myName,
        message: `${myName} حذفك من قائمة أصدقائه`,
      });

      io.to(friendSocketId).emit("update_status", {
        user: myName,
        online: false,
        lastSeen: lastSeenMap.get(myName) || "غير معروف",
      });
    }
  });

  // 13) عند قطع الاتصال
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);

    removeFromWaiting(socket.id);

    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);

      activeChats.delete(socket.id);
      activeChats.delete(partnerId);

      if (partnerSocket) {
        partnerSocket.emit(
          "system_msg",
          "انقطع اتصال الطرف الآخر. تم إنهاء المحادثة."
        );
        partnerSocket.emit("stop_typing");
      }
    }

    if (socket.username) {
      onlineUsers.delete(socket.username);
      lastSeenMap.set(socket.username, new Date().toISOString());
      notifyFriendStatusChange(socket.username);
    }
  });
});
