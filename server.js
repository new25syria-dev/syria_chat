const io = require("socket.io")(process.env.PORT || 3000, {
  cors: { origin: "*" },
});

// المستخدمون المنتظرون للدردشة العشوائية
let waitingUsers = [];

// ربط كل مستخدم بشريكه في الدردشة العشوائية
const activeChats = new Map();

// تخزين المستخدمين المتصلين بالاسم
// username => socket.id
const onlineUsers = new Map();

// آخر ظهور للمستخدمين
// username => ISO date string
const lastSeenMap = new Map();

io.on("connection", (socket) => {
  console.log("New connection:", socket.id);

  // 1) تسجيل المستخدم
  socket.on("register_user", (username) => {
    const safeName =
      typeof username === "string" && username.trim()
        ? username.trim()
        : `مستخدم_${socket.id.slice(0, 5)}`;

    socket.username = safeName;
    onlineUsers.set(safeName, socket.id);

    console.log(`User registered: ${safeName} -> ${socket.id}`);
  });

  // 2) البحث عن شريك / التخطي
  socket.on("find_partner", () => {
    // إذا كان في محادثة حالية، افصلها أولاً
    const currentPartnerId = activeChats.get(socket.id);
    if (currentPartnerId) {
      const partnerSocket = io.sockets.sockets.get(currentPartnerId);

      if (partnerSocket) {
        partnerSocket.emit(
          "system_msg",
          "قام الطرف الآخر بإنهاء المحادثة وتخطيك."
        );

        activeChats.delete(partnerSocket.id);

        if (!waitingUsers.includes(partnerSocket.id)) {
          waitingUsers.push(partnerSocket.id);
          partnerSocket.emit("system_msg", "جاري البحث عن صديق جديد لك...");
        }
      }

      activeChats.delete(socket.id);
    }

    // إزالة المستخدم من الانتظار إن كان موجودًا
    waitingUsers = waitingUsers.filter((id) => id !== socket.id);

    // إيجاد شريك
    if (waitingUsers.length > 0) {
      const partnerId = waitingUsers.shift();
      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (partnerSocket && partnerSocket.id !== socket.id) {
        activeChats.set(socket.id, partnerId);
        activeChats.set(partnerId, socket.id);

        socket.emit(
          "system_msg",
          `تم العثور على صديق: ${partnerSocket.username || "مستخدم مجهول"}`
        );
        partnerSocket.emit(
          "system_msg",
          `تم العثور على صديق: ${socket.username || "مستخدم مجهول"}`
        );
      } else {
        waitingUsers.push(socket.id);
        socket.emit("system_msg", "جاري البحث عن صديق...");
      }
    } else {
      waitingUsers.push(socket.id);
      socket.emit("system_msg", "جاري البحث عن صديق...");
    }
  });

  // 3) رسائل الدردشة العشوائية
  socket.on("message", (msg) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد طرف آخر لاستلام رسالتك.");
      return;
    }

    io.to(partnerId).emit("message", msg);
  });

  // 4) إرسال صورة في الدردشة العشوائية
  socket.on("image", (base64Image) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) {
      socket.emit("system_msg", "لا يوجد طرف آخر لاستلام الصورة.");
      return;
    }

    io.to(partnerId).emit("image", base64Image);
  });

  // 5) حالة الكتابة في الدردشة العشوائية
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

  // 6) طلبات الصداقة
  socket.on("send_friend_request", (senderName) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) return;

    io.to(partnerId).emit("friend_request_received", {
      name: senderName || socket.username || "مستخدم مجهول",
    });
  });

  socket.on("accept_friend", (data) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) return;

    const myName =
      data && typeof data.myName === "string" && data.myName.trim()
        ? data.myName.trim()
        : socket.username || "مستخدم مجهول";

    const partnerName =
      data && typeof data.partnerName === "string" && data.partnerName.trim()
        ? data.partnerName.trim()
        : "مستخدم مجهول";

    socket.emit("friend_added_successfully", partnerName);
    io.to(partnerId).emit("friend_added_successfully", myName);
  });

  // 7) الرسائل الخاصة بين الأصدقاء
  socket.on("private_message", (data) => {
    if (!data || typeof data !== "object") return;

    const to = typeof data.to === "string" ? data.to.trim() : "";
    const from =
      typeof data.from === "string" && data.from.trim()
        ? data.from.trim()
        : socket.username || "مستخدم مجهول";
    const text = typeof data.text === "string" ? data.text : "";

    if (!to || !text.trim()) return;

    const targetSocketId = onlineUsers.get(to);

    if (targetSocketId) {
      io.to(targetSocketId).emit("private_message_received", {
        from,
        text,
      });
    }
  });

  // 8) جلب حالات الأصدقاء
  socket.on("get_friends_status", (friendsList) => {
    if (!Array.isArray(friendsList)) return;

    friendsList.forEach((friendName) => {
      if (typeof friendName !== "string" || !friendName.trim()) return;

      const online = onlineUsers.has(friendName);
      const lastSeen = lastSeenMap.get(friendName) || "غير معروف";

      socket.emit("update_status", {
        user: friendName,
        online,
        lastSeen,
      });
    });
  });

  // 9) قطع الاتصال
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);

    // إزالة من قائمة الانتظار
    waitingUsers = waitingUsers.filter((id) => id !== socket.id);

    // إنهاء أي دردشة عشوائية حالية
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (partnerSocket) {
        partnerSocket.emit(
          "system_msg",
          "انقطع الاتصال بالطرف الآخر (غادر التطبيق)."
        );
        activeChats.delete(partnerId);
      }
    }

    activeChats.delete(socket.id);

    // تحديث حالة المستخدم
    if (socket.username) {
      onlineUsers.delete(socket.username);
      lastSeenMap.set(socket.username, new Date().toISOString());
    }
  });
});
