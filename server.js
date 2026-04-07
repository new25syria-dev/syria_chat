const io = require("socket.io")(process.env.PORT || 3000, {
  cors: { origin: "*" }
});

// قائمة المستخدمين الذين ينتظرون شريكاً
let waitingUsers = [];
// قاموس لربط كل مستخدم بشريكه (لسرعة الوصول)
let activeChats = new Map();

io.on("connection", (socket) => {
  console.log("New connection:", socket.id);

  // 1. تسجيل المستخدم
  socket.on("register_user", (username) => {
    socket.username = username || "مستخدم مجهول";
    console.log(`User registered: ${socket.username}`);
  });

  // 2. نظام البحث عن شريك (مع ميزة التخطي)
  socket.on("find_partner", () => {
    // أ: إذا كان المستخدم في محادثة حالية، أخبر الطرف الآخر بالتخطّي
    const currentPartnerId = activeChats.get(socket.id);
    if (currentPartnerId) {
      const partnerSocket = io.sockets.sockets.get(currentPartnerId);
      if (partnerSocket) {
        partnerSocket.emit("system_msg", "قام الطرف الآخر بإنهاء المحادثة وتخطيك.");
        activeChats.delete(partnerSocket.id);
        // إعادة الطرف الآخر لقائمة الانتظار تلقائياً ليجد شخصاً جديداً
        if (!waitingUsers.includes(partnerSocket.id)) {
            waitingUsers.push(partnerSocket.id);
            partnerSocket.emit("system_msg", "جاري البحث عن صديق جديد لك...");
        }
      }
      activeChats.delete(socket.id);
    }

    // ب: إزالة المستخدم من قائمة الانتظار إذا كان موجوداً مسبقاً (لتجنب التكرار)
    waitingUsers = waitingUsers.filter(id => id !== socket.id);

    // ج: محاولة إيجاد شريك جديد
    if (waitingUsers.length > 0) {
      const partnerId = waitingUsers.shift();
      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (partnerSocket && partnerSocket.id !== socket.id) {
        // إنشاء الرابط في الخريطة (Map)
        activeChats.set(socket.id, partnerId);
        activeChats.set(partnerId, socket.id);

        // إرسال إشعارات النجاح
        socket.emit("system_msg", `تم العثور على صديق: ${partnerSocket.username}`);
        partnerSocket.emit("system_msg", `تم العثور على صديق: ${socket.username}`);
      } else {
        waitingUsers.push(socket.id);
        socket.emit("system_msg", "جاري البحث عن صديق...");
      }
    } else {
      waitingUsers.push(socket.id);
      socket.emit("system_msg", "جاري البحث عن صديق...");
    }
  });

  // 3. تبادل الرسائل (حل مشكلة عدم وصول الكلام)
  socket.on("message", (msg) => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      // إرسال الرسالة للطرف الآخر فقط
      io.to(partnerId).emit("message", msg);
    } else {
      socket.emit("system_msg", "لا يوجد طرف آخر لاستلام رسالتك.");
    }
  });

  // 4. طلبات الصداقة (إرسال واستقبال وقبول)
  socket.on("send_friend_request", (senderName) => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("friend_request_received", { name: senderName });
    }
  });

  socket.on("accept_friend", (data) => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      // إبلاغ الطرفين بنجاح العملية
      socket.emit("friend_added_successfully", data.partnerName); // للذي قَبِل
      io.to(partnerId).emit("friend_added_successfully", data.myName); // للذي أرْسَل
    }
  });

  // 5. عند قطع الاتصال (إغلاق التطبيق أو ضعف الإنترنت)
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit("system_msg", "انقطع الاتصال بالطرف الآخر (غادر التطبيق).");
        activeChats.delete(partnerId);
      }
    }
    // إزالته من قائمة الانتظار
    waitingUsers = waitingUsers.filter(id => id !== socket.id);
    activeChats.delete(socket.id);
  });
});
