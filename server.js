const port = process.env.PORT || 3000;
const io = require("socket.io")(port, {
    cors: {
        origin: "*", // السماح بجميع الاتصالات من تطبيقات الموبايل
        methods: ["GET", "POST"]
    }
});

console.log(`السيرفر يعمل الآن على المنفذ: ${port}`);

let waitingUser = null;

io.on("connection", (socket) => {
    console.log("مستخدم جديد متصل: " + socket.id);

    // 1. البحث عن شريك (عشوائي)
    socket.on("find_partner", () => {
        // التأكد أن المستخدم ليس هو نفسه المنتظر
        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            // إنشاء غرفة فريدة للمستخدمين
            const roomId = `room_${partner.id}_${socket.id}`;
            
            socket.join(roomId);
            partner.join(roomId);

            socket.roomId = roomId;
            partner.roomId = roomId;

            // إرسال إشعار للطرفين ببدء الدردشة
            io.to(roomId).emit("system_msg", "تم العثور على شريك! يمكنك البدء بالدردشة الآن 🇸🇾");
            console.log(`تم الربط بين ${socket.id} و ${partner.id} في غرفة: ${roomId}`);
        } else {
            waitingUser = socket;
            socket.emit("system_msg", "جاري البحث عن شريك متاح... يرجى الانتظار.");
        }
    });

    // 2. استقبال وإرسال الرسائل
    socket.on("message", (msg) => {
        if (socket.roomId) {
            // إرسال الرسالة للطرف الآخر في نفس الغرفة فقط
            socket.to(socket.roomId).emit("message", msg);
        }
    });

    // 3. نظام طلب الصداقة
    socket.on("send_friend_request", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_request_received", { from: socket.id });
            console.log(`طلب صداقة من ${socket.id}`);
        }
    });

    // 4. قبول طلب الصداقة
    socket.on("accept_friend", () => {
        if (socket.roomId) {
            io.to(socket.roomId).emit("system_msg", "تهانينا! لقد أصبحتم أصدقاء الآن ✨");
        }
    });

    // 5. إنهاء المحادثة (التخطي)
    socket.on("skip_chat", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "قام الشريك بإنهاء المحادثة.");
            
            // مغادرة الغرفة
            io.in(socket.roomId).socketsLeave(socket.roomId);
            socket.roomId = null;
            
            console.log(`انتهت المحادثة في الغرفة بناءً على طلب المستخدم.`);
        }
    });

    // 6. عند قطع الاتصال (إغلاق التطبيق)
    socket.on("disconnect", () => {
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "قطع الشريك الاتصال.");
        }
        console.log("غادر المستخدم: " + socket.id);
    });
});