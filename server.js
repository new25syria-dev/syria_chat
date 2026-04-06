const port = process.env.PORT || 3000;
const io = require("socket.io")(port, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

console.log(`سيرفر سوريا شات يعمل على المنفذ: ${port}`);

let waitingUser = null;

io.on("connection", (socket) => {
    console.log("اتصال جديد: " + socket.id);

    // 1. البحث عن شريك (مع منع التكرار)
    socket.on("find_partner", () => {
        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            const roomId = `room_${partner.id}_${socket.id}`;
            
            socket.join(roomId);
            partner.join(roomId);

            socket.roomId = roomId;
            partner.roomId = roomId;

            // إرسال إشعار للطرفين ببدء المحادثة
            io.to(roomId).emit("system_msg", "تم العثور على شريك! أهلاً بك في سوريا شات 🇸🇾");
        } else {
            waitingUser = socket;
            socket.emit("system_msg", "جاري البحث عن شريك متاح... يرجى الانتظار.");
        }
    });

    // 2. إرسال واستقبال الرسائل
    socket.on("message", (msg) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("message", msg);
        }
    });

    // 3. طلب الصداقة
    socket.on("send_friend_request", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_request_received");
        }
    });

    // 4. قبول الصداقة (تعديل حيوي للمزامنة بين الطرفين)
    socket.on("accept_friend", () => {
        if (socket.roomId) {
            // نرسل للطرفين معاً لضمان الحفظ في كلا الهاتفين
            io.to(socket.roomId).emit("friend_added_successfully", "شريك جديد");
            io.to(socket.roomId).emit("system_msg", "تمت إضافة الصديق بنجاح لدى الطرفين! ✅");
        }
    });

    // 5. التخطي (إصلاح مشكلة البحث الجديد)
    socket.on("skip_chat", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "قام الشريك بإنهاء المحادثة.");
            
            const oldRoom = socket.roomId;
            // إخراج المستخدمين من الغرفة القديمة
            io.in(oldRoom).socketsLeave(oldRoom);
            
            socket.roomId = null;
        }
        // تنظيف قائمة الانتظار إذا كان المستخدم هو من ينتظر
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
    });

    // 6. عند المغادرة أو قطع الاتصال
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
