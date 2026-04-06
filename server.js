const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let waitingUser = null;

io.on("connection", (socket) => {
    // البحث عن شريك
    socket.on("find_partner", () => {
        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;
            const roomId = `room_${partner.id}_${socket.id}`;
            socket.join(roomId);
            partner.join(roomId);
            socket.roomId = roomId;
            partner.roomId = roomId;
            io.to(roomId).emit("system_msg", "تم العثور على شريك! 🇸🇾");
        } else {
            waitingUser = socket;
            socket.emit("system_msg", "جاري البحث عن شريك متاح...");
        }
    });

    // إرسال رسالة
    socket.on("message", (msg) => {
        if (socket.roomId) socket.to(socket.roomId).emit("message", msg);
    });

    // طلب صداقة مع إرسال "اسم المرسل"
    socket.on("send_friend_request", (myUserName) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_request_received", { name: myUserName });
        }
    });

    // قبول الصداقة مع تبادل الأسماء للطرفين
    socket.on("accept_friend", (myData) => {
        if (socket.roomId) {
            // نرسل لكل طرف اسم الطرف الآخر
            socket.to(socket.roomId).emit("friend_added_successfully", myData.name);
            socket.emit("system_msg", "تمت إضافة الصديق بنجاح! ✅");
        }
    });

    // التخطي السريع
    socket.on("skip_chat", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "انتهت المحادثة.");
            io.in(socket.roomId).socketsLeave(socket.roomId);
            socket.roomId = null;
        }
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
    });

    socket.on("disconnect", () => {
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
        if (socket.roomId) socket.to(socket.roomId).emit("system_msg", "غادر الشريك.");
    });
});
