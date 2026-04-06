const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let waitingUser = null;
let onlineUsers = {}; // لربط اسم المستخدم بـ ID السوكيت

io.on("connection", (socket) => {
    // تسجيل المستخدم عند دخول التطبيق
    socket.on("register_user", (username) => {
        socket.username = username;
        onlineUsers[username] = socket.id;
        console.log(`${username} متصل الآن`);
    });

    // الدردشة العشوائية
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
            socket.emit("system_msg", "جاري البحث عن شريك...");
        }
    });

    socket.on("message", (msg) => {
        if (socket.roomId) socket.to(socket.roomId).emit("message", msg);
    });

    // طلب الصداقة وتبادل الأسماء
    socket.on("send_friend_request", (myUserName) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_request_received", { name: myUserName });
        }
    });

    socket.on("accept_friend", (myData) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_added_successfully", myData.name);
        }
    });

    // إرسال رسالة خاصة لصديق محدد
    socket.on("private_message", (data) => {
        const targetId = onlineUsers[data.to];
        if (targetId) {
            io.to(targetId).emit("private_message_received", {
                from: socket.username,
                text: data.text
            });
        }
    });

    socket.on("skip_chat", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "انتهت المحادثة.");
            io.in(socket.roomId).socketsLeave(socket.roomId);
            socket.roomId = null;
        }
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
    });

    socket.on("disconnect", () => {
        if (socket.username) delete onlineUsers[socket.username];
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
    });
});
