const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let waitingUser = null;
let onlineUsers = {}; 

io.on("connection", (socket) => {
    // تسجيل المستخدم لربط الاسم بالـ ID
    socket.on("register_user", (username) => {
        if (username) {
            socket.username = username;
            onlineUsers[username] = socket.id;
            console.log(`${username} متصل`);
        }
    });

    // البحث عن شريك (تم إصلاح التكرار)
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

    // إرسال رسالة عامة في الدردشة العشوائية
    socket.on("message", (msg) => {
        if (socket.roomId) socket.to(socket.roomId).emit("message", msg);
    });

    // إرسال رسالة خاصة لصديق
    socket.on("private_message", (data) => {
        const targetId = onlineUsers[data.to];
        if (targetId) {
            io.to(targetId).emit("private_message_received", {
                from: data.from,
                text: data.text
            });
        }
    });

    // حذف الصداقة من الطرف الآخر
    socket.on("remove_friend_request", (data) => {
        const targetId = onlineUsers[data.friendName];
        if (targetId) {
            io.to(targetId).emit("friend_removed_by_other", { name: data.myName });
        }
    });

    socket.on("send_friend_request", (name) => {
        if (socket.roomId) socket.to(socket.roomId).emit("friend_request_received", { name: name });
    });

    socket.on("accept_friend", (data) => {
        if (socket.roomId) socket.to(socket.roomId).emit("friend_added_successfully", data.name);
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
