const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let waitingUser = null;
let onlineUsers = {}; // { "اسم_المستخدم": "socket_id" }

io.on("connection", (socket) => {
    
    socket.on("register_user", (username) => {
        if (!username) return;
        socket.username = username;
        onlineUsers[username] = socket.id; // تحديث الـ ID دائماً عند كل دخول
        console.log(`المستخدم ${username} مسجل الآن بـ ID: ${socket.id}`);
    });

    socket.on("find_partner", () => {
        // تنظيف أي غرفة قديمة كان فيها المستخدم
        if (socket.roomId) {
            socket.leave(socket.roomId);
            socket.roomId = null;
        }

        if (waitingUser && waitingUser.id !== socket.id && waitingUser.connected) {
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
        if (socket.roomId) {
            socket.to(socket.roomId).emit("message", msg);
        }
    });

    socket.on("private_message", (data) => {
        // التأكد من أن المستلم مسجل حالياً في الـ onlineUsers
        const targetSocketId = onlineUsers[data.to];
        if (targetSocketId) {
            io.to(targetSocketId).emit("private_message_received", {
                from: data.from,
                text: data.text
            });
        }
    });

    socket.on("skip_chat", () => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("system_msg", "غادر الشريك المحادثة.");
            socket.leave(socket.roomId);
            socket.roomId = null;
        }
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
    });

    socket.on("disconnect", () => {
        if (socket.username) delete onlineUsers[socket.username];
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
    });
});
