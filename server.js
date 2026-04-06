const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let waitingUser = null;
let onlineUsers = {}; 

io.on("connection", (socket) => {
    
    socket.on("register_user", (username) => {
        if (username) {
            socket.username = username;
            onlineUsers[username] = socket.id;
            console.log(`المستخدم ${username} متصل بـ ID: ${socket.id}`);
        }
    });

    socket.on("find_partner", () => {
        if (socket.roomId) { socket.leave(socket.roomId); socket.roomId = null; }
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
        if (socket.roomId) socket.to(socket.roomId).emit("message", msg);
    });

    socket.on("send_friend_request", (myName) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_request_received", { name: myName });
        }
    });

    socket.on("accept_friend", (data) => {
        if (socket.roomId) {
            socket.to(socket.roomId).emit("friend_added_successfully", data.name);
        }
    });

    socket.on("private_message", (data) => {
        const targetId = onlineUsers[data.to];
        if (targetId) {
            io.to(targetId).emit("private_message_received", { from: data.from, text: data.text });
        }
    });

    socket.on("remove_friend_request", (data) => {
        const targetId = onlineUsers[data.friendName];
        if (targetId) {
            io.to(targetId).emit("friend_removed_by_other", { name: data.myName });
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
