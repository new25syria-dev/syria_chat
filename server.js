const port = process.env.PORT || 3000;
const io = require("socket.io")(port, { cors: { origin: "*" } });

let users = {}; // لتخزين المستخدمين المتصلين ومعرفاتهم

io.on("connection", (socket) => {
    
    // تسجيل المستخدم عند الاتصال
    socket.on("register_user", (userName) => {
        socket.userName = userName;
        users[userName] = socket.id; 
        console.log(`المستخدم ${userName} متصل الآن`);
    });

    socket.on("find_partner", () => {
        // ... نفس منطق البحث السابق ...
    });

    // إرسال رسالة خاصة لصديق
    socket.on("private_message", (data) => {
        const targetSocketId = users[data.to];
        if (targetSocketId) {
            io.to(targetSocketId).emit("private_message_received", {
                from: socket.userName,
                text: data.text
            });
        }
    });

    socket.on("skip_chat", () => { /* نفس المنطق السابق */ });

    socket.on("disconnect", () => {
        if (socket.userName) delete users[socket.userName];
    });
});
