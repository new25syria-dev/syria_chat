const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*" },
    transports: ['websocket'] // إجبار السيرفر على استخدام أسرع وسيلة اتصال
});

let waitingUser = null;

io.on('connection', (socket) => {
    console.log('✅ مستخدم متصل:', socket.id);

    socket.on('join_random_chat', (data) => {
        console.log('🔎 طلب بحث من:', socket.id);
        
        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            io.to(socket.id).emit('chat_started', { partnerId: partner.id });
            io.to(partner.id).emit('chat_started', { partnerId: socket.id });
            console.log('🎊 تم الربط بنجاح!');
        } else {
            waitingUser = socket;
            console.log('⏳ في انتظار شريك...');
        }
    });

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
        console.log('❌ قطع الاتصال');
    });
});

// السطر السحري: يعمل على 3000 محلياً وعلى منفذ Render تلقائياً
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on port ${PORT}`);
});
