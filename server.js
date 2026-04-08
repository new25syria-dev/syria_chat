const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);

const io = new Server(server, {
    cors: { origin: "*" },
    transports: ['websocket', 'polling']
});

let waitingUser = null;

io.on('connection', (socket) => {
    console.log('✅ مستخدم متصل:', socket.id);

    // دالة الربط المشتركة
    const handleJoin = (data) => {
        console.log(`🔎 محاولة مطابقة للمستخدم: ${socket.id}`);

        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            // إرسال حدث البداية للجهازين
            io.to(socket.id).emit('chat_started', { partnerId: partner.id });
            io.to(partner.id).emit('chat_started', { partnerId: socket.id });

            console.log('🎊 تم التطابق بنجاح بين جهازيين!');
        } else {
            waitingUser = socket;
            console.log('⏳ مستخدم واحد في الانتظار...');
        }
    };

    // الاستماع للحدث الذي يرسله تطبيقك فعلياً (من واقع الصورة)
    socket.on('register_user', handleJoin); 
    
    // احتياطاً سنبقي على الأسماء الأخرى
    socket.on('join_random_chat', handleJoin);

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
        console.log('❌ مستخدم قطع الاتصال');
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 السيرفر يعمل على المنفذ ${PORT}`);
});
