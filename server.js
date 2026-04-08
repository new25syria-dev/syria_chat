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

    // سطر المراقبة الشامل: يطبع أي شيء يرسله الموبايل مهما كان اسمه
    socket.onAny((event, data) => {
        console.log(`📩 استلمت حدثاً باسم [${event}] ببيانات:`, data);
    });

    // معالج البحث عن لاعب
    const handleSearch = (data) => {
        console.log('🔎 طلب بحث مستلم من:', socket.id);

        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            // إرسال لغة البداية للطرفين
            io.to(socket.id).emit('chat_started', { partnerId: partner.id });
            io.to(partner.id).emit('chat_started', { partnerId: socket.id });

            console.log('🎊 تم التطابق والربط بنجاح!');
        } else {
            waitingUser = socket;
            console.log('⏳ في انتظار لاعب آخر...');
        }
    };

    // الاستماع لكل المسميات الممكنة
    socket.on('join_random_chat', handleSearch);
    socket.on('search_partner', handleSearch);

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) waitingUser = null;
        console.log('❌ مستخدم قطع الاتصال');
    });
});

// المنفذ المتوافق مع Render و المحلي
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 السيرفر يعمل على منفذ: ${PORT}`);
});
