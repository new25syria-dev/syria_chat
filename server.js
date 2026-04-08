const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let waitingUser = null; 

io.on('connection', (socket) => {
    console.log('✅ مستخدم جديد متصل الآن:', socket.id);

    // حدث البحث (يدعم الاسمين الأكثر شيوعاً لضمان العمل)
    const handleSearch = (data) => {
        console.log('🔎 طلب بحث مستلم من:', socket.id);

        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            // إرسال رد للجهازين لبدء الدردشة
            io.to(socket.id).emit('chat_started', { partnerId: partner.id });
            io.to(partner.id).emit('chat_started', { partnerId: socket.id });

            console.log('🎊 تم الربط بنجاح بين جهازين!');
        } else {
            waitingUser = socket;
            console.log('⏳ واحد في قائمة الانتظار...');
        }
    };

    // الاستماع لكافة أسماء الأحداث المحتملة من الفلاتر
    socket.on('join_random_chat', handleSearch);
    socket.on('search_partner', handleSearch);

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
        console.log('❌ مستخدم قطع الاتصال');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 السيرفر يعمل على المنفذ ${PORT}`);
});
