const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);

// إعدادات السوكت مع تفعيل كل الخيارات لضمان وصول الإشارة
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    },
    transports: ['websocket', 'polling'] 
});

let waitingUser = null; 

io.on('connection', (socket) => {
    // هذه الجملة تظهر عندك في الصورة (✅ مستخدم جديد متصل الآن)
    console.log('✅ مستخدم جديد متصل الآن:', socket.id);

    // دالة المعالجة الموحدة
    const handleSearch = (data) => {
        console.log('🔎 [إشارة وصلت] مستخدم يطلب البحث الآن:', socket.id);

        if (waitingUser && waitingUser.id !== socket.id) {
            const partner = waitingUser;
            waitingUser = null;

            io.to(socket.id).emit('chat_started', { partnerId: partner.id });
            io.to(partner.id).emit('chat_started', { partnerId: socket.id });

            console.log('🎊 تم الربط بنجاح بين جهازيين!');
        } else {
            waitingUser = socket;
            console.log('⏳ واحد في قائمة الانتظار، ننتظر الثاني...');
        }
    };

    // سنستمع لكل الاحتمالات التي قد يرسلها الفلاتر
    socket.on('join_random_chat', handleSearch);
    socket.on('search_partner', handleSearch);
    socket.on('message', handleSearch); // كاحتياط

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
        console.log('❌ مستخدم قطع الاتصال');
    });
});

// ملاحظة: Render يستخدم المنفذ 10000 تلقائياً وهذا سليم
const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 السيرفر يعمل الآن على المنفذ ${PORT}`);
});
