const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let waitingUser = null; // نستخدم متغير واحد لربط شخصين

io.on('connection', (socket) => {
    console.log('مستخدم جديد اتصل:', socket.id);

    // هذا هو الاسم الذي يبحث عنه كود الفلاتر الخاص بك
    socket.on('join_random_chat', (data) => {
        console.log('طلب بحث عن لاعب من:', socket.id);

        if (waitingUser && waitingUser.id !== socket.id) {
            // وجدنا شريك!
            const partner = waitingUser;
            waitingUser = null;

            // إرسال 'chat_started' للطرفين (هذا ما ينتظره كود الفلاتر)
            io.to(socket.id).emit('chat_started', {
                partnerId: partner.id,
                partnerName: "مستخدم عشوائي"
            });
            io.to(partner.id).emit('chat_started', {
                partnerId: socket.id,
                partnerName: "مستخدم عشوائي"
            });

            console.log('✅ تم الربط بين جهازين!');
        } else {
            // لا يوجد أحد، نضعه في قائمة الانتظار
            waitingUser = socket;
            console.log('⏳ في انتظار لاعب آخر...');
        }
    });

    socket.on('disconnect', () => {
        if (waitingUser && waitingUser.id === socket.id) {
            waitingUser = null;
        }
        console.log('مستخدم قطع الاتصال');
    });
});

server.listen(process.env.PORT || 3000, () => {
    console.log('السيرفر يعمل الآن بنجاح...');
});
