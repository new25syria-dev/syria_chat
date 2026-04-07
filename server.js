const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const userSockets = {}; // للرسائل الخاصة (الاسم -> ID)
const waitingUsers = []; // قائمة بانتظار البحث العشوائي

io.on('connection', (socket) => {
    console.log('مستخدم متصل:', socket.id);

    // 1. تسجيل المستخدم فور الدخول
    socket.on('register_user', (userName) => {
        if (!userName) return;
        socket.userName = userName;
        userSockets[userName] = socket.id;
        console.log(`تم تسجيل: ${userName}`);
    });

    // 2. منطق البحث العشوائي
    socket.on('find_partner', () => {
        // إذا كان المستخدم موجوداً بالفعل في قائمة الانتظار، لا تكرره
        if (waitingUsers.includes(socket.id)) return;

        if (waitingUsers.length > 0) {
            const partnerId = waitingUsers.pop();
            const roomId = `room_${socket.id}_${partnerId}`;

            socket.join(roomId);
            io.sockets.sockets.get(partnerId)?.join(roomId);

            socket.partnerId = partnerId;
            const partnerSocket = io.sockets.sockets.get(partnerId);
            if (partnerSocket) partnerSocket.partnerId = socket.id;

            io.to(roomId).emit('system_msg', 'تم العثور على صديق! يمكنك الدردشة الآن.');
        } else {
            waitingUsers.push(socket.id);
            socket.emit('system_msg', 'جاري البحث عن صديق...');
        }
    });

    // 3. إرسال رسالة في الدردشة العشوائية
    socket.on('message', (msg) => {
        // البحث عن الغرف التي ينتمي إليها السوكت (باستثناء غرفته الخاصة)
        const rooms = Array.from(socket.rooms).filter(r => r !== socket.id);
        if (rooms.length > 0) {
            socket.to(rooms[0]).emit('message', msg);
        }
    });

    // 4. نظام طلبات الصداقة
    socket.on('send_friend_request', (myName) => {
        if (socket.partnerId) {
            io.to(socket.partnerId).emit('friend_request_received', { name: myName });
        }
    });

    socket.on('accept_friend', (data) => {
        if (socket.partnerId) {
            io.to(socket.partnerId).emit('friend_added_successfully', data.name);
        }
    });

    // 5. الرسائل الخاصة (بين الأصدقاء المضافين)
    socket.on('private_message', (data) => {
        const targetSocketId = userSockets[data.to];
        if (targetSocketId) {
            io.to(targetSocketId).emit('private_message_received', {
                from: data.from,
                text: data.text
            });
        }
    });

    socket.on('disconnect', () => {
        // إزالة المستخدم من قائمة الانتظار إذا خرج
        const index = waitingUsers.indexOf(socket.id);
        if (index > -1) waitingUsers.splice(index, 1);
        
        if (socket.userName) {
            delete userSockets[socket.userName];
        }
        console.log('مستخدم غادر');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
