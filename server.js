const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// تخزين المستخدمين المتصلين: { "اسم_المستخدم": "socket_id" }
const userSockets = {};
// تخزين حالة الاتصال ووقت الخروج: { "اسم_المستخدم": { online: true, lastSeen: "..." } }
const userStatus = {};

// للدردشة العشوائية (Random Chat)
let waitingUsers = [];

io.on('connection', (socket) => {
    console.log('مستخدم جديد اتصل: ' + socket.id);

    // 1. تسجيل المستخدم وربط اسمه بالـ ID الخاص به
    socket.on('register_user', (userName) => {
        if (!userName) return;
        socket.userName = userName;
        userSockets[userName] = socket.id;
        
        // تحديث الحالة إلى "متصل"
        userStatus[userName] = {
            online: true,
            lastSeen: "متصل الآن"
        };

        // إبلاغ الجميع أن هذا المستخدم أصبح متصلاً
        io.emit('update_status', { user: userName, online: true, lastSeen: "متصل الآن" });
        console.log(`تم تسجيل: ${userName}`);
    });

    // 2. البحث عن شريك (الدردشة العشوائية)
    socket.on('find_partner', () => {
        if (waitingUsers.length > 0) {
            let partner = waitingUsers.shift();
            socket.partner = partner;
            partner.partner = socket;

            socket.emit('system_msg', "تم العثور على لاعب! يمكنك الدردشة الآن.");
            partner.emit('system_msg', "تم العثور على لاعب! يمكنك الدردشة الآن.");
        } else {
            waitingUsers.push(socket);
            socket.emit('system_msg', "جاري البحث عن لاعب متاح...");
        }
    });

    // 3. إرسال رسالة في الدردشة العشوائية
    socket.on('message', (msg) => {
        if (socket.partner) {
            socket.partner.emit('message', msg);
        }
    });

    // 4. طلبات الصداقة
    socket.on('send_friend_request', (myName) => {
        if (socket.partner && socket.partner.userName) {
            socket.partner.emit('friend_request_received', { name: myName });
        }
    });

    socket.on('accept_friend', (data) => {
        if (socket.partner) {
            socket.partner.emit('friend_added_successfully', data.name);
        }
    });

    // 5. الرسائل الخاصة (بين الأصدقاء) - (Critical Fix)
    socket.on('private_message', (data) => {
        const targetSocketId = userSockets[data.to];
        if (targetSocketId) {
            io.to(targetSocketId).emit('private_message_received', {
                from: data.from,
                text: data.text
            });
            console.log(`رسالة من ${data.from} إلى ${data.to}`);
        }
    });

    // 6. طلب حالة الأصدقاء (Online/Offline)
    socket.on('get_friends_status', (friendsList) => {
        if (!friendsList) return;
        friendsList.forEach(friendName => {
            const status = userStatus[friendName] || { online: false, lastSeen: "غير معروف" };
            socket.emit('update_status', { 
                user: friendName, 
                online: status.online, 
                lastSeen: status.lastSeen 
            });
        });
    });

    // 7. الحذف المتبادل (Mutual Delete)
    socket.on('delete_friend_request', (data) => {
        const friendId = userSockets[data.friend];
        if (friendId) {
            io.to(friendId).emit('friend_deleted_me', { from: data.me });
            console.log(`${data.me} حذف ${data.friend}`);
        }
    });

    // 8. التعامل مع قطع الاتصال
    socket.on('disconnect', () => {
        if (socket.userName) {
            const now = new Date();
            const timeStr = `${now.getHours()}:${now.getMinutes()}`;
            
            userStatus[socket.userName] = {
                online: false,
                lastSeen: timeStr
            };

            // إبلاغ الجميع أن المستخدم خرج
            io.emit('update_status', { 
                user: socket.userName, 
                online: false, 
                lastSeen: timeStr 
            });

            delete userSockets[socket.userName];
        }

        // إخراج المستخدم من قائمة الانتظار إذا كان فيها
        waitingUsers = waitingUsers.filter(u => u.id !== socket.id);

        // إبلاغ شريك الدردشة العشوائية بانقطاع الاتصال
        if (socket.partner) {
            socket.partner.emit('system_msg', "انقطع الاتصال بالشريك.");
            socket.partner.partner = null;
        }
        console.log('مستخدم غادر');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`السيرفر يعمل على المنفذ: ${PORT}`);
});
