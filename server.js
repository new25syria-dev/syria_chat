const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// 1. الاتصال بقاعدة البيانات (استبدل الرابط برابط قاعدة بياناتك)
mongoose.connect('mongodb://localhost:27017/chat_app')
    .then(() => console.log("✅ Connected to Database"))
    .catch(err => console.error("❌ DB Connection Error:", err));

// 2. تعريف شكل الرسالة (Schema) لضمان حفظ الوقت والطرفين
const MessageSchema = new mongoose.Schema({
    from: String,
    to: String,
    text: String,
    time: { type: Date, default: Date.now },
    isRead: { type: Boolean, default: false }
});
const Message = mongoose.model('Message', MessageSchema);

// تخزين المستخدمين المتصلين حالياً لربطهم بـ Sockets
const onlineUsers = {}; 

io.on('connection', (socket) => {
    console.log('👤 New User Connected:', socket.id);

    // تسجيل دخول المستخدم وربط اسمه بالـ Socket ID الخاص به
    socket.on('register_user', (userName) => {
        onlineUsers[userName] = socket.id;
        console.log(`📍 User ${userName} is now online`);
    });

    // --- [إرسال رسالة وحفظها] ---
    socket.on('private_message', async (data) => {
        try {
            // أ) حفظ الرسالة فوراً في قاعدة البيانات (لحل مشكلة الاختفاء)
            const savedMsg = await Message.create({
                from: data.from,
                to: data.to,
                text: data.text,
                time: data.time || new Date()
            });

            // ب) إرسال للطرف الآخر إذا كان متصلاً
            const recipientSocket = onlineUsers[data.to];
            if (recipientSocket) {
                io.to(recipientSocket).emit('private_message_received', savedMsg);
            }
            
            console.log(`📩 Message from ${data.from} to ${data.to} saved.`);
        } catch (err) {
            console.error("Error saving message:", err);
        }
    });

    // --- [جلب الأرشيف - History] ---
    socket.on('get_private_history', async (data) => {
        try {
            // جلب كل الرسائل بين الطرفين مرتبة زمنياً
            const history = await Message.find({
                $or: [
                    { from: data.from, to: data.to },
                    { from: data.to, to: data.from }
                ]
            }).sort({ time: 1 });

            // إرسال الأرشيف للشخص الذي طلبه فقط
            socket.emit('private_history', history);
        } catch (err) {
            console.error("Error fetching history:", err);
        }
    });

    // --- [تصفير عداد الرسائل] ---
    socket.on('mark_messages_read', async (data) => {
        try {
            await Message.updateMany(
                { from: data.friend, to: data.user, isRead: false },
                { $set: { isRead: true } }
            );
            console.log(`📖 Messages between ${data.user} and ${data.friend} marked as read.`);
        } catch (err) {
            console.error("Error updating read status:", err);
        }
    });

    socket.on('disconnect', () => {
        // تنظيف القائمة عند خروج المستخدم
        for (let user in onlineUsers) {
            if (onlineUsers[user] === socket.id) {
                delete onlineUsers[user];
                break;
            }
        }
        console.log('❌ User Disconnected');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
});
