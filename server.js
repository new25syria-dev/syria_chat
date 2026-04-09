const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const mongoose = require("mongoose");
require("dotenv").config();

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

mongoose
  .connect(process.env.DATABASE_URL)
  .then(() => console.log("MongoDB connected"))
  .catch((err) => console.log("MongoDB error:", err));

app.get("/", (req, res) => {
  res.send("Server is running");
});

// المستخدمون المنتظرون
const waitingUsers = [];

// ربط كل مستخدم بشريكه
const activeChats = new Map(); // socket.id => partnerSocketId

function removeFromWaiting(socketId) {
  const index = waitingUsers.indexOf(socketId);
  if (index !== -1) {
    waitingUsers.splice(index, 1);
  }
}

function endChat(socket, notifyPartner = true) {
  const partnerId = activeChats.get(socket.id);

  if (partnerId) {
    activeChats.delete(socket.id);
    activeChats.delete(partnerId);

    if (notifyPartner) {
      io.to(partnerId).emit("partner_disconnected");
    }
  }

  removeFromWaiting(socket.id);
}

io.on("connection", (socket) => {
  console.log("User connected:", socket.id);

  // المستخدم يطلب شريك
  socket.on("find_partner", () => {
    console.log("find_partner:", socket.id);

    // إذا كان عنده شريك مسبقًا لا نفعل شيئًا
    if (activeChats.has(socket.id)) {
      return;
    }

    // إذا كان موجودًا في قائمة الانتظار لا نكرره
    if (waitingUsers.includes(socket.id)) {
      return;
    }

    // البحث عن أول مستخدم منتظر صالح
    while (waitingUsers.length > 0) {
      const partnerId = waitingUsers.shift();

      if (!partnerId || partnerId === socket.id) {
        continue;
      }

      const partnerSocket = io.sockets.sockets.get(partnerId);

      if (!partnerSocket) {
        continue;
      }

      // تم العثور على شريك
      activeChats.set(socket.id, partnerId);
      activeChats.set(partnerId, socket.id);

      socket.emit("partner_found", {
        partnerId: partnerId,
      });

      partnerSocket.emit("partner_found", {
        partnerId: socket.id,
      });

      console.log(`Matched: ${socket.id} <-> ${partnerId}`);
      return;
    }

    // لا يوجد شريك حاليًا، ضعه في الانتظار
    waitingUsers.push(socket.id);
    socket.emit("waiting_for_partner");
    console.log("User waiting:", socket.id);
  });

  // إرسال رسالة للشريك فقط
  socket.on("message", (data) => {
    const partnerId = activeChats.get(socket.id);

    if (!partnerId) {
      return;
    }

    io.to(partnerId).emit("message", data);
  });

  // تخطي الشريك الحالي والعودة للبحث
  socket.on("skip_partner", () => {
    const partnerId = activeChats.get(socket.id);

    if (partnerId) {
      activeChats.delete(socket.id);
      activeChats.delete(partnerId);

      io.to(partnerId).emit("partner_skipped");
    }

    socket.emit("partner_skipped");
    console.log("skip_partner:", socket.id);
  });

  // إذا انفصل المستخدم
  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);

    removeFromWaiting(socket.id);

    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      activeChats.delete(socket.id);
      activeChats.delete(partnerId);
      io.to(partnerId).emit("partner_disconnected");
    }
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
