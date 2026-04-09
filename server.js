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

const waitingUsers = [];
const activeChats = new Map(); // socket.id -> partnerSocketId

function removeFromWaiting(socketId) {
  const index = waitingUsers.findIndex((s) => s.id === socketId);
  if (index !== -1) {
    waitingUsers.splice(index, 1);
  }
}

function cleanupUser(socketId, notifyPartner = true) {
  const partnerId = activeChats.get(socketId);

  if (partnerId) {
    activeChats.delete(socketId);
    activeChats.delete(partnerId);

    if (notifyPartner) {
      io.to(partnerId).emit("partner_disconnected");
    }
  }

  removeFromWaiting(socketId);
}

io.on("connection", (socket) => {
  console.log("User connected:", socket.id);

  socket.onAny((event, ...args) => {
    console.log("Incoming event:", event, args);
  });

  socket.on("register_user", (userName) => {
    socket.userName = userName;
    console.log("Registered:", userName);
  });

  socket.on("find_partner", () => {
    console.log("Searching partner:", socket.id);

    if (activeChats.has(socket.id)) {
      console.log("Already matched:", socket.id);
      return;
    }

    removeFromWaiting(socket.id);

    while (waitingUsers.length > 0) {
      const partnerSocket = waitingUsers.shift();

      if (!partnerSocket) continue;
      if (partnerSocket.id === socket.id) continue;

      const stillConnected = io.sockets.sockets.get(partnerSocket.id);
      if (!stillConnected) continue;

      activeChats.set(socket.id, partnerSocket.id);
      activeChats.set(partnerSocket.id, socket.id);

      const socketName = socket.userName || socket.id;
      const partnerName = partnerSocket.userName || partnerSocket.id;

      socket.emit("chat_started", {
        partnerId: partnerName,
      });

      partnerSocket.emit("chat_started", {
        partnerId: socketName,
      });

      console.log(`Matched ${socket.id} with ${partnerSocket.id}`);
      return;
    }

    waitingUsers.push(socket);
    console.log("Waiting:", socket.id);
  });

  // رسائل عامة بين الشريكين
  socket.on("message", (data) => {
    console.log("message received from", socket.id, data);

    const partnerId = activeChats.get(socket.id);
    if (!partnerId) {
      console.log("No partner found for", socket.id);
      return;
    }

    // نرسل الحدثين معًا للتوافق مع Flutter
    io.to(partnerId).emit("message", data);
    io.to(partnerId).emit("private_message", data);

    console.log("message forwarded to", partnerId);
  });

  // رسائل خاصة بين الشريكين
  socket.on("private_message", (data) => {
    console.log("private_message received from", socket.id, data);

    const partnerId = activeChats.get(socket.id);
    if (!partnerId) {
      console.log("No partner found for", socket.id);
      return;
    }

    // نرسل الحدثين معًا للتوافق
    io.to(partnerId).emit("private_message", data);
    io.to(partnerId).emit("message", data);

    console.log("private_message forwarded to", partnerId);
  });

  socket.on("skip_partner", () => {
    console.log("skip_partner:", socket.id);

    const partnerId = activeChats.get(socket.id);

    if (partnerId) {
      activeChats.delete(socket.id);
      activeChats.delete(partnerId);

      io.to(partnerId).emit("partner_skipped");
      console.log("partner_skipped sent to", partnerId);
    }

    socket.emit("partner_skipped");
    removeFromWaiting(socket.id);
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
    cleanupUser(socket.id, true);
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
