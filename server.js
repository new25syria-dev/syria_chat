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
  },
});

mongoose.connect(process.env.DATABASE_URL)
  .then(() => console.log("MongoDB connected"))
  .catch(err => console.log(err));

app.get("/", (req, res) => {
  res.send("Server is running");
});

const waitingUsers = [];
const activeChats = new Map();

io.on("connection", (socket) => {
  console.log("User connected:", socket.id);

  socket.on("register_user", (userName) => {
    socket.userName = userName;
    console.log("Registered:", userName);
  });

  socket.on("find_partner", () => {
    console.log("Searching partner:", socket.id);

    if (waitingUsers.length > 0) {
      const partner = waitingUsers.shift();

      activeChats.set(socket.id, partner.id);
      activeChats.set(partner.id, socket.id);

      // 🔥 هذا الحدث مهم لأنه Flutter ينتظره
      socket.emit("chat_started", {
        partnerId: partner.userName || partner.id,
      });

      partner.emit("chat_started", {
        partnerId: socket.userName || socket.id,
      });

      console.log(`Matched ${socket.id} with ${partner.id}`);
    } else {
      waitingUsers.push(socket);
      console.log("Waiting:", socket.id);
    }
  });

  socket.on("message", (data) => {
    const partnerId = activeChats.get(socket.id);
    if (partnerId) {
      io.to(partnerId).emit("message", data);
    }
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);

    const index = waitingUsers.findIndex(s => s.id === socket.id);
    if (index !== -1) {
      waitingUsers.splice(index, 1);
    }

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
