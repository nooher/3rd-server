const { createServer } = require('http');
const { Server } = require('socket.io');

const httpServer = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('3rd signal server - by naim systems - free forever');
});

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
});

// ── In-memory stores ──────────────────────────────────────────────────────
const users = new Map();        // userId -> socketId
const sockets = new Map();      // socketId -> userId
const offlineQueue = new Map(); // userId -> [messages]
const statusStore = new Map();  // userId -> [statusItems]
const nearbyUsers = new Map();  // userId -> { lastSeen }

function getSocket(userId) { return users.get(userId); }

// ── Cleanup expired statuses every hour ───────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [userId, items] of statusStore.entries()) {
    const fresh = items.filter(i => now - i.timestamp < 24 * 60 * 60 * 1000);
    if (fresh.length === 0) statusStore.delete(userId);
    else statusStore.set(userId, fresh);
  }
}, 60 * 60 * 1000);

// ── Cleanup stale nearby every minute ────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [uid, data] of nearbyUsers.entries()) {
    if (now - data.lastSeen > 5 * 60 * 1000) nearbyUsers.delete(uid);
  }
}, 60 * 1000);

// ─────────────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  const userId = socket.handshake.query.userId;
  if (!userId) return;

  users.set(userId, socket.id);
  sockets.set(socket.id, userId);
  nearbyUsers.set(userId, { lastSeen: Date.now() });
  console.log(`[+] ${userId}`);

  // ── Deliver offline queue ──────────────────────────────────────────────
  if (offlineQueue.has(userId)) {
    const queued = offlineQueue.get(userId);
    queued.forEach(msg => socket.emit('message', msg));
    offlineQueue.delete(userId);
    console.log(`[~] delivered ${queued.length} queued to ${userId}`);
  }

  // ── Send stored statuses on connect ───────────────────────────────────
  const allStatuses = [];
  for (const [uid, items] of statusStore.entries()) {
    if (uid !== userId) {
      items.forEach(item => allStatuses.push({ item, from: uid }));
    }
  }
  if (allStatuses.length > 0) socket.emit('status-bulk', allStatuses);
  if (statusStore.has(userId)) socket.emit('status-mine', statusStore.get(userId));

  // ── MESSAGING ─────────────────────────────────────────────────────────
  socket.on('message', (msg) => {
    if (!msg || !msg.to || !msg.id) return;
    const t = getSocket(msg.to);
    if (t) {
      io.to(t).emit('message', msg);
      socket.emit('message-sent', { messageId: msg.id });
      console.log(`[>] ${userId} -> ${msg.to}`);
    } else {
      if (!offlineQueue.has(msg.to)) offlineQueue.set(msg.to, []);
      offlineQueue.get(msg.to).push(msg);
      socket.emit('message-queued', { messageId: msg.id });
      console.log(`[Q] queued for ${msg.to}`);
    }
  });

  socket.on('message-delivered', ({ messageId, to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('message-delivered', { messageId });
  });

  socket.on('mark-read', ({ conversationId }) => {
    const t = getSocket(conversationId);
    if (t) io.to(t).emit('message-read', { conversationId: userId });
  });

  socket.on('typing', ({ to, isTyping }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('typing', { from: userId, isTyping });
  });

  // ── GROUP MESSAGING ───────────────────────────────────────────────────
  socket.on('group-message', ({ groupId, message, memberIds }) => {
    (memberIds || []).forEach(memberId => {
      if (memberId === userId) return;
      const t = getSocket(memberId);
      if (t) {
        io.to(t).emit('group-message', { groupId, message });
      } else {
        if (!offlineQueue.has(memberId)) offlineQueue.set(memberId, []);
        offlineQueue.get(memberId).push({ ...message, groupId, isGroup: true });
      }
    });
  });

  socket.on('group-create', (group) => {
    (group.members || []).forEach(memberId => {
      if (memberId === userId) return;
      const t = getSocket(memberId);
      if (t) io.to(t).emit('group-invite', group);
    });
  });

  // ── WEBRTC SIGNALING ──────────────────────────────────────────────────
  socket.on('offer', ({ to, offer, type }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('offer', { from: userId, offer, type });
    else socket.emit('call-failed', { reason: 'user-offline' });
  });

  socket.on('answer', ({ to, answer }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('answer', { from: userId, answer });
  });

  socket.on('ice-candidate', ({ to, candidate }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('ice-candidate', { from: userId, candidate });
  });

  socket.on('end-call', ({ to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('call-ended', { from: userId });
  });

  socket.on('call-rejected', ({ to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('call-rejected', { from: userId });
  });

  // ── P2P FILE TRANSFER SIGNALING ───────────────────────────────────────
  // Sender requests to send file — receiver gets confirmation prompt
  socket.on('p2p-request', ({ transferId, to, fileName, fileSize, fileType }) => {
    const t = getSocket(to);
    if (t) {
      io.to(t).emit('p2p-request', { transferId, from: userId, fileName, fileSize, fileType });
      console.log(`[P2P] ${userId} -> ${to}: ${fileName}`);
    } else {
      // Receiver offline — notify sender
      socket.emit('p2p-offline', { transferId, to });
    }
  });

  // Receiver accepted — sender can start WebRTC
  socket.on('p2p-accepted', ({ transferId, to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('p2p-accepted', { transferId, from: userId });
  });

  // Receiver declined
  socket.on('p2p-declined', ({ transferId, to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('p2p-declined', { transferId, from: userId });
  });

  // WebRTC handshake for P2P — server just routes, never sees content
  socket.on('p2p-offer', ({ transferId, to, offer }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('p2p-offer', { transferId, from: userId, offer });
  });

  socket.on('p2p-answer', ({ transferId, to, answer }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('p2p-answer', { transferId, from: userId, answer });
  });

  socket.on('p2p-ice', ({ transferId, to, candidate }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('p2p-ice', { transferId, from: userId, candidate });
  });

  // ── STATUS ────────────────────────────────────────────────────────────
  socket.on('status-post', ({ item }) => {
    if (!item) return;
    if (!statusStore.has(userId)) statusStore.set(userId, []);
    const items = statusStore.get(userId);
    const idx = items.findIndex(s => s.id === item.id);
    if (idx >= 0) items[idx] = item;
    else items.push(item);
    socket.broadcast.emit('status-new', { item, from: userId });
    console.log(`[S] ${userId} posted status`);
  });

  socket.on('status-delete', ({ itemId }) => {
    if (statusStore.has(userId)) {
      statusStore.set(userId, statusStore.get(userId).filter(s => s.id !== itemId));
    }
    socket.broadcast.emit('status-deleted', { itemId, from: userId });
  });

  socket.on('status-seen', ({ statusUserId }) => {
    const t = getSocket(statusUserId);
    if (t) io.to(t).emit('status-seen', { viewerId: userId });
  });

  // ── NEARBY DISCOVERY ──────────────────────────────────────────────────
  socket.on('nearby-ping', () => {
    nearbyUsers.set(userId, { lastSeen: Date.now() });
    const now = Date.now();
    const nearby = [];
    for (const [uid, data] of nearbyUsers.entries()) {
      if (uid === userId) continue;
      if (now - data.lastSeen > 5 * 60 * 1000) continue;
      if (!users.has(uid)) continue;
      nearby.push({ userId: uid });
    }
    socket.emit('nearby-users', { users: nearby });
  });

  socket.on('nearby-request', ({ to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('nearby-request', { from: userId });
  });

  socket.on('nearby-accept', ({ to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('nearby-accepted', { from: userId });
  });

  socket.on('nearby-decline', ({ to }) => {
    const t = getSocket(to);
    if (t) io.to(t).emit('nearby-declined', { from: userId });
  });

  // ── DISCONNECT ────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const uid = sockets.get(socket.id);
    if (uid) {
      users.delete(uid);
      sockets.delete(socket.id);
      nearbyUsers.delete(uid);
      console.log(`[-] ${uid}`);
    }
  });
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`
  ┌──────────────────────────────────────┐
  │   3rd signal server                  │
  │   by naim systems                    │
  │   port: ${PORT}                         │
  │   free · open · belongs to everyone  │
  └──────────────────────────────────────┘
  `);
});
