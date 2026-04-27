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
const statusStore = new Map();  // userId -> [statusItems]  ← NEW: persists status
const nearbyUsers = new Map();  // userId -> { lastSeen, socketId } ← NEW: proximity

function getSocket(userId) { return users.get(userId); }

// ── Status cleanup: remove expired (24h) statuses ────────────────────────
function cleanExpiredStatuses() {
  const now = Date.now();
  const TTL = 24 * 60 * 60 * 1000; // 24 hours
  for (const [userId, items] of statusStore.entries()) {
    const fresh = items.filter(item => now - item.timestamp < TTL);
    if (fresh.length === 0) {
      statusStore.delete(userId);
    } else {
      statusStore.set(userId, fresh);
    }
  }
}
// Run cleanup every hour
setInterval(cleanExpiredStatuses, 60 * 60 * 1000);

// ── Nearby cleanup: remove stale entries (5 min) ─────────────────────────
function cleanNearby() {
  const now = Date.now();
  for (const [userId, data] of nearbyUsers.entries()) {
    if (now - data.lastSeen > 5 * 60 * 1000) {
      nearbyUsers.delete(userId);
    }
  }
}
setInterval(cleanNearby, 60 * 1000);

// ─────────────────────────────────────────────────────────────────────────
io.on('connection', (socket) => {
  const userId = socket.handshake.query.userId;
  if (!userId) return;

  // Register user
  users.set(userId, socket.id);
  sockets.set(socket.id, userId);
  nearbyUsers.set(userId, { lastSeen: Date.now(), socketId: socket.id });
  console.log(`[+] ${userId} (${socket.id})`);

  // ── Deliver offline queue ───────────────────────────────────────────────
  if (offlineQueue.has(userId)) {
    const queued = offlineQueue.get(userId);
    queued.forEach(msg => socket.emit('message', msg));
    offlineQueue.delete(userId);
    console.log(`[~] delivered ${queued.length} queued msgs to ${userId}`);
  }

  // ── Send stored statuses to newly connected user ────────────────────────
  // They will see all statuses that were posted while they were offline
  const allStatuses = [];
  for (const [statusUserId, items] of statusStore.entries()) {
    if (statusUserId !== userId) {
      items.forEach(item => {
        allStatuses.push({ item, from: statusUserId });
      });
    }
  }
  if (allStatuses.length > 0) {
    socket.emit('status-bulk', allStatuses);
  }

  // ── Also send own stored statuses back ─────────────────────────────────
  if (statusStore.has(userId)) {
    socket.emit('status-mine', statusStore.get(userId));
  }

  // ── MESSAGING ────────────────────────────────────────────────────────────
  socket.on('message', (msg) => {
    if (!msg || !msg.to || !msg.id) return;

    const targetSocket = getSocket(msg.to);
    if (targetSocket) {
      io.to(targetSocket).emit('message', msg);
      socket.emit('message-sent', { messageId: msg.id });
      console.log(`[>] ${userId} -> ${msg.to}: ${msg.text?.slice(0, 30) || '[media]'}`);
    } else {
      // Queue for offline user
      if (!offlineQueue.has(msg.to)) offlineQueue.set(msg.to, []);
      offlineQueue.get(msg.to).push(msg);
      socket.emit('message-queued', { messageId: msg.id });
      console.log(`[Q] queued for offline ${msg.to}`);
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

  // ── GROUP MESSAGING ───────────────────────────────────────────────────────
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

  // ── WEBRTC SIGNALING ──────────────────────────────────────────────────────
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

  // ── STATUS — now with persistence ─────────────────────────────────────────
  socket.on('status-post', ({ item, friendIds }) => {
    // Store status so offline users see it when they come online
    if (!statusStore.has(userId)) statusStore.set(userId, []);
    const userStatuses = statusStore.get(userId);

    // Replace if same id, otherwise add
    const existingIdx = userStatuses.findIndex(s => s.id === item.id);
    if (existingIdx >= 0) {
      userStatuses[existingIdx] = item;
    } else {
      userStatuses.push(item);
    }

    // Broadcast to all online users (not just friendIds)
    // Since 3rd has no "friends" concept — everyone can see statuses
    socket.broadcast.emit('status-new', { item, from: userId });
    console.log(`[S] ${userId} posted status`);
  });

  socket.on('status-delete', ({ itemId }) => {
    if (statusStore.has(userId)) {
      const filtered = statusStore.get(userId).filter(s => s.id !== itemId);
      statusStore.set(userId, filtered);
    }
    socket.broadcast.emit('status-deleted', { itemId, from: userId });
  });

  socket.on('status-seen', ({ statusUserId }) => {
    const t = getSocket(statusUserId);
    if (t) io.to(t).emit('status-seen', { viewerId: userId });
  });

  // ── NEARBY DISCOVERY — NEW ────────────────────────────────────────────────
  // User announces they are looking for nearby users
  socket.on('nearby-ping', ({ userId: pingUserId }) => {
    // Update their last seen
    nearbyUsers.set(userId, { lastSeen: Date.now(), socketId: socket.id });

    // Get all online users (connected in last 5 minutes)
    const now = Date.now();
    const nearbyList = [];
    for (const [nearId, data] of nearbyUsers.entries()) {
      if (nearId === userId) continue;
      if (now - data.lastSeen > 5 * 60 * 1000) continue;
      if (!users.has(nearId)) continue; // must be online
      nearbyList.push({ userId: nearId });
    }

    // Send back list of nearby users
    socket.emit('nearby-users', { users: nearbyList });
    console.log(`[P] ${userId} pinged nearby, found ${nearbyList.length} users`);
  });

  // User requests to connect with a nearby user
  socket.on('nearby-request', ({ to, from: fromId }) => {
    const t = getSocket(to);
    if (t) {
      io.to(t).emit('nearby-request', { from: userId });
      console.log(`[N] ${userId} -> ${to} nearby request`);
    }
  });

  // User accepts nearby request
  socket.on('nearby-accept', ({ to }) => {
    const t = getSocket(to);
    if (t) {
      io.to(t).emit('nearby-accepted', { from: userId });
    }
  });

  // User declines nearby request
  socket.on('nearby-decline', ({ to }) => {
    const t = getSocket(to);
    if (t) {
      io.to(t).emit('nearby-declined', { from: userId });
    }
  });

  // ── PRESENCE / DISCONNECT ─────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const uid = sockets.get(socket.id);
    if (uid) {
      users.delete(uid);
      sockets.delete(socket.id);
      nearbyUsers.delete(uid);
      console.log(`[-] ${uid} disconnected`);
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
