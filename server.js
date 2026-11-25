// Room-based chat + WebRTC calls.
// - Rooms are open: everyone who enters the same room name is in the same chat/call.
// - Messages are END-TO-END ENCRYPTED by the browsers: this server only ever sees
//   ciphertext (text/media/reply bodies arrive as "e1.<iv>.<ct>" blobs).
// - Messages AUTO-DELETE 5 minutes after they were sent (MSG_TTL_MS), for everyone.
// - Audio/video flows directly between browsers (mesh WebRTC); this server only
//   relays small signaling messages and call state.
const fs = require('fs');
const express = require('express');
const http = require('http');
const https = require('https');
const os = require('os');
const { Server } = require('socket.io');
const selfsigned = require('selfsigned');

const app = express();
const PORT = process.env.PORT || 3000;
const HTTPS_PORT = process.env.HTTPS_PORT || 3443;
const MAX_ROOM = parseInt(process.env.MAX_ROOM || process.env.MAX_GROUP || '50', 10); // people in a room chat
const MAX_CALL = parseInt(process.env.MAX_CALL || '6', 10);                           // people in one call (mesh)
const HIST_MAX = 60;                                                                  // kept messages per room (ciphertext)
const TTL_MS = Math.max(10000, parseInt(process.env.MSG_TTL_MS || '300000', 10));     // 5 minutes

app.use(express.static(__dirname + '/public'));
const httpServer = http.createServer(app);
const io = new Server({ maxHttpBufferSize: 6e6 });

/* ---------- persistent data: room meta + history (ciphertext only) ---------- */
const FILE = __dirname + '/data.json';
let db = { meta: {}, history: {} };              // meta[room] = { type, members: { uid: name } }
try { db = Object.assign(db, JSON.parse(fs.readFileSync(FILE, 'utf8'))); } catch (e) {}
let saveTimer;
const save = () => { clearTimeout(saveTimer); saveTimer = setTimeout(() => fs.writeFile(FILE, JSON.stringify(db), () => {}), 500); };
const findMsg = (room, id) => (db.history[room] || []).find((m) => m.id === id);
const liveMsgs = (room) => (db.history[room] || []).filter((m) => (m.exp || 0) > Date.now());

// Give every stored message an expiry (old rows without one expire right away).
for (const room of Object.keys(db.history)) {
  for (const m of db.history[room]) if (!m.exp) m.exp = m.ts + TTL_MS;
}

const calls = new Map();                         // room -> { startedAt, mode, starter, starterUid, parts: Map(socketId -> {name, mode}) }
const clip = (v, n) => String(v == null ? '' : v).slice(0, n);

const roomInfo = (room) => {
  const m = db.meta[room];
  return { room, type: m.type, max: MAX_ROOM,
           members: Object.entries(m.members).map(([uid, name]) => ({ uid, name })) };
};
const callInfo = (room) => {
  const c = calls.get(room);
  return c ? { count: c.parts.size, mode: c.mode, starter: c.starter, starterUid: c.starterUid, startedAt: c.startedAt } : null;
};
const payload = (room) => ({ ...roomInfo(room), ttl: TTL_MS, history: liveMsgs(room), call: callInfo(room) });

function leaveCall(socket, room) {
  const c = calls.get(room);
  if (!c || !c.parts.delete(socket.id)) return;
  for (const id of c.parts.keys()) io.to(id).emit('call-peer-left', { room, id: socket.id });
  if (!c.parts.size) calls.delete(room);
  io.to(room).emit('call-state', { room, call: callInfo(room) });
}

/* ---------- auto-delete sweep: every 10s, tell the room which messages expired ---------- */
function sweepExpired() {
  const now = Date.now();
  for (const room of Object.keys(db.history)) {
    const keep = db.history[room].filter((m) => (m.exp || 0) > now);
    if (keep.length !== db.history[room].length) {
      const ids = db.history[room].filter((m) => (m.exp || 0) <= now).map((m) => m.id);
      db.history[room] = keep;
      if (!keep.length) delete db.history[room];
      save();
      io.to(room).emit('expired', { room, ids });
    }
  }
}
setInterval(sweepExpired, 10000);

io.on('connection', (socket) => {
  socket.data.rooms = new Set();
  const inRoom = (room) => socket.data.rooms.has(room);
  // Rooms are open. If the server forgot a room (Render free restarts/sleeps and wipes memory),
  // quietly re-create it and re-join this socket so chat never silently stops working.
  const ensureIn = (room) => {
    if (inRoom(room)) return true;
    const uid = socket.data.uid;
    if (!room || !uid) return false;
    let m = db.meta[room];
    if (!m) m = db.meta[room] = { type: 'room', members: {} };
    if (!m.members[uid]) {
      if (Object.keys(m.members).length >= MAX_ROOM) return false;
      m.members[uid] = socket.data.name; save();
      socket.to(room).emit('members', roomInfo(room));
    }
    socket.join(room); socket.data.rooms.add(room);
    return true;
  };

  // Sent once on connect: who I am + the chats saved in my browser.
  socket.on('hello', (d = {}) => {
    socket.data.uid = clip(d.uid, 40) || socket.id;
    socket.data.name = clip(d.name, 30).trim() || 'Guest';
    const out = [];
    for (const room of Array.isArray(d.rooms) ? d.rooms.slice(0, 100) : []) {
      if (typeof room !== 'string' || !room || !ensureIn(room)) continue;   // rooms are open: re-join, or re-create if the server forgot it
      const m = db.meta[room];
      if (m.members[socket.data.uid] !== socket.data.name) { m.members[socket.data.uid] = socket.data.name; save(); io.to(room).emit('members', roomInfo(room)); }
      out.push(payload(room));
    }
    socket.emit('rooms', out);
  });

  // Start or join a room. Everyone with the same room name lands in the same room.
  socket.on('open', (d = {}) => {
    const room = clip(d.room, 64).trim(); const uid = socket.data.uid;
    if (!room || !uid) return;
    let m = db.meta[room];
    if (!m) m = db.meta[room] = { type: 'room', members: {} };
    if (!m.members[uid] && Object.keys(m.members).length >= MAX_ROOM) {
      socket.emit('notice', { text: `“${room}” is full (${MAX_ROOM} people).` });
      return;
    }
    m.members[uid] = socket.data.name; save();
    socket.join(room); socket.data.rooms.add(room);
    socket.emit('room', payload(room));
    socket.to(room).emit('members', roomInfo(room));
  });

  // m.text / m.media.* / m.reply.* arrive as ciphertext ("e1.<iv>.<ct>").
  socket.on('chat', (m, ack) => {
    const room = m && m.room;
    if (!room || !ensureIn(room)) return;
    const ts = Date.now();
    const msg = {
      id: clip(m.id, 40), uid: socket.data.uid, name: socket.data.name, ts,
      exp: ts + TTL_MS,                                   // auto-delete time
      text: clip(m.text, 4000), media: null, reactions: {},
      reply: m.reply ? { name: clip(m.reply.name, 60), text: clip(m.reply.text, 400) } : null,
    };
    if (m.media && typeof m.media.data === 'string' && m.media.data.length < 4e6) {
      const k = m.media.kind;
      msg.media = { kind: k === 'image' || k === 'audio' ? k : 'file', name: clip(m.media.name || 'file', 200), mime: clip(m.media.mime, 60), data: m.media.data };
    }
    if (!msg.id || (!msg.text.trim() && !msg.media)) return;
    (db.history[room] = db.history[room] || []).push(msg);
    if (db.history[room].length > HIST_MAX) db.history[room].shift();
    save();
    socket.to(room).emit('chat', { ...msg, room });
    if (typeof ack === 'function') ack(msg.ts);
  });
  socket.on('receipt', (d = {}) => { if (inRoom(d.room)) socket.to(d.room).emit('receipt', { room: d.room, id: d.id, kind: d.kind }); });
  socket.on('typing', (d = {}) => { if (ensureIn(d.room)) socket.to(d.room).emit('typing', { room: d.room, uid: socket.data.uid, name: socket.data.name, on: !!d.on }); });
  socket.on('react', (d = {}) => {
    const m = inRoom(d.room) && findMsg(d.room, d.id);
    if (!m || m.deleted) return;
    m.reactions = m.reactions || {};
    if (m.reactions[socket.data.uid] === d.emoji) delete m.reactions[socket.data.uid];
    else m.reactions[socket.data.uid] = clip(d.emoji, 8);
    save(); io.to(d.room).emit('reactions', { room: d.room, id: d.id, reactions: m.reactions });
  });
  socket.on('delete', (d = {}) => {
    const m = inRoom(d.room) && findMsg(d.room, d.id);
    if (!m || m.uid !== socket.data.uid) return;          // only your own messages, at any time
    Object.assign(m, { deleted: true, text: '', media: null, reply: null, reactions: {} });
    save(); io.to(d.room).emit('deleted', { room: d.room, id: d.id });
  });

  /* ----- calls: one call per room; everyone in the room is told when it starts ----- */
  socket.on('call-join', (d = {}) => {
    const room = d.room; if (!ensureIn(room)) return;
    const mode = d.mode === 'audio' ? 'audio' : 'video';
    let c = calls.get(room);
    if (!c) { c = { startedAt: Date.now(), mode, starter: socket.data.name, starterUid: socket.data.uid, parts: new Map() }; calls.set(room, c); }
    if (c.parts.size >= MAX_CALL) { socket.emit('call-full', { room, max: MAX_CALL }); return; }
    const peers = [...c.parts].map(([id, p]) => ({ id, name: p.name, mode: p.mode }));
    c.parts.set(socket.id, { name: socket.data.name, mode });
    socket.emit('call-joined', { room, id: socket.id, peers });          // the newcomer sends the offers
    for (const [id] of c.parts) if (id !== socket.id) io.to(id).emit('call-peer-joined', { room, id: socket.id, name: socket.data.name, mode });
    io.to(room).emit('call-state', { room, call: callInfo(room) });
  });
  socket.on('call-leave', (d = {}) => leaveCall(socket, d.room));
  socket.on('signal', (d = {}) => {
    const c = calls.get(d.room);
    if (c && c.parts.has(socket.id) && c.parts.has(d.to)) io.to(d.to).emit('signal', { room: d.room, from: socket.id, data: d.data });
  });
  socket.on('state', (d = {}) => {
    const c = calls.get(d.room);
    if (c && c.parts.has(socket.id)) for (const id of c.parts.keys()) if (id !== socket.id) io.to(id).emit('state', { from: socket.id, mic: !!d.mic, cam: !!d.cam });
  });

  socket.on('disconnect', () => { for (const room of socket.data.rooms) leaveCall(socket, room); });
});

function lanIPs() {
  return Object.values(os.networkInterfaces()).flat()
    .filter((i) => i && i.family === 'IPv4' && !i.internal).map((i) => i.address);
}

(async () => {
  // Self-signed HTTPS so a phone on the same Wi-Fi can use camera/mic.
  // Behind a real HTTPS proxy (Caddy/Nginx) you only need the HTTP port.
  const ips = lanIPs();
  const pems = await selfsigned.generate([{ name: 'commonName', value: 'webrtc-chat.local' }], {
    days: 365, keySize: 2048, algorithm: 'sha256',
    extensions: [{ name: 'subjectAltName', altNames: [
      { type: 2, value: 'localhost' }, { type: 7, ip: '127.0.0.1' }, ...ips.map((ip) => ({ type: 7, ip })),
    ] }],
  });
  const httpsServer = https.createServer({ key: pems.private, cert: pems.cert }, app);
  io.attach(httpServer); io.attach(httpsServer);
  httpServer.listen(PORT, () => console.log(`HTTP  (PC only): http://localhost:${PORT}`));
  httpsServer.listen(HTTPS_PORT, () => {
    console.log(`HTTPS (use on phone): ${ips.map((ip) => `https://${ip}:${HTTPS_PORT}`).join('  ') || `https://localhost:${HTTPS_PORT}`}`);
  });
  console.log(`Message auto-delete: ${TTL_MS / 1000}s   Room cap: ${MAX_ROOM}   Call cap: ${MAX_CALL}`);
})();
