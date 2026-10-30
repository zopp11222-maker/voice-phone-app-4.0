"use strict";
// Voice Phone relay: ห้องละ 1 โฮสต์ (ส่งตำแหน่งผู้เล่นจากเกม) + เพื่อนหลายคน (ส่ง/รับเสียง)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_CLIENTS = 16;
const ROOM_RE = /^\/([A-Za-z0-9_-]{4,32})\/?$/;
const rooms = new Map();

function getRoom(code) {
  let r = rooms.get(code);
  if (!r) {
    r = { code, host: null, hostSeen: 0, mc: false, players: new Map(), clients: new Set(), nextId: 0 };
    rooms.set(code, r);
  }
  return r;
}
const hostAlive = (room) => !!room.host && room.host.readyState === 1;
const keyOf = (name) => String(name || "").toLowerCase();

// ความดังตามระยะ: ใกล้ <=40% ของระยะ = เต็ม แล้วค่อยๆ เบาลงจนเงียบที่ระยะสูงสุด / คุยโทรศัพท์ = เต็มทุกระยะ
function gainFor(s, l) {
  const sc = s.call.toLowerCase(), lc = l.call.toLowerCase();
  if (sc && lc && sc === l.name.toLowerCase() && lc === s.name.toLowerCase()) return 1;
  if (!s.pos || !l.pos || s.dim !== l.dim) return 0;
  const d = Math.hypot(s.pos[0] - l.pos[0], s.pos[1] - l.pos[1], s.pos[2] - l.pos[2]);
  const r = s.range;
  if (d > r) return 0;
  const near = r * 0.4;
  if (d <= near) return 1;
  return Math.max(0, 1 - (d - near) / (r - near));
}

function statusFor(room, ws) {
  const me = ws.vname ? room.players.get(keyOf(ws.vname)) : null;
  const list = [...room.players.values()].map((p) => ({ name: p.name, range: p.range, muted: p.muted, call: p.call }));
  return JSON.stringify({
    t: "status", mc: room.mc && hostAlive(room), game: !!me,
    range: me ? me.range : 0, muted: me ? me.muted : true, call: me ? me.call : "", players: list,
  });
}

function onAudio(room, ws, data) {
  if (!ws.vname) return;
  const s = room.players.get(keyOf(ws.vname));
  if (!s || s.muted) return;
  const buf = Buffer.isBuffer(data) ? data : Buffer.concat(data);
  if (buf.length < 2 || buf.length > 4096) return;
  for (const w2 of room.clients) {
    if (w2 === ws || w2.readyState !== 1 || !w2.vname) continue;
    const l = room.players.get(keyOf(w2.vname));
    if (!l) continue;
    const g = gainFor(s, l);
    if (g < 0.02) continue;
    const out = Buffer.allocUnsafe(2 + buf.length);
    out[0] = ws.vid;
    out[1] = Math.round(g * 255);
    buf.copy(out, 2);
    w2.send(out, { binary: true });
  }
}

function applySnapshot(room, j) {
  room.mc = !!j.mc;
  room.hostSeen = Date.now();
  const next = new Map();
  for (const p of (Array.isArray(j.list) ? j.list : []).slice(0, 64)) {
    if (!p || typeof p.name !== "string") continue;
    const name = p.name.slice(0, 32);
    const hasPos = Number.isFinite(p.x) && Number.isFinite(p.z);
    next.set(keyOf(name), {
      name,
      range: Math.min(128, Math.max(1, Math.round(Number(p.range)) || 30)),
      muted: !!p.muted,
      call: String(p.call || "").slice(0, 32),
      pos: hasPos ? [Number(p.x), Number.isFinite(p.y) ? Number(p.y) : 0, Number(p.z)] : null,
      dim: Number(p.dim) | 0,
    });
  }
  room.players = next;
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("Voice Phone relay: OK\n");
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

wss.on("connection", (ws, req) => {
  const m = ROOM_RE.exec((req.url || "").split("?")[0]);
  if (!m) { ws.close(1008, "bad room"); return; }
  const room = getRoom(m[1]);
  ws.isAlive = true;
  ws.role = null;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("error", () => {});

  ws.on("message", (data, isBinary) => {
    ws.isAlive = true;
    if (isBinary) { if (ws.role === "client") onAudio(room, ws, data); return; }
    let j;
    try { j = JSON.parse(data.toString()); } catch (e) { return; }
    if (!j || typeof j !== "object") return;

    if (j.t === "host") {
      if (hostAlive(room) && room.host !== ws) room.host.close(1000, "replaced");
      ws.role = "host";
      room.host = ws;
      room.hostSeen = Date.now();
    } else if (j.t === "players" && ws.role === "host") {
      applySnapshot(room, j);
    } else if (j.t === "join") {
      if (room.clients.size >= MAX_CLIENTS) { ws.close(1013, "room full"); return; }
      ws.role = "client";
      ws.vname = String(j.name || "").trim().slice(0, 32);
      ws.vid = room.nextId++ % 250;
      room.clients.add(ws);
      ws.send(statusFor(room, ws));
    } else if (j.t === "mute" && ws.role === "client") {
      const p = room.players.get(keyOf(ws.vname));
      if (!p) return;
      const mv = j.m === 1 ? 1 : 0;
      p.muted = mv === 1;
      if (hostAlive(room)) room.host.send(JSON.stringify({ t: "cmd", name: p.name, m: mv }));
    }
  });

  ws.on("close", () => {
    room.clients.delete(ws);
    if (room.host === ws) { room.host = null; room.mc = false; room.players = new Map(); }
    if (!room.host && room.clients.size === 0) rooms.delete(room.code);
  });
});

// ส่งสถานะให้ทุกคนทุก 1 วินาที + เคลียร์ข้อมูลโฮสต์ที่เงียบหายไป
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    if (room.host && now - room.hostSeen > 10000) { room.mc = false; room.players = new Map(); }
    for (const ws of room.clients) if (ws.readyState === 1) ws.send(statusFor(room, ws));
  }
}, 1000);

// ping เป็นระยะ กันถูกตัดเพราะเงียบ และเก็บกวาดการเชื่อมต่อที่ตายแล้ว
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  }
}, 20000);

server.listen(PORT, () => console.log("Voice Phone relay listening on " + PORT));
module.exports = { gainFor, statusFor, rooms };
                       
