const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

// ---------- HTTP (liefert das Spiel aus, wie vorher) ----------
const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);

  // Leichter Endpunkt zum Wachhalten (z. B. UptimeRobot) – liest keine Datei
  if (urlPath === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }

  if (urlPath === '/' || urlPath === '') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(path.resolve(__dirname, 'Futuristic_Arena.html')).pipe(res);
    return;
  }

  const cleanRel = urlPath.replace(/^\/+/, '');
  const candidateNames = [cleanRel, `${cleanRel}.html`];
  for (const name of candidateNames) {
    const fullPath = path.resolve(__dirname, name);
    if (fullPath.startsWith(__dirname) && fs.existsSync(fullPath) && fs.statSync(fullPath).isFile()) {
      const ext = path.extname(fullPath).toLowerCase();
      let contentType = 'text/plain; charset=utf-8';
      if (ext === '.html') contentType = 'text/html; charset=utf-8';
      if (ext === '.js') contentType = 'application/javascript; charset=utf-8';
      if (ext === '.css') contentType = 'text/css; charset=utf-8';
      res.writeHead(200, { 'Content-Type': contentType });
      fs.createReadStream(fullPath).pipe(res);
      return;
    }
  }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Arquivo nao encontrado');
});

// Nagle aus: kleine Pakete sofort senden statt zu warten
server.on('connection', (socket) => socket.setNoDelay(true));

// ---------- WebSocket ----------
const wss = new WebSocket.Server({
  server,
  perMessageDeflate: false, // Kompression aus: spart viel CPU bei kleinen Nachrichten
  maxPayload: 64 * 1024,    // Schutz gegen riesige Nachrichten
});

const rooms = new Map();     // roomCode -> Map(clientId -> { ws, id, presence })
const directory = new Map(); // roomCode -> { c, n, p, m, updatedAt }

const MAX_BUFFER = 64 * 1024; // so viel darf pro Client ungesendet warten

function sendRaw(ws, data, droppable) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  // Position-Updates (droppable) werden übersprungen, wenn der Client hinterherhängt.
  // Das nächste Update ersetzt sie sowieso, so wächst die Warteschlange nie an.
  if (droppable && ws.bufferedAmount > MAX_BUFFER) return;
  ws.send(data);
}

function send(ws, obj) {
  sendRaw(ws, JSON.stringify(obj), false);
}

function broadcastToRoom(roomCode, senderId, obj, droppable) {
  const r = rooms.get(roomCode);
  if (!r || r.size === 0 || (r.size === 1 && r.has(senderId))) return; // niemand da, der etwas empfangen könnte
  const data = JSON.stringify(obj); // nur EINMAL in Text umwandeln
  for (const [cid, client] of r.entries()) {
    if (cid !== senderId) sendRaw(client.ws, data, droppable);
  }
}

function removeClient(ws) {
  const meta = ws._meta;
  if (!meta || !meta.room || !meta.id) return;
  const r = rooms.get(meta.room);
  if (r) {
    // Nur löschen, wenn der Eintrag wirklich zu DIESER Verbindung gehört
    const cur = r.get(meta.id);
    if (cur && cur.ws === ws) {
      r.delete(meta.id);
      broadcastToRoom(meta.room, meta.id, { t: 'l', id: meta.id }, false);
    }
    if (r.size === 0) {
      rooms.delete(meta.room);
      directory.delete(meta.room);
    }
  }
  ws._meta = null;
}

function dirList() {
  const now = Date.now();
  const list = [];
  for (const entry of directory.values()) {
    if (now - entry.updatedAt < 60000) {
      list.push({ c: entry.c, n: entry.n, p: entry.p, m: entry.m, ago: now - entry.updatedAt });
    }
  }
  return list;
}

wss.on('connection', (ws) => {
  if (ws._socket && ws._socket.setNoDelay) ws._socket.setNoDelay(true);
  ws._meta = { id: null, room: null };
  ws._alive = true;
  ws.on('pong', () => { ws._alive = true; });

  ws.on('message', (raw) => {
    ws._alive = true;
    let msg;
    try { msg = JSON.parse(String(raw)); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    if (msg.t === 'join') {
      const roomCode = String(msg.room || '').replace(/[^a-z0-9-]/gi, '').toLowerCase().slice(0, 16);
      const clientId = String(msg.id || '').slice(0, 24);
      if (!roomCode || !clientId) return;
      removeClient(ws);
      if (!rooms.has(roomCode)) rooms.set(roomCode, new Map());
      const r = rooms.get(roomCode);
      r.set(clientId, { ws, id: clientId, presence: null });
      ws._meta = { id: clientId, room: roomCode };
      send(ws, { t: 'ok' });
      for (const [otherId, other] of r.entries()) {
        if (otherId !== clientId && other.presence) {
          send(ws, { t: 'p', id: otherId, d: other.presence });
        }
      }
      return;
    }

    const meta = ws._meta;
    if (!meta || !meta.room || !meta.id) {
      if (msg.t === 'list') send(ws, { t: 'dir', list: dirList() });
      return;
    }

    if (msg.t === 'p' && msg.d && typeof msg.d === 'object') {
      const r = rooms.get(meta.room);
      if (r && r.has(meta.id)) r.get(meta.id).presence = msg.d;
      broadcastToRoom(meta.room, meta.id, { t: 'p', id: meta.id, d: msg.d }, true);
    } else if (msg.t === 'e' && typeof msg.e === 'string') {
      broadcastToRoom(meta.room, meta.id, { t: 'e', id: meta.id, e: msg.e, d: msg.d }, false);
    } else if (msg.t === 'x') {
      removeClient(ws);
    } else if (msg.t === 'dir') {
      const c = String(msg.c || '').replace(/[^a-z0-9-]/gi, '').toLowerCase().slice(0, 16);
      if (!c) return;
      if (!msg.e) {
        directory.delete(c);
      } else {
        const r = rooms.get(c);
        directory.set(c, {
          c,
          n: String(msg.e.n || c).slice(0, 20),
          p: r ? r.size : 1,
          m: Number(msg.e.m) || 0,
          updatedAt: Date.now(),
        });
      }
    } else if (msg.t === 'list') {
      send(ws, { t: 'dir', list: dirList() });
    }
  });

  ws.on('close', () => removeClient(ws));
  ws.on('error', () => removeClient(ws));
});

// Tote Verbindungen entfernen (Browser zu, WLAN weg ...), damit sie keine Warteschlange füllen
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws._alive === false) {
      removeClient(ws);
      ws.terminate();
      continue;
    }
    ws._alive = false;
    try { ws.ping(); } catch {}
  }
  // alte Einträge aus dem Gruppenverzeichnis räumen
  const now = Date.now();
  for (const [c, e] of directory.entries()) {
    if (now - e.updatedAt > 120000) directory.delete(c);
  }
}, 30000);

server.listen(PORT, () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
});
