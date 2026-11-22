const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);

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

const wss = new WebSocket.Server({ server });

const rooms = new Map();
const directory = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcastToRoom(roomCode, senderId, obj) {
  const r = rooms.get(roomCode);
  if (!r) return;
  for (const [cid, client] of r.entries()) {
    if (cid !== senderId) {
      send(client.ws, obj);
    }
  }
}

function removeClient(ws) {
  const meta = ws._meta;
  if (!meta || !meta.room || !meta.id) return;
  const r = rooms.get(meta.room);
  if (r) {
    r.delete(meta.id);
    broadcastToRoom(meta.room, meta.id, { t: 'l', id: meta.id });
    if (r.size === 0) {
      rooms.delete(meta.room);
      directory.delete(meta.room);
    }
  }
  ws._meta = null;
}

wss.on('connection', (ws) => {
  ws._meta = { id: null, room: null };

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
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
      if (msg.t === 'list') {
        const now = Date.now();
        const list = [];
        for (const [c, entry] of directory.entries()) {
          if (now - entry.updatedAt < 60000) {
            list.push({ c: entry.c, n: entry.n, p: entry.p, m: entry.m, ago: now - entry.updatedAt });
          }
        }
        send(ws, { t: 'dir', list });
      }
      return;
    }

    if (msg.t === 'p' && msg.d && typeof msg.d === 'object') {
      const r = rooms.get(meta.room);
      if (r && r.has(meta.id)) {
        r.get(meta.id).presence = msg.d;
      }
      broadcastToRoom(meta.room, meta.id, { t: 'p', id: meta.id, d: msg.d });
    } else if (msg.t === 'e' && typeof msg.e === 'string') {
      broadcastToRoom(meta.room, meta.id, { t: 'e', id: meta.id, e: msg.e, d: msg.d });
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
      const now = Date.now();
      const list = [];
      for (const [c, entry] of directory.entries()) {
        if (now - entry.updatedAt < 60000) {
          list.push({ c: entry.c, n: entry.n, p: entry.p, m: entry.m, ago: now - entry.updatedAt });
        }
      }
      send(ws, { t: 'dir', list });
    }
  });

  ws.on('close', () => removeClient(ws));
  ws.on('error', () => removeClient(ws));
});

server.listen(PORT, () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
});
