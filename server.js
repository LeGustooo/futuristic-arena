const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.resolve(__dirname);
const MAIN_PAGE = 'Futuristic_Arena.html';

// Somente estes tipos de arquivo podem ser baixados (server.js e package.json ficam protegidos)
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.wav': 'audio/wav',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
};
const BLOCKED = new Set(['server.js', 'package.json', 'package-lock.json']);

function serveFile(res, fullPath, contentType) {
  const stream = fs.createReadStream(fullPath);
  stream.on('error', () => {
    if (!res.headersSent) res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Erro ao ler arquivo');
  });
  res.writeHead(200, { 'Content-Type': contentType, 'Cache-Control': 'no-cache' });
  stream.pipe(res);
}

const server = http.createServer((req, res) => {
  let urlPath;
  try {
    urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  } catch {
    res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('URL invalida');
    return;
  }

  // Health check (útil para o Render)
  if (urlPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }

  if (urlPath === '/' || urlPath === '') {
    serveFile(res, path.join(ROOT, MAIN_PAGE), MIME['.html']);
    return;
  }

  const cleanRel = urlPath.replace(/^\/+/, '');
  const candidateNames = [cleanRel, `${cleanRel}.html`];

  for (const name of candidateNames) {
    const fullPath = path.resolve(ROOT, name);

    // Impede sair da pasta do projeto (path traversal)
    if (!fullPath.startsWith(ROOT + path.sep)) continue;

    const rel = path.relative(ROOT, fullPath);
    const parts = rel.split(path.sep);
    if (parts.some((p) => p.startsWith('.') || p === 'node_modules')) continue;
    if (BLOCKED.has(rel)) continue;

    const ext = path.extname(fullPath).toLowerCase();
    if (!MIME[ext]) continue;

    let stat;
    try {
      stat = fs.statSync(fullPath);
    } catch {
      continue;
    }
    if (!stat.isFile()) continue;

    serveFile(res, fullPath, MIME[ext]);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Arquivo nao encontrado');
});

const wss = new WebSocket.Server({
  server,
  perMessageDeflate: false, // compressão adiciona latência em mensagens pequenas
  maxPayload: 16 * 1024, // 16 KB por mensagem
});

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
  // Serializa uma vez só e envia para todos
  const data = JSON.stringify(obj);
  for (const [cid, client] of r.entries()) {
    if (cid !== senderId && client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(data);
    }
  }
}

function removeClient(ws) {
  const meta = ws._meta;
  if (!meta || !meta.room || !meta.id) return;
  const r = rooms.get(meta.room);
  if (r) {
    const entry = r.get(meta.id);
    // CORREÇÃO: só remove se esta conexão ainda é a dona desse id.
    // Antes, uma conexão antiga fechando (ex.: reconexão no celular) apagava a nova.
    if (entry && entry.ws === ws) {
      r.delete(meta.id);
      broadcastToRoom(meta.room, meta.id, { t: 'l', id: meta.id });
    }
    if (r.size === 0) {
      rooms.delete(meta.room);
      directory.delete(meta.room);
    }
  }
  ws._meta = null;
}

function sendDirectory(ws) {
  const now = Date.now();
  const list = [];
  for (const [c, entry] of directory.entries()) {
    if (now - entry.updatedAt < 60000) {
      list.push({ c: entry.c, n: entry.n, p: entry.p, m: entry.m, ago: now - entry.updatedAt });
    }
  }
  send(ws, { t: 'dir', list });
}

function cleanRoomCode(v) {
  return String(v || '').replace(/[^a-z0-9-]/gi, '').toLowerCase().slice(0, 16);
}

wss.on('connection', (ws) => {
  ws._meta = { id: null, room: null };
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    ws.isAlive = true;

    let msg;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (!msg || typeof msg.t !== 'string') return;

    if (msg.t === 'join') {
      const roomCode = cleanRoomCode(msg.room);
      const clientId = String(msg.id || '').slice(0, 24);
      if (!roomCode || !clientId) return;

      removeClient(ws);

      if (!rooms.has(roomCode)) rooms.set(roomCode, new Map());
      const r = rooms.get(roomCode);

      // Se o mesmo id já estava conectado (reconexão), fecha a conexão antiga
      const old = r.get(clientId);
      if (old && old.ws !== ws) {
        old.ws._meta = null;
        try {
          old.ws.terminate();
        } catch {}
      }

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
      if (msg.t === 'list') sendDirectory(ws);
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
      const c = cleanRoomCode(msg.c);
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
      sendDirectory(ws);
    }
  });

  ws.on('close', () => removeClient(ws));
  ws.on('error', () => removeClient(ws));
});

// Heartbeat: o proxy do Render derruba conexões ociosas, e no celular as conexões
// "mortas" ficam penduradas. Isso mantém a conexão viva e limpa as quebradas.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch {}
  }
}, 25000);

// Limpa entradas antigas do diretório de salas
const dirCleanup = setInterval(() => {
  const now = Date.now();
  for (const [c, entry] of directory.entries()) {
    if (now - entry.updatedAt > 120000) directory.delete(c);
  }
}, 30000);

wss.on('close', () => {
  clearInterval(heartbeat);
  clearInterval(dirCleanup);
});

// Evita que um erro inesperado derrube o servidor inteiro
process.on('uncaughtException', (err) => console.error('uncaughtException:', err));
process.on('unhandledRejection', (err) => console.error('unhandledRejection:', err));

process.on('SIGTERM', () => {
  wss.clients.forEach((c) => c.close(1001, 'server restarting'));
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
