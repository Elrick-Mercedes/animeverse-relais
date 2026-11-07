// Serveur relais Animeverse Arena : relie les joueurs qui ne sont pas sur le même Wi-Fi.
// Il ne calcule rien : il crée des salons avec un code et transmet les messages entre joueurs.
// Lancer :  npm install  puis  npm start   (port : variable PORT, 8080 par défaut)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 6;
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const rooms = new Map(); // code -> { members: Map(id -> ws), next: id suivant }
const queues = new Map(); // file de recherche (« duel », « eq », « rnd ») -> joueur qui attend un adversaire

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end(`Animeverse Arena relais OK - ${rooms.size} salon(s), ${queues.size} joueur(s) en recherche\n`);
});
const wss = new WebSocketServer({ server });

function newCode() {
  let c;
  do {
    c = "";
    for (let i = 0; i < 5; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
  } while (rooms.has(c));
  return c;
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// retire un joueur de la file de recherche où il attend
function unqueue(ws) {
  if (ws.queue && queues.get(ws.queue) === ws) queues.delete(ws.queue);
  ws.queue = null;
}

function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  room.members.delete(ws.pid);
  if (ws.pid === 1 || room.members.size === 0) {
    for (const m of room.members.values()) { send(m, { t: "closed" }); m.room = null; }
    rooms.delete(ws.room);
  } else {
    for (const m of room.members.values()) send(m, { t: "left", id: ws.pid });
  }
  ws.room = null;
}

wss.on("connection", (ws) => {
  ws.alive = true;
  ws.on("pong", () => (ws.alive = true));
  ws.on("message", (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "create") {
      leave(ws);
      const code = newCode();
      rooms.set(code, { members: new Map([[1, ws]]), next: 2 });
      ws.room = code; ws.pid = 1;
      send(ws, { t: "joined", room: code, id: 1 });
    } else if (m.t === "join") {
      leave(ws);
      const code = String(m.room || "").toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { t: "error", msg: "Salon introuvable. Vérifie le code." });
      if (room.members.size >= MAX_PLAYERS) return send(ws, { t: "error", msg: "Salon plein (6 joueurs)." });
      const id = room.next++;
      room.members.set(id, ws);
      ws.room = code; ws.pid = id;
      send(ws, { t: "joined", room: code, id });
    } else if (m.t === "send") {
      const room = rooms.get(ws.room);
      if (!room) return;
      const out = { t: "msg", from: ws.pid, data: m.data };
      if (m.to) { const dst = room.members.get(m.to); if (dst) send(dst, out); }
      else for (const [id, dst] of room.members) if (id !== ws.pid) send(dst, out);
    } else if (m.t === "queue") {
      // recherche automatique : les deux premiers joueurs de la même file sont mis ensemble dans un salon
      leave(ws);
      unqueue(ws);
      const q = String(m.q || "duel").slice(0, 12);
      const other = queues.get(q);
      if (other && other !== ws && other.readyState === 1) {
        queues.delete(q);
        other.queue = null;
        const code = newCode();
        rooms.set(code, { members: new Map([[1, other], [2, ws]]), next: 3 });
        other.room = code; other.pid = 1;
        ws.room = code; ws.pid = 2;
        send(other, { t: "joined", room: code, id: 1, match: true });
        send(ws, { t: "joined", room: code, id: 2, match: true });
      } else {
        queues.set(q, ws);
        ws.queue = q;
        send(ws, { t: "queued", q });
      }
    } else if (m.t === "unqueue") {
      unqueue(ws);
    } else if (m.t === "ping") {
      send(ws, { t: "pong" });
    } else if (m.t === "leave") {
      unqueue(ws);
      leave(ws);
    }
  });
  ws.on("close", () => { unqueue(ws); leave(ws); });
});

// garde les connexions ouvertes et nettoie les joueurs disparus
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => console.log("Relais Animeverse Arena sur le port " + PORT));
