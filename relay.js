// Serveur relais Animeverse Arena : relie les joueurs qui ne sont pas sur le même Wi-Fi.
// Il ne calcule rien : il crée des salons, met les joueurs ensemble et transmet les messages entre eux.
//  - Duel / Arène : recherche automatique (les deux premiers joueurs d'une même file sont mis ensemble).
//  - Raid en équipe : groupes de 3 joueurs au plus, publics (vus par tous) ou privés (sur invitation),
//    recherche automatique d'un groupe, et l'hôte qui part ne ferme pas le groupe.
//  - Présence : chaque jeu connecté se présente (« hello ») pour pouvoir recevoir des invitations.
// Lancer :  npm install  puis  npm start   (port : variable PORT, 8080 par défaut)
const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 6;
const RAID_MAX = 3;
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const rooms = new Map();    // code -> { members: Map(id -> ws), next, raid? }
const queues = new Map();   // file de recherche (« duel », « eq », « rnd ») -> joueur qui attend un adversaire
const raidq = new Map();    // boss -> joueur sans groupe qui cherche un raid
const presence = new Map(); // identifiant du joueur (pid) -> sa connexion « présence »

const server = http.createServer((req, res) => {
  let groups = 0;
  for (const r of rooms.values()) if (r.raid) groups++;
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end(`Animeverse Arena relais OK - ${rooms.size} salon(s), ${groups} groupe(s) de raid, ${queues.size} joueur(s) en recherche, ${presence.size} joueur(s) connecté(s)\n`);
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
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// retire un joueur de la file de recherche où il attend
function unqueue(ws) {
  if (ws.queue && queues.get(ws.queue) === ws) queues.delete(ws.queue);
  ws.queue = null;
  if (ws.raidq && raidq.get(ws.raidq) === ws) raidq.delete(ws.raidq);
  ws.raidq = null;
}

function leave(ws) {
  const code = ws.room;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  room.members.delete(ws.pid);
  if (room.members.size === 0 || (ws.pid === 1 && !room.raid)) {
    // un salon normal se ferme quand l'hôte part ; un groupe de raid seulement quand il est vide
    for (const m of room.members.values()) { send(m, { t: "closed" }); m.room = null; }
    rooms.delete(code);
  } else {
    for (const m of room.members.values()) send(m, { t: "left", id: ws.pid });
  }
}

// met un joueur dans un salon existant
function enter(ws, code, room, extra) {
  const id = room.next++;
  room.members.set(id, ws);
  ws.room = code; ws.pid = id;
  send(ws, Object.assign({ t: "joined", room: code, id }, extra || {}));
}

// fiche publique d'un groupe de raid
function groupInfo(code, r) {
  return { room: code, boss: r.raid.boss, size: r.members.size, max: RAID_MAX, pub: r.raid.pub,
    state: r.raid.state, wave: r.raid.wave, host: r.raid.hostName };
}

function newRaid(ws, boss, pub, name) {
  const code = newCode();
  const room = { members: new Map([[1, ws]]), next: 2, raid: { boss, pub, state: "lobby", wave: 1, hostName: name, invited: new Set() } };
  rooms.set(code, room);
  ws.room = code; ws.pid = 1;
  return [code, room];
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
      enter(ws, code, room);
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

    // ---------------- RAID EN ÉQUIPE ----------------
    } else if (m.t === "raid_create") {
      leave(ws);
      unqueue(ws);
      const [code] = newRaid(ws, String(m.boss || "groxmor").slice(0, 24), m.pub !== false, String(m.name || "?").slice(0, 16));
      send(ws, { t: "joined", room: code, id: 1, raid: true });
    } else if (m.t === "raid_join") {
      leave(ws);
      unqueue(ws);
      const code = String(m.room || "").toUpperCase().trim();
      const room = rooms.get(code);
      if (!room || !room.raid) return send(ws, { t: "error", msg: "Ce groupe n'existe plus." });
      if (room.members.size >= RAID_MAX) return send(ws, { t: "error", msg: "Ce groupe est complet." });
      if (!room.raid.pub && !room.raid.invited.has(String(m.pid || ""))) return send(ws, { t: "error", msg: "Ce groupe est privé : il faut une invitation." });
      enter(ws, code, room, { raid: true });
    } else if (m.t === "raid_find") {
      // recherche : d'abord un groupe public pas complet (de préférence pas en combat), sinon un autre joueur seul
      leave(ws);
      unqueue(ws);
      const boss = String(m.boss || "groxmor").slice(0, 24);
      let best = null;
      for (const [code, r] of rooms) {
        if (!r.raid || !r.raid.pub || r.raid.boss !== boss || r.members.size >= RAID_MAX) continue;
        if (!best || (best[1].raid.state !== "lobby" && r.raid.state === "lobby")) best = [code, r];
      }
      if (best) return enter(ws, best[0], best[1], { raid: true, match: true });
      const other = raidq.get(boss);
      if (other && other !== ws && other.readyState === 1) {
        raidq.delete(boss);
        other.raidq = null;
        const [code, room] = newRaid(other, boss, true, String(other.pname || "?"));
        send(other, { t: "joined", room: code, id: 1, raid: true, match: true });
        enter(ws, code, room, { raid: true, match: true });
      } else {
        ws.pname = String(m.name || "?").slice(0, 16);
        raidq.set(boss, ws);
        ws.raidq = boss;
        send(ws, { t: "queued", q: "raid" });
      }
    } else if (m.t === "raid_state") {
      // l'hôte du groupe annonce où en est le raid (salle d'attente / combat, vague)
      const room = rooms.get(ws.room);
      if (!room || !room.raid) return;
      if (m.state) room.raid.state = String(m.state).slice(0, 12);
      if (m.wave) room.raid.wave = Number(m.wave) || 1;
      if (m.pub !== undefined) room.raid.pub = !!m.pub;
      if (m.host) room.raid.hostName = String(m.host).slice(0, 16);
    } else if (m.t === "raid_list") {
      const list = [];
      for (const [code, r] of rooms) if (r.raid && r.raid.pub) list.push(groupInfo(code, r));
      send(ws, { t: "raid_list", groups: list.slice(0, 30) });

    // ---------------- PRÉSENCE ET INVITATIONS ----------------
    } else if (m.t === "hello") {
      const pid = String(m.pid || "");
      if (pid.length < 8) return;
      if (ws.ppid && presence.get(ws.ppid) === ws) presence.delete(ws.ppid);
      ws.ppid = pid;
      ws.pname = String(m.name || "?").slice(0, 16);
      presence.set(pid, ws);
      send(ws, { t: "hello_ok" });
    } else if (m.t === "invite") {
      // invitation d'un joueur à un autre (raid : on l'ajoute aux invités du groupe privé)
      const to = presence.get(String(m.to || ""));
      if (!to || to.readyState !== 1) return send(ws, { t: "invite_fail", to: m.to, why: "absent" });
      const data = m.data || {};
      if (data.kind === "raid") {
        const room = rooms.get(String(data.room || ""));
        if (!room || !room.raid) return send(ws, { t: "invite_fail", to: m.to, why: "groupe" });
        room.raid.invited.add(String(m.to));
      }
      send(to, { t: "invite", from: ws.ppid || "", name: ws.pname || "?", data });
      send(ws, { t: "invite_ok", to: m.to });
    } else if (m.t === "ping") {
      send(ws, { t: "pong" });
    } else if (m.t === "leave") {
      unqueue(ws);
      leave(ws);
    }
  });
  ws.on("close", () => {
    unqueue(ws);
    leave(ws);
    if (ws.ppid && presence.get(ws.ppid) === ws) presence.delete(ws.ppid);
  });
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
