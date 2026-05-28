// =============================================================
// L'ALIBI — Serveur Backend
// Stack : Fastify + Socket.IO + Redis (ioredis) + Anthropic SDK
// =============================================================
import Fastify from "fastify";
import cors from "@fastify/cors";
import { Server as SocketIO } from "socket.io";
import Redis from "ioredis";
import Anthropic from "@anthropic-ai/sdk";
import { v4 as uuidv4 } from "uuid";

// =============================================================
// CONFIG
// =============================================================
const PORT        = parseInt(process.env.PORT || "3001");
const REDIS_URL   = process.env.REDIS_URL || "redis://localhost:6379";
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || "";
const CLIENT_URL  = process.env.CLIENT_URL || "http://localhost:5173";
const ROOM_TTL    = 60 * 60 * 4; // 4 heures en secondes
const PREP_DURATION = 90;
const THEMES = ["musée","boulangerie","zoo","espace","sous-marin","casino",
                "bibliothèque","stade","opéra","cirque","centrale nucléaire","manège"];

// =============================================================
// PHASES — miroir exact du front
// =============================================================
const PHASES = {
  LOBBY:            "LOBBY",
  BRIEFING:         "BRIEFING",
  PREP_TIMER:       "PREP_TIMER",
  INTERROGATION_A:  "INTERROGATION_A",
  ISOLATION_B:      "ISOLATION_B",
  INTERROGATION_B:  "INTERROGATION_B",
  CONFRONTATION:    "CONFRONTATION",
  BETWEEN_TEAMS:    "BETWEEN_TEAMS",
  VERDICT:          "VERDICT",
};

// =============================================================
// RÔLES CLIENT — ce que chaque socket peut voir/faire
// =============================================================
const ROLES = {
  HOST:      "HOST",      // animateur (laptop/TV centrale)
  PLAYER_A:  "PLAYER_A",  // suspect A
  PLAYER_B:  "PLAYER_B",  // suspect B
  SPECTATOR: "SPECTATOR", // lecture seule
};

// =============================================================
// REDIS — state persistant par room
// =============================================================
const redis = new Redis(REDIS_URL, {
  maxRetriesPerRequest: 3,
  lazyConnect: true,
});

redis.on("error", (err) => console.error("[Redis]", err.message));
redis.on("connect", () => console.log("[Redis] connecté"));

const roomKey = (roomId) => `room:${roomId}`;

async function getRoom(roomId) {
  const raw = await redis.get(roomKey(roomId));
  return raw ? JSON.parse(raw) : null;
}

async function saveRoom(room) {
  await redis.setex(roomKey(room.roomId), ROOM_TTL, JSON.stringify(room));
}

async function deleteRoom(roomId) {
  await redis.del(roomKey(roomId));
}

// =============================================================
// STATE FACTORY
// =============================================================
const createRound = (teamId) => ({
  teamId,
  scenario:  null,
  answers:   {},   // "A-1": "texte...", "B-1": "texte..."
  judgments: {},   // 1: { isMatching, reason }
});

const createRoom = (roomId, mode = "2P") => ({
  roomId,
  mode,                   // "2P" | "4P"
  phase:      PHASES.LOBBY,
  activeTeam: 1,
  players:    [],         // [{ id, name, role, team, socketId }]
  rounds:     [],
  prepTimer:  null,       // setInterval handle (non sérialisé)
  createdAt:  Date.now(),
});

const activeRound = (room) =>
  room.rounds.find((r) => r.teamId === room.activeTeam) || null;

// =============================================================
// ANTHROPIC
// =============================================================
const anthropic = new Anthropic({ apiKey: ANTHROPIC_KEY });

async function callClaude(system, user) {
  const msg = await anthropic.messages.create({
    model:      "claude-haiku-4-5-20251001",
    max_tokens: 1000,
    system,
    messages:   [{ role: "user", content: user }],
  });
  const text = msg.content.map((b) => b.text || "").join("");
  return JSON.parse(text.replace(/```json|```/g, "").trim());
}

const GENERATOR_SYSTEM = `Tu es le "Commissaire", animateur d'un jeu de soirée appelé L'Alibi.
Génère un scénario de crime absurde et les questions d'interrogatoire.

RÈGLES :
- Délit : farfelu, précis, drôle. Un lieu + un méfait + une circonstance absurde.
- 5 questions sur des DÉTAILS CONCRETS de l'alibi (où, avec qui, quoi, détail sensoriel, action précise). Anodines en apparence, pièges en réalité.
- Langue française, ton décalé et dramatique.

Réponds UNIQUEMENT avec ce JSON strict :
{"accusation":"string (max 20 mots)","accusation_dramatique":"string (version TTS théâtrale, 2-3 phrases)","questions":[{"id":1,"text":"string"},{"id":2,"text":"string"},{"id":3,"text":"string"},{"id":4,"text":"string"},{"id":5,"text":"string"}]}`;

const JUDGE_SYSTEM = `Tu es le "Juge" d'un jeu de soirée comparant deux témoignages.

RÈGLES :
1. TOLÉRANCE SÉMANTIQUE : "voiture rouge" et "Clio bordeaux" = CONCORDENT. "métro" et "voiture" = CONTREDISENT. "vers 22h" et "presque minuit" = CONCORDENT. Vague ≠ contradiction automatique.
2. Réponse manquante = contradiction légère (isMatching: false).
3. "reason" : max 12 mots, sarcastique, ton juge télé-réalité.

Réponds UNIQUEMENT avec : {"isMatching":boolean,"reason":"string"}`;

const TIEBREAK_SYSTEM = `Tu es le "Grand Juge" d'un jeu de soirée. Deux équipes ont le même nombre d'incohérences. Tu dois choisir un perdant selon la GRAVITÉ et l'ABSURDITÉ des contradictions, pas le nombre.

Réponds UNIQUEMENT avec : {"loser":1|2,"verdict":"string (max 20 mots, sarcastique et définitif)"}`;

const SPEECH_SYSTEM = `Tu es un juge dramatique de jeu de soirée. Discours de verdict final en français, drôle, sarcastique et théâtral. Moque-toi des contradictions spécifiques. Max 100 mots.
Réponds UNIQUEMENT avec {"speech":"string"}`;

// =============================================================
// FASTIFY
// =============================================================
const app = Fastify({ logger: { level: "warn" } });
await app.register(cors, {
  origin: [CLIENT_URL, /localhost/],
  methods: ["GET", "POST"],
});

// Health check (Railway ping)
app.get("/health", async () => ({ status: "ok", ts: Date.now() }));

// Route : créer une room (appelée par le host avant de partager le code)
app.post("/rooms", async (req, reply) => {
  const roomId = Math.random().toString(36).substring(2, 6).toUpperCase();
  const room = createRoom(roomId, req.body?.mode || "2P");
  await saveRoom(room);
  return { roomId };
});

// Route : vérifier qu'une room existe
app.get("/rooms/:roomId", async (req, reply) => {
  const room = await getRoom(req.params.roomId);
  if (!room) return reply.code(404).send({ error: "Room introuvable" });
  return { roomId: room.roomId, mode: room.mode, phase: room.phase, playerCount: room.players.length };
});

// =============================================================
// SOCKET.IO
// =============================================================
const io = new SocketIO(app.server, {
  cors: { origin: [CLIENT_URL, /localhost/], methods: ["GET", "POST"] },
  pingTimeout:  20000,
  pingInterval: 10000,
});

// ---------- helpers ----------
function broadcastState(roomId, room) {
  // On émet le state complet SAUF les réponses de l'adversaire en cours d'interrogatoire
  // (le serveur filtre : le joueur B ne voit pas les réponses de A pendant INTERROGATION_A)
  io.to(roomId).emit("state:update", sanitizeForBroadcast(room));
}

function sanitizeForBroadcast(room) {
  // Copie légère — on retire les socketIds pour la sécurité
  return {
    ...room,
    players: room.players.map(({ socketId: _, ...p }) => p),
  };
}

// Émet uniquement vers le socket d'un joueur spécifique
function emitToPlayer(room, role, teamId, event, payload) {
  const player = room.players.find(
    (p) => p.role === role && p.team === teamId
  );
  if (player?.socketId) {
    io.to(player.socketId).emit(event, payload);
  }
}

// Émet vers tous les membres d'une équipe
function emitToTeam(room, teamId, event, payload) {
  room.players
    .filter((p) => p.team === teamId)
    .forEach((p) => p.socketId && io.to(p.socketId).emit(event, payload));
}

// ---------- timer de préparation géré côté serveur ----------
const prepTimers = new Map(); // roomId → { intervalId, remaining }

function startPrepTimer(roomId, io) {
  stopPrepTimer(roomId);
  let remaining = PREP_DURATION;

  const id = setInterval(async () => {
    remaining--;
    io.to(roomId).emit("timer:tick", { seconds: remaining });

    if (remaining === 10) {
      io.to(roomId).emit("timer:warning"); // front joue le TTS "dix secondes"
    }

    if (remaining <= 0) {
      stopPrepTimer(roomId);
      const room = await getRoom(roomId);
      if (!room) return;
      room.phase = PHASES.INTERROGATION_A;
      await saveRoom(room);
      broadcastState(roomId, room);
      io.to(roomId).emit("phase:interrogation_a");
    }
  }, 1000);

  prepTimers.set(roomId, { id, remaining });
}

function stopPrepTimer(roomId) {
  const t = prepTimers.get(roomId);
  if (t) {
    clearInterval(t.id);
    prepTimers.delete(roomId);
  }
}

// =============================================================
// SOCKET EVENTS
// =============================================================
io.on("connection", (socket) => {
  console.log(`[socket] connecté : ${socket.id}`);

  // ── REJOINDRE UNE ROOM ──────────────────────────────────────
  // Payload : { roomId, playerName, role: "A"|"B", team: 1|2, clientRole: "HOST"|"PLAYER_A"|... }
  socket.on("room:join", async ({ roomId, playerName, role, team, clientRole }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    // Enregistrer le joueur (ou mettre à jour son socketId en cas de reconnexion)
    const existing = room.players.find((p) => p.role === role && p.team === team);
    if (existing) {
      existing.socketId = socket.id;
      existing.name = playerName || existing.name;
    } else {
      room.players.push({
        id:       uuidv4(),
        name:     playerName || `Joueur ${role}`,
        role,
        team,
        clientRole: clientRole || ROLES.HOST,
        socketId: socket.id,
      });
    }

    await saveRoom(room);
    socket.join(roomId);
    socket.data.roomId = roomId;

    // Renvoyer le state complet au joueur qui vient de rejoindre
    socket.emit("state:sync", sanitizeForBroadcast(room));
    // Informer tous les clients du lobby (liste joueurs mise à jour)
    io.to(roomId).emit("lobby:update", {
      players: room.players.map(({ socketId: _, ...p }) => p),
      mode:    room.mode,
    });
    ack?.({ ok: true });
  });

  // ── DÉMARRER LA PARTIE ──────────────────────────────────────
  socket.on("game:start", async ({ roomId, mode }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    room.mode      = mode;
    room.phase     = PHASES.BRIEFING;
    room.activeTeam = 1;
    room.rounds    = [createRound(1)];
    if (mode === "4P") room.rounds.push(createRound(2));

    await saveRoom(room);
    broadcastState(roomId, room);

    // Générer le scénario pour l'équipe 1 en tâche de fond
    generateScenario(roomId, 1);
    ack?.({ ok: true });
  });

  // ── DÉMARRER LE TIMER DE PREP ───────────────────────────────
  socket.on("prep:start", async ({ roomId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    room.phase = PHASES.PREP_TIMER;
    await saveRoom(room);
    broadcastState(roomId, room);
    startPrepTimer(roomId, io);
    ack?.({ ok: true });
  });

  // Raccourci "on est prêts" — stoppe le timer et passe directement
  socket.on("prep:skip", async ({ roomId }, ack) => {
    stopPrepTimer(roomId);
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    room.phase = PHASES.INTERROGATION_A;
    await saveRoom(room);
    broadcastState(roomId, room);
    io.to(roomId).emit("phase:interrogation_a");
    ack?.({ ok: true });
  });

  // ── RÉPONSE D'UN JOUEUR ─────────────────────────────────────
  // Payload : { roomId, role, questionId, transcript }
  socket.on("answer:submit", async ({ roomId, role, questionId, transcript }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    const round = activeRound(room);
    if (!round) return ack?.({ error: "Pas de round actif" });

    round.answers[`${role}-${questionId}`] = transcript;
    await saveRoom(room);

    // Confirmer uniquement à l'émetteur (pas au partenaire !)
    socket.emit("answer:ack", { questionId, role });

    // Vérifier si toutes les réponses du joueur actif sont complètes
    const questions = round.scenario?.questions || [];
    const allAnswered = questions.every(
      (q) => round.answers[`${role}-${q.id}`] !== undefined
    );

    if (allAnswered) {
      if (role === "A") {
        room.phase = PHASES.ISOLATION_B;
        await saveRoom(room);
        broadcastState(roomId, room);
        io.to(roomId).emit("phase:isolation_b");
      } else {
        // B a fini → confrontation
        room.phase = PHASES.CONFRONTATION;
        await saveRoom(room);
        broadcastState(roomId, room);
        io.to(roomId).emit("phase:confrontation");
      }
    }

    ack?.({ ok: true });
  });

  // ── CONFIRMER ISOLATION B ───────────────────────────────────
  socket.on("isolation:confirm", async ({ roomId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    room.phase = PHASES.INTERROGATION_B;
    await saveRoom(room);
    broadcastState(roomId, room);
    io.to(roomId).emit("phase:interrogation_b");
    ack?.({ ok: true });
  });

  // ── DEMANDER LE JUGEMENT D'UNE QUESTION ─────────────────────
  // Payload : { roomId, questionId }
  socket.on("judge:request", async ({ roomId, questionId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    const round = activeRound(room);
    if (!round) return ack?.({ error: "Pas de round actif" });

    // Déjà jugé ?
    if (round.judgments[questionId]) {
      return ack?.({ ok: true, judgment: round.judgments[questionId] });
    }

    const question = round.scenario?.questions.find((q) => q.id === questionId);
    const answerA  = round.answers[`A-${questionId}`] || "(aucune réponse)";
    const answerB  = round.answers[`B-${questionId}`] || "(aucune réponse)";

    if (!question) return ack?.({ error: "Question introuvable" });

    // Signal "en cours" à tous
    io.to(roomId).emit("judge:thinking", { questionId });

    try {
      const judgment = await callClaude(
        JUDGE_SYSTEM,
        `QUESTION : "${question.text}"\nRÉPONSE Joueur A : "${answerA}"\nRÉPONSE Joueur B : "${answerB}"`
      );

      round.judgments[questionId] = judgment;
      await saveRoom(room);

      io.to(roomId).emit("judge:verdict", { questionId, judgment });
      ack?.({ ok: true, judgment });
    } catch (err) {
      const fallback = { isMatching: false, reason: "Le juge a perdu ses notes." };
      round.judgments[questionId] = fallback;
      await saveRoom(room);
      io.to(roomId).emit("judge:verdict", { questionId, judgment: fallback });
      ack?.({ ok: true, judgment: fallback });
    }
  });

  // ── FIN DE CONFRONTATION ────────────────────────────────────
  socket.on("confrontation:end", async ({ roomId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    if (room.mode === "4P" && room.activeTeam === 1) {
      // Transition vers équipe 2
      room.phase      = PHASES.BETWEEN_TEAMS;
      room.activeTeam = 2;
      await saveRoom(room);
      broadcastState(roomId, room);
    } else {
      room.phase = PHASES.VERDICT;
      await saveRoom(room);
      broadcastState(roomId, room);
    }

    ack?.({ ok: true });
  });

  // ── DÉMARRER LE TOUR DE L'ÉQUIPE 2 ─────────────────────────
  socket.on("team2:start", async ({ roomId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    room.phase = PHASES.BRIEFING;
    await saveRoom(room);
    broadcastState(roomId, room);

    // Générer un nouveau scénario pour l'équipe 2
    generateScenario(roomId, 2);
    ack?.({ ok: true });
  });

  // ── DEMANDER LE DISCOURS DE VERDICT ─────────────────────────
  socket.on("verdict:speech", async ({ roomId }, ack) => {
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    io.to(roomId).emit("verdict:speech_loading");

    const round1 = room.rounds.find((r) => r.teamId === 1);
    const round2 = room.rounds.find((r) => r.teamId === 2);

    const buildContras = (round) =>
      Object.entries(round?.judgments || {})
        .filter(([_, j]) => !j.isMatching)
        .map(([id, j]) => {
          const q = round?.scenario?.questions?.find((q) => String(q.id) === String(id));
          return `"${q?.text}" : ${j.reason}`;
        })
        .join("; ");

    const p1 = Object.values(round1?.judgments || {}).filter((j) => !j.isMatching).length;
    const p2 = round2
      ? Object.values(round2.judgments || {}).filter((j) => !j.isMatching).length
      : null;

    const isTie = room.mode === "4P" && p1 === p2;
    let speech, tiebreak = null;

    try {
      // Départage si égalité
      if (isTie) {
        tiebreak = await callClaude(
          TIEBREAK_SYSTEM,
          `Équipe 1 : ${room.players.filter((p) => p.team === 1).map((p) => p.name).join(" & ")}. Contradictions : ${buildContras(round1)}\n` +
          `Équipe 2 : ${room.players.filter((p) => p.team === 2).map((p) => p.name).join(" & ")}. Contradictions : ${buildContras(round2)}`
        );
        speech = tiebreak.verdict;
      }

      // Discours principal
      const ctx = room.mode === "4P"
        ? `Mode compétitif 2v2. Équipe 1 (${room.players.filter((p) => p.team === 1).map((p) => p.name).join(" & ")}) : ${p1} incohérences. Équipe 2 (${room.players.filter((p) => p.team === 2).map((p) => p.name).join(" & ")}) : ${p2} incohérences. Gagnants : Équipe ${isTie ? (tiebreak.loser === 1 ? 2 : 1) : p1 < p2 ? 1 : 2}. Contradictions équipe 1 : ${buildContras(round1)}. Contradictions équipe 2 : ${buildContras(round2)}.`
        : `Mode duo. Suspects : ${room.players.map((p) => p.name).join(" et ")}. Délit : ${round1?.scenario?.accusation}. Incohérences : ${p1}. ${buildContras(round1)}. Verdict : ${p1 < 3 ? "INNOCENTÉS" : "COUPABLES"}.`;

      const result = await callClaude(SPEECH_SYSTEM, ctx);
      speech = result.speech;
    } catch {
      speech = room.mode === "4P"
        ? "Après délibération, un vainqueur se dégage... difficilement."
        : p1 < 3 ? "Innocentés. Pour cette fois." : "COUPABLES. L'alibi était pathétique.";
    }

    io.to(roomId).emit("verdict:speech_ready", { speech, tiebreak });
    ack?.({ ok: true, speech, tiebreak });
  });

  // ── RESET ───────────────────────────────────────────────────
  socket.on("game:reset", async ({ roomId }, ack) => {
    stopPrepTimer(roomId);
    const room = await getRoom(roomId);
    if (!room) return ack?.({ error: "Room introuvable" });

    const fresh = createRoom(roomId, room.mode);
    // Garder les joueurs connectés mais réinitialiser le state
    fresh.players = room.players.map((p) => ({ ...p }));
    await saveRoom(fresh);
    broadcastState(roomId, fresh);
    ack?.({ ok: true });
  });

  // ── DÉCONNEXION ─────────────────────────────────────────────
  socket.on("disconnect", async () => {
    const roomId = socket.data.roomId;
    if (!roomId) return;

    const room = await getRoom(roomId);
    if (!room) return;

    // Marquer le joueur comme déconnecté (sans le supprimer — reconnexion possible)
    const player = room.players.find((p) => p.socketId === socket.id);
    if (player) {
      player.socketId = null;
      player.disconnectedAt = Date.now();
      await saveRoom(room);
      io.to(roomId).emit("player:disconnected", { name: player.name, role: player.role });
    }

    console.log(`[socket] déconnecté : ${socket.id}`);
  });
});

// =============================================================
// GÉNÉRATION DE SCÉNARIO (tâche de fond)
// =============================================================
async function generateScenario(roomId, teamId) {
  const theme = THEMES[Math.floor(Math.random() * THEMES.length)];

  io.to(roomId).emit("scenario:loading", { teamId });

  try {
    const scenario = await callClaude(
      GENERATOR_SYSTEM,
      `Génère un nouveau scénario. Thème suggéré : "${theme}"`
    );

    const room = await getRoom(roomId);
    if (!room) return;

    const round = room.rounds.find((r) => r.teamId === teamId);
    if (round) {
      round.scenario = scenario;
      await saveRoom(room);
      io.to(roomId).emit("scenario:ready", { teamId, scenario });
      broadcastState(roomId, room);
    }
  } catch (err) {
    io.to(roomId).emit("scenario:error", {
      teamId,
      message: "Connexion au Commissaire impossible.",
    });
    console.error("[generateScenario]", err.message);
  }
}

// =============================================================
// DÉMARRAGE
// =============================================================
try {
  await redis.connect();
} catch {
  console.warn("[Redis] Démarrage sans Redis — state en mémoire uniquement");
}

await app.listen({ port: PORT, host: "0.0.0.0" });
console.log(`
╔════════════════════════════════════════╗
║   L'ALIBI — Serveur démarré           ║
║   http://localhost:${PORT}               ║
╚════════════════════════════════════════╝
`);
