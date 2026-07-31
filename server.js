/**
 * AI 做法猜測與實時搶答系統 — 單一服務版
 */

const path = require("path");
const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { GoogleGenAI } = require("@google/genai");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  transports: ["websocket", "polling"],
  pingInterval: 10000,
  pingTimeout: 5000,
});

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || "" });
const HOST_SECRET = process.env.HOST_SECRET || "changeme";

const gameState = {
  phase: "idle",
  question: null,
  players: new Map(),
  currentAnswers: new Map(),
  votingStartedAt: null,
  votingDurationMs: 20000,
  correctAnswer: null,
};

function broadcastPlayerCount() {
  io.emit("player_count", { count: gameState.players.size });
}

let tallyThrottle = null;
function broadcastVoteTally() {
  if (tallyThrottle) return;
  tallyThrottle = setTimeout(() => {
    tallyThrottle = null;
    const tally = { A: 0, B: 0, C: 0 };
    for (const ans of gameState.currentAnswers.values()) tally[ans.choice]++;
    const total = gameState.currentAnswers.size || 1;
    io.emit("vote_tally", {
      counts: tally,
      percents: {
        A: Math.round((tally.A / total) * 100),
        B: Math.round((tally.B / total) * 100),
        C: Math.round((tally.C / total) * 100),
      },
      totalAnswered: gameState.currentAnswers.size,
    });
  }, 250);
}

io.on("connection", (socket) => {
  socket.isHost = false;

  socket.on("host:auth", (token, cb) => {
    socket.isHost = token === HOST_SECRET;
    if (socket.isHost) socket.join("host-room");
    cb?.({ ok: socket.isHost });
  });

  socket.on("player:join", ({ nickname }) => {
    gameState.players.set(socket.id, {
      nickname: (nickname || "").trim().slice(0, 12) || `玩家${socket.id.slice(0, 4)}`,
      score: 0,
    });
    socket.join("players");
    broadcastPlayerCount();
    socket.emit("player:joined", { phase: gameState.phase, question: gameState.question });
  });

  socket.on("host:setQuestion", (payload) => {
    if (!socket.isHost) return;
    gameState.question = payload;
    gameState.phase = "idle";
    gameState.currentAnswers.clear();
    gameState.correctAnswer = null;
    io.emit("question:new", gameState.question);
  });

  socket.on("host:openVoting", ({ durationMs = 20000 } = {}) => {
    if (!socket.isHost) return;
    gameState.phase = "voting";
    gameState.votingStartedAt = Date.now();
    gameState.votingDurationMs = durationMs;
    gameState.currentAnswers.clear();
    io.emit("voting:opened", { serverStartTime: gameState.votingStartedAt, durationMs });

    setTimeout(() => {
      if (gameState.phase === "voting") {
        gameState.phase = "locked";
        io.emit("voting:closed");
      }
    }, durationMs);
  });

  socket.on("player:lockAnswer", ({ choice }, cb) => {
    if (gameState.phase !== "voting") return cb?.({ ok: false, reason: "voting_closed" });
    if (gameState.currentAnswers.has(socket.id))
      return cb?.({ ok: false, reason: "already_locked" });

    const serverTimestamp = Date.now();
    const latencyMs = serverTimestamp - gameState.votingStartedAt;
    gameState.currentAnswers.set(socket.id, { choice, serverTimestamp, latencyMs });
    broadcastVoteTally();
    cb?.({ ok: true, latencyMs });
  });

  socket.on("host:closeVoting", () => {
    if (!socket.isHost) return;
    gameState.phase = "locked";
    io.emit("voting:closed");
  });

  socket.on("host:revealAI", async () => {
    if (!socket.isHost) return;
    gameState.phase = "revealing";
    io.emit("ai:revealStart");

    const q = gameState.question;
    const prompt = `你是一位活潑風趣的主持人 AI。請針對以下題目進行簡短、口語化的推理，
最後明確給出你認為最可能的答案（A、B 或 C），控制在 120 字以內：
題目：${q.text}
選項：A. ${q.options.A} / B. ${q.options.B} / C. ${q.options.C}`;

    try {
      const response = await ai.models.generateContentStream({
        model: "gemini-flash-latest",
        contents: prompt,
      });
      for await (const chunk of response) {
        const text = chunk.text;
        if (text) io.emit("ai:streamChunk", { text });
      }
      io.emit("ai:streamEnd");
    } catch (err) {
      console.error("Gemini stream error:", err);
      io.emit("ai:streamError", { message: "AI 生成失敗,請重試" });
    }
  });

  socket.on("host:scoreRound", ({ correctChoice }) => {
    if (!socket.isHost) return;
    gameState.correctAnswer = correctChoice;
    gameState.phase = "scored";

    const results = [...gameState.currentAnswers.entries()]
      .map(([socketId, ans]) => ({
        socketId,
        nickname: gameState.players.get(socketId)?.nickname || "匿名",
        correct: ans.choice === correctChoice,
        latencyMs: ans.latencyMs,
      }))
      .sort((a, b) => {
        if (a.correct !== b.correct) return a.correct ? -1 : 1;
        return a.latencyMs - b.latencyMs;
      });

    const speedBonus = [50, 40, 32, 25, 19, 14, 10, 7, 5, 3];
    results.forEach((r, idx) => {
      const roundScore = r.correct ? 100 + (speedBonus[idx] || 0) : 0;
      const player = gameState.players.get(r.socketId);
      if (player) player.score += roundScore;
      r.roundScore = roundScore;
      r.totalScore = player?.score || 0;
    });

    const leaderboard = [...gameState.players.entries()]
      .map(([id, p]) => ({ nickname: p.nickname, score: p.score }))
      .sort((a, b) => b.score - a.score)
      .slice(0, 10);

    io.emit("round:scored", { correctChoice, results, leaderboard });
  });

  socket.on("host:reset", () => {
    if (!socket.isHost) return;
    gameState.phase = "idle";
    gameState.question = null;
    gameState.currentAnswers.clear();
    gameState.correctAnswer = null;
    gameState.players.forEach((p) => (p.score = 0));
    io.emit("game:reset");
  });

  socket.on("disconnect", () => {
    gameState.players.delete(socket.id);
    gameState.currentAnswers.delete(socket.id);
    broadcastPlayerCount();
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
