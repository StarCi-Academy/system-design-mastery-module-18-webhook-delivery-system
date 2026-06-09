"use strict";
// receiver-mock: idempotent webhook receiver.
// - Fails the first FAIL_FIRST_N_ATTEMPTS attempts per Idempotency-Key (to demo retry).
// - Caches the successful result with SET … NX EX and replays it byte-identical.
const http = require("http");
const crypto = require("crypto");
const Redis = require("ioredis");

const PORT = Number(process.env.PORT || 3000);
const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const FAIL_FIRST_N_ATTEMPTS = Number(process.env.FAIL_FIRST_N_ATTEMPTS || 0);
const TTL_SECONDS = Number(process.env.IDEM_TTL_SECONDS || 86400);

const redis = new Redis(REDIS_URL);

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function readBody(req) {
  return new Promise((resolve) => {
    let buf = "";
    req.on("data", (c) => (buf += c));
    req.on("end", () => resolve(buf));
  });
}

function send(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/health") return send(res, 200, { ok: true });
  if (!(req.method === "POST" && req.url === "/api/hook")) {
    return send(res, 404, { statusCode: 404, message: "not found" });
  }
  const rawBody = await readBody(req);
  const idemKey = req.headers["idempotency-key"];
  if (!idemKey) return send(res, 400, { statusCode: 400, message: "missing Idempotency-Key" });

  const bodyHash = sha256(rawBody);
  const cacheKey = `hook:idem:${idemKey}:${bodyHash}`;
  // Key-reuse-different-body detection: a stored hash for this key that differs → 409.
  const seenHashKey = `hook:idem:hash:${idemKey}`;

  const cached = await redis.get(cacheKey);
  if (cached) {
    // Replay: return the first response verbatim. Side-effect does NOT run again.
    res.writeHead(200, { "Content-Type": "application/json", "Idempotent-Replayed": "true" });
    return res.end(cached);
  }

  const priorHash = await redis.get(seenHashKey);
  if (priorHash && priorHash !== bodyHash) {
    return send(res, 409, { statusCode: 409, message: "Idempotency-Key reused with a different body" });
  }

  // Demo retry: count attempts per key; fail the first N.
  const attemptKey = `hook:attempt:${idemKey}`;
  const attempt = await redis.incr(attemptKey);
  await redis.expire(attemptKey, TTL_SECONDS);
  if (attempt <= FAIL_FIRST_N_ATTEMPTS) {
    return send(res, 503, { statusCode: 503, message: `simulated failure ${attempt}/${FAIL_FIRST_N_ATTEMPTS}` });
  }

  // Cache miss → run side-effect exactly once, capture result.
  let bodyEcho;
  try {
    bodyEcho = JSON.parse(rawBody);
  } catch {
    bodyEcho = rawBody;
  }
  const result = {
    received: true,
    eventId: bodyEcho && bodyEcho.id ? bodyEcho.id : idemKey,
    idempotencyKey: idemKey,
    sideEffectExecutedAt: new Date().toISOString(),
    bodyEcho,
  };
  const payload = JSON.stringify(result);
  // SET … NX EX: only the winner writes; losers read the winner's value.
  const setRes = await redis.set(cacheKey, payload, "EX", TTL_SECONDS, "NX");
  await redis.set(seenHashKey, bodyHash, "EX", TTL_SECONDS);
  if (setRes === null) {
    const winner = await redis.get(cacheKey);
    res.writeHead(200, { "Content-Type": "application/json", "Idempotent-Replayed": "true" });
    return res.end(winner || payload);
  }
  return send(res, 200, result);
});

server.listen(PORT, () => console.log(`receiver-mock listening on ${PORT}, FAIL_FIRST_N_ATTEMPTS=${FAIL_FIRST_N_ATTEMPTS}`));
