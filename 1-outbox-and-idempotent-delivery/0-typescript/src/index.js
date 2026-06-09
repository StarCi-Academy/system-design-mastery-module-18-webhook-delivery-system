"use strict";
// Emitter service: transactional outbox + skip-locked poller + retry sender.
// Plain Node + pg + ioredis. Implements the lesson's API contract exactly.
const http = require("http");
const { Pool } = require("pg");
const Redis = require("ioredis");

const PORT = Number(process.env.PORT || 3000);
const POSTGRES_URL = process.env.POSTGRES_URL || "postgres://app:app@postgres:5432/app";
const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 500);
const BATCH = Number(process.env.POLL_BATCH || 10);
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS || 6);
const IN_FLIGHT_TIMEOUT_MS = Number(process.env.IN_FLIGHT_TIMEOUT_MS || 5000);
// receiver-mock is internal-only (no published host port); the hook-proxy lets the
// host drive the idempotency-replay flow through the emitter's published port.
const RECEIVER_URL = process.env.RECEIVER_URL || "http://receiver-mock:3000/api/hook";
const PROXY_MAX_ATTEMPTS = Number(process.env.PROXY_MAX_ATTEMPTS || 12);

const pool = new Pool({ connectionString: POSTGRES_URL, max: 10 });
const redis = new Redis(REDIS_URL);

async function migrate() {
  await pool.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ref VARCHAR(128)
    )`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS outbox_event (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      type VARCHAR(128) NOT NULL,
      target_url VARCHAR(512) NOT NULL,
      payload JSONB NOT NULL,
      status VARCHAR(16) NOT NULL DEFAULT 'pending',
      attempts INT NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_event(status, created_at)`);
}

// REQ 1: business change + outbox row in ONE transaction.
async function publish(input) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const orderRes = await client.query(
      `INSERT INTO orders (ref) VALUES ($1) RETURNING id`,
      [input.payload && input.payload.orderId ? String(input.payload.orderId) : null]
    );
    const orderId = orderRes.rows[0].id;
    const evRes = await client.query(
      `INSERT INTO outbox_event (type, target_url, payload, status, attempts)
       VALUES ($1, $2, $3, 'pending', 0) RETURNING id, status`,
      [input.type, input.targetUrl, JSON.stringify({ ...input.payload, orderId })]
    );
    await client.query("COMMIT");
    return { eventId: evRes.rows[0].id, status: evRes.rows[0].status };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

async function getEvent(id) {
  const res = await pool.query(
    `SELECT id, status, attempts, last_error, created_at, updated_at FROM outbox_event WHERE id = $1`,
    [id]
  );
  if (res.rows.length === 0) return null;
  const r = res.rows[0];
  return {
    id: r.id,
    status: r.status,
    attempts: r.attempts,
    lastError: r.last_error,
    createdAt: r.created_at.toISOString(),
    updatedAt: r.updated_at.toISOString(),
  };
}

// REQ 2: claim pending rows with FOR UPDATE SKIP LOCKED, flip to in_flight in same txn.
async function claimPending(batch) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const res = await client.query(
      `SELECT id, type, target_url, payload, attempts FROM outbox_event
       WHERE status = 'pending' ORDER BY created_at ASC
       LIMIT $1 FOR UPDATE SKIP LOCKED`,
      [batch]
    );
    if (res.rows.length === 0) {
      await client.query("COMMIT");
      return [];
    }
    const ids = res.rows.map((r) => r.id);
    await client.query(
      `UPDATE outbox_event SET status = 'in_flight', updated_at = now() WHERE id = ANY($1)`,
      [ids]
    );
    await client.query("COMMIT");
    return res.rows;
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}

function postJson(url, body, headers) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const data = Buffer.from(JSON.stringify(body));
    const req = http.request(
      {
        method: "POST",
        hostname: u.hostname,
        port: u.port || 80,
        path: u.pathname,
        headers: { "Content-Type": "application/json", "Content-Length": data.length, ...headers },
      },
      (res) => {
        let buf = "";
        res.on("data", (c) => (buf += c));
        res.on("end", () => resolve({ status: res.statusCode, body: buf }));
      }
    );
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

// REQ 3: deliver, transition status, track attempts + lastError with backoff.
async function deliverOne(row) {
  try {
    const res = await postJson(
      row.target_url,
      typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload,
      { "Idempotency-Key": row.id }
    );
    if (res.status >= 200 && res.status < 300) {
      await pool.query(
        `UPDATE outbox_event SET status = 'sent', last_error = NULL, updated_at = now() WHERE id = $1`,
        [row.id]
      );
    } else {
      throw new Error(`receiver HTTP ${res.status}: ${res.body.slice(0, 200)}`);
    }
  } catch (err) {
    const nextStatus = row.attempts + 1 >= MAX_ATTEMPTS ? "failed" : "pending";
    await pool.query(
      `UPDATE outbox_event SET status = $2, attempts = attempts + 1, last_error = $3, updated_at = now() WHERE id = $1`,
      [row.id, nextStatus, String(err.message)]
    );
    // Exponential backoff: leave row pending; next eligible after a short delay.
    await new Promise((r) => setTimeout(r, Math.min(4000, 250 * 2 ** row.attempts)));
  }
}

async function pollOnce() {
  const rows = await claimPending(BATCH);
  await Promise.all(rows.map(deliverOne));
}

// Watchdog: reset rows stuck in_flight (emitter crashed mid-delivery) back to pending.
async function watchdog() {
  await pool.query(
    `UPDATE outbox_event SET status = 'pending', updated_at = now()
     WHERE status = 'in_flight' AND updated_at < now() - ($1::text || ' milliseconds')::interval`,
    [IN_FLIGHT_TIMEOUT_MS]
  );
}

function startLoops() {
  setInterval(() => pollOnce().catch((e) => console.error("poll error", e.message)), POLL_INTERVAL_MS);
  setInterval(() => watchdog().catch((e) => console.error("watchdog error", e.message)), 2000);
}

function readBody(req) {
  return new Promise((resolve) => {
    let buf = "";
    req.on("data", (c) => (buf += c));
    req.on("end", () => resolve(buf));
  });
}

function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(body);
}

// Forward a hook call to receiver-mock with the client's Idempotency-Key, retrying
// on a 5xx (the receiver fails the first N attempts) so the caller sees the first
// successful response and, on a replay, the same cached response byte-identical.
async function hookProxy(req, res) {
  const raw = await readBody(req);
  const idemKey = req.headers["idempotency-key"];
  let last = { status: 502, body: "{}" };
  for (let i = 0; i < PROXY_MAX_ATTEMPTS; i++) {
    try {
      last = await postJson(RECEIVER_URL, raw === "" ? {} : JSON.parse(raw), {
        "Idempotency-Key": idemKey || "",
      });
      if (last.status < 500) break;
    } catch (e) {
      last = { status: 502, body: JSON.stringify({ statusCode: 502, message: String(e.message) }) };
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  res.writeHead(last.status, { "Content-Type": "application/json" });
  res.end(last.body);
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.method === "POST" && req.url === "/api/events/publish") {
      const raw = await readBody(req);
      const input = JSON.parse(raw);
      const out = await publish(input);
      return send(res, 201, out);
    }
    if (req.method === "POST" && req.url === "/api/hook-proxy") {
      return hookProxy(req, res);
    }
    const m = req.url && req.url.match(/^\/api\/events\/([^/]+)$/);
    if (req.method === "GET" && m) {
      const ev = await getEvent(decodeURIComponent(m[1]));
      if (!ev) return send(res, 404, { statusCode: 404, message: `Event ${m[1]} not found` });
      return send(res, 200, ev);
    }
    if (req.method === "GET" && req.url === "/health") return send(res, 200, { ok: true });
    return send(res, 404, { statusCode: 404, message: "not found" });
  } catch (e) {
    return send(res, 500, { statusCode: 500, message: String(e.message) });
  }
});

(async () => {
  for (let i = 0; i < 30; i++) {
    try {
      await pool.query("SELECT 1");
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  await migrate();
  startLoops();
  server.listen(PORT, () => console.log(`emitter listening on ${PORT}`));
})();
