# Webhook signing and in-process retry — TypeScript (NestJS)

Two NestJS services sharing one HMAC secret and a Docker network:

- **emitter** (`:3000`): `POST /api/events/publish` signs `t.body` with HMAC-SHA256, retries in-process with exponential backoff + jitter, re-signs a fresh timestamp per attempt, and exposes `GET /api/events/:eventId`.
- **receiver-mock** (internal): verifies the ±300s timestamp window first, then the signature with `crypto.timingSafeEqual`. `?fail=<code>` forces a status code after a valid signature.

## Run

```bash
cd .docker
docker compose up -d --build
```

## Smoke test

```bash
# Flow 1 — happy delivery
EID=$(curl -s -X POST http://localhost:3000/api/events/publish -H "Content-Type: application/json" \
  -d '{"type":"order.created","payload":{"orderId":"o1"},"targetUrl":"http://receiver-mock:3000/api/hook"}' | jq -r .eventId)
sleep 2
curl -s http://localhost:3000/api/events/$EID

# Flow 2 — exhausted retries
EID=$(curl -s -X POST http://localhost:3000/api/events/publish -H "Content-Type: application/json" \
  -d '{"type":"order.fail","payload":{"orderId":"o2"},"targetUrl":"http://receiver-mock:3000/api/hook?fail=503"}' | jq -r .eventId)
sleep 15
curl -s http://localhost:3000/api/events/$EID
```

## Cleanup

```bash
docker compose down -v
```
