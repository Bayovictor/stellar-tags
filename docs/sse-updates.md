# #730 — Server-Sent Events (SSE) for Real-Time Payment Status Updates

Real-time payment status now streams to clients over **Server-Sent Events (SSE)**
instead of a bidirectional WebSocket connection. Payment status is strictly
one-way (server → client), so SSE gives the same live experience with plain
HTTP, no extra protocol upgrade, and **native browser reconnection** via the
`EventSource` API.

## Endpoints

| Endpoint | Description |
| --- | --- |
| `GET /api/v1/events/status` | SSE stream of payment status events |
| `GET /api/v1/events/status?address=G…` | Only events involving one Stellar address |
| `GET /api/v1/events/status?username=x` | Only events involving one username |
| `GET /api/v1/events/health` | Hub diagnostics (connected clients, buffer size, config) |

The stream emits `text/event-stream` with named events:

- `connected` — sent immediately on connect (includes the suggested retry interval)
- `payment.created` — a payment intent was registered (status `pending`)
- `payment.received` — a payment was detected on-chain by the Horizon listener

Each event carries an `id:`; a reconnecting `EventSource` automatically sends it
back as `Last-Event-ID` and the server replays anything the client missed
(bounded by a replay buffer, see configuration below).

## Client usage (browser)

```js
const es = new EventSource('https://api.example.com/api/v1/events/status?address=GABC…');

es.addEventListener('payment.received', (e) => {
  const payment = JSON.parse(e.data);
  console.log('payment received', payment);
});

// Reconnection is native: the browser retries automatically, including after
// server restarts and network blips, and replays missed events via
// Last-Event-ID. To stop: es.close();
```

A ready-made React hook is included in the dashboard:
`payment-dashboard/src/usePaymentEvents.js`

```js
const { status, lastEvent } = usePaymentEvents({ address: userPublicKey });
// status: 'idle' | 'connecting' | 'open' | 'error' | 'closed'
```

## Non-browser clients

Any HTTP client works. To resume after a reconnect, send the last seen event id:

```
GET /api/v1/events/status
Last-Event-ID: <last event id>
```

(or pass `?lastEventId=<id>`).

## Server architecture

```
publishEvent('payment.received', …)
        │
        ▼
┌──────────────────────────────┐
│ src/services/sseService.js   │  in-process hub
│  • per-client filters        │
│  • heartbeat every 25s       │
│  • bounded replay buffer     │
│  • backpressure guard        │
│  • Redis pub/sub fan-out     │──▶ other API instances
└──────────────────────────────┘
        │  writes
        ▼
GET /events/status (Express route)
        │
        ▼
Browser EventSource (native reconnection)
```

Key properties:

- **One timer + one listener per connection** — no socket upgrades, so memory
  per client is far below an equivalent WebSocket deployment.
- **Heartbeat comments** (`: heartbeat`) every 25 s keep proxies from reaping
  idle streams and surface dead sockets as write errors.
- **Backpressure**: if a client's socket buffers more than
  `SSE_MAX_BUFFERED_BYTES` the server drops it; the browser reconnects and
  resumes via `Last-Event-ID`, so no server-side unbounded queues.
- **Multi-instance**: when `REDIS_URL` is set, events are published to Redis
  pub/sub and re-emitted on every instance, so any node can serve any client.
  Without Redis the hub still works single-instance.

## Publishing events (server-side)

```js
const { publishEvent } = require('./src/services/sseService');

publishEvent('payment.created', { payment_id: '…', status: 'pending', … });
```

Currently wired:

- `src/routes/v1/paymentRoutes.js` — emits `payment.created` after a bulk
  payment-intent registration.
- `horizonListener.js` — emits `payment.received` for each on-chain payment
  detected on a watched account.

## Configuration (environment variables)

| Variable | Default | Purpose |
| --- | --- | --- |
| `SSE_HEARTBEAT_MS` | `25000` | Heartbeat comment interval |
| `SSE_REPLAY_BUFFER` | `100` | Events kept for `Last-Event-ID` replay (0 disables) |
| `SSE_EVENT_TTL_MS` | `300000` | How long an event stays eligible for replay |
| `SSE_MAX_BUFFERED_BYTES` | `1048576` | Per-connection socket buffer cap before dropping a slow client |

## Reverse proxy (nginx)

`nginx.conf` has a dedicated `location /api/events/` block with
`proxy_buffering off`, `proxy_read_timeout 1h`, and `gzip off` — buffering or
compressing an SSE stream delays every event until a buffer fills, defeating
real-time delivery. Behind other proxies, the server also sends
`X-Accel-Buffering: no`.

## Metrics

Prometheus gauge `stellar_tags_sse_clients_connected` reports the number of
live SSE connections (exposed on `/metrics`).

## Resource comparison vs WebSockets

| | SSE (this change) | WebSocket |
| --- | --- | --- |
| Handshake | Plain HTTP GET | HTTP `Upgrade` + frame protocol |
| Per-connection state | 1 timer + 1 listener | Socket + frame codec + ping/pong state machine |
| Reconnect logic | Native in `EventSource` | Hand-rolled by every client |
| Proxy compatibility | Standard HTTP | Often needs explicit config |
| Direction | Server → client (matches need) | Bidirectional (unused here) |

## Tests

```bash
cd stellar-payment-platform
npx jest tests/sse-service.test.js tests/sse-routes.test.js
```

The suites cover the SSE wire format, per-client filtering, heartbeats,
`Last-Event-ID` replay (including aged-out ids), backpressure disconnects,
write-error resilience, route headers, hub registration/release on disconnect,
and the health endpoint.
