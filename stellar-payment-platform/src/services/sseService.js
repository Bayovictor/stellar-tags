// ---------------------------------------------------------------------------
// #730 — Server-Sent Events (SSE) hub for real-time payment status updates
// ---------------------------------------------------------------------------
// A lightweight in-process pub/sub hub that Express SSE endpoints subscribe to
// and that publishers (payment intent creation, the Horizon listener) push
// status events onto.
//
// Why SSE instead of WebSockets:
//   • Payment status is a strictly one-way (server → client) data flow, so the
//     bidirectional overhead of the WS upgrade handshake buys nothing.
//   • EventSource re reconnects natively with the browser retry logic — no
//     client-side reconnection machinery to write or test.
//   • Plain HTTP keeps the existing middleware (CORS, compression, metrics)
//     and reverse-proxy (nginx) configuration working unchanged.
//
// Design notes:
//   • Each connection is a single Node.js timer (the heartbeat) and an event
//     listener — no per-connection socket upgrades, so resource consumption
//     stays far below a WS deployment with the same client count.
//   • A bounded replay buffer (SSE_REPLAY_BUFFER) lets a reconnected
//     EventSource resume where it left off via its Last-Event-ID header, so a
//     brief network blip does not lose updates. Events age out of the buffer
//     after SSE_EVENT_TTL_MS, so a client offline for hours starts fresh
//     instead of replaying stale history.
//   • When REDIS_URL is configured, events are also published to Redis
//     pub/sub and SSE events from other API instances are re-emitted locally,
//     so the stream works behind a load balancer with any node count.
//   • Backpressure: if a client's socket buffers more than
//     SSE_MAX_BUFFERED_BYTES the connection is closed; the browser's native
//     reconnect-with-Last-Event-ID flow recovers it without server memory
//     growth.
// ---------------------------------------------------------------------------

const crypto = require('crypto');
const { logger } = require('../logger');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const SSE_HEARTBEAT_MS = parseInt(process.env.SSE_HEARTBEAT_MS, 10) || 25_000;
const SSE_REPLAY_BUFFER = parseInt(process.env.SSE_REPLAY_BUFFER, 10) || 100;
const SSE_EVENT_TTL_MS = parseInt(process.env.SSE_EVENT_TTL_MS, 10) || 5 * 60_000;
const SSE_MAX_BUFFERED_BYTES =
  parseInt(process.env.SSE_MAX_BUFFERED_BYTES, 10) || 1024 * 1024;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

// All connected clients. Keyed by connection id so removal is O(1).
const clients = new Map();

// Bounded ring of recent events for Last-Event-ID replay on reconnect.
const replayBuffer = [];

// Optional Redis pub/sub fan-out (enabled when REDIS_URL is set).
let redisPublisher = null;
let redisSubscriber = null;
const REDIS_CHANNEL = 'stellar-tags:sse-events';

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/** Integer id generator — compact, monotonic, collision-free per process. */
const nextClientId = (() => {
  let current = 0;
  return () => ++current;
})();

const newEventId = () =>
  `${Date.now().toString(36)}-${crypto.randomBytes(4).toString('hex')}`;

const nowIso = () => new Date().toISOString();

// Monotonic ordering for the replay buffer. Date.now() has millisecond
// resolution and several events can share one millisecond, so replay
// "everything after event X" must compare sequence numbers, not timestamps.
const nextSeq = (() => {
  let current = 0;
  return () => ++current;
})();

/**
 * Does an event pass a client's filter set?
 * An empty/missing filter list means "subscribe to everything".
 */
const eventMatchesFilters = (eventName, filters) => {
  if (!Array.isArray(filters) || filters.length === 0) return true;
  return filters.includes(eventName);
};

/**
 * Serialize one event using the SSE wire format:
 *   id: <id>\n
 *   event: <name>\n
 *   data: <json>\n\n
 */
const serializeEvent = (id, event, payload) =>
  `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;

/**
 * Trim the replay buffer: drop entries older than the TTL and evict the
 * oldest entries beyond the configured size.
 */
const trimReplayBuffer = () => {
  const cutoff = Date.now() - SSE_EVENT_TTL_MS;
  while (replayBuffer.length && replayBuffer[0].ts < cutoff) {
    replayBuffer.shift();
  }
  while (replayBuffer.length > SSE_REPLAY_BUFFER) {
    replayBuffer.shift();
  }
};

// ---------------------------------------------------------------------------
// Client write helpers
// ---------------------------------------------------------------------------

const tryWrite = (client, chunk) => {
  if (client.res.writableEnded || client.res.destroyed) return false;

  // Backpressure guard: buffered bytes are what the socket has accepted but
  // the OS has not sent yet. Past the limit, the client is effectively gone
  // (stalled tab, dead network) — drop it and let EventSource reconnect.
  const buffered =
    typeof client.res.flushHeaders === 'function' && client.res.socket
      ? client.res.socket.writableLength
      : 0;
  if (buffered > SSE_MAX_BUFFERED_BYTES) {
    logger.warn(
      `[sse] Closing slow client id=${client.id} user=${client.address || 'anonymous'} buffered=${buffered}B`,
    );
    removeClient(client.id, 'backpressure');
    return false;
  }

  try {
    client.res.write(chunk);
    return true;
  } catch (err) {
    logger.warn(`[sse] Write failed for client id=${client.id}: ${err.message}`);
    removeClient(client.id, 'write-error');
    return false;
  }
};

const sendHeartbeat = (client) => {
  // A comment line keeps intermediaries from closing an idle connection and
  // doubles as an liveness probe: a broken socket surfaces as a write error.
  client.lastSeenAt = Date.now();
  return tryWrite(client, `: heartbeat ${nowIso()}\n\n`);
};

// ---------------------------------------------------------------------------
// Core hub API
// ---------------------------------------------------------------------------

/**
 * Register a new SSE connection. The response is expected to have SSE headers
 * set already (see routes/v1/sseRoutes.js). Returns the client record and an
 * unsubscribe function.
 */
const addClient = ({ res, filters = [], address = null }) => {
  const client = {
    id: nextClientId(),
    res,
    filters: Array.isArray(filters) ? filters : [],
    address: address || null,
    connectedAt: Date.now(),
    lastSeenAt: Date.now(),
    eventsSent: 0,
  };

  clients.set(client.id, client);

  // Announce the connection so the client can display "connected" state.
  tryWrite(
    client,
    serializeEvent(newEventId(), 'connected', {
      message: 'SSE stream connected',
      timestamp: nowIso(),
      retry: SSE_HEARTBEAT_MS,
      filters: client.filters,
    }),
  );

  client.heartbeatTimer = setInterval(() => sendHeartbeat(client), SSE_HEARTBEAT_MS);
  // Do not hold the process open for heartbeats during graceful shutdown.
  client.heartbeatTimer.unref?.();

  // The request's 'close' event fires on client disconnect, navigation away,
  // and EventSource.close() — one listener per connection is all cleanup needs.
  res.on('close', () => removeClient(client.id, 'client-disconnect'));

  logger.info(
    `[sse] Client connected id=${client.id} user=${client.address || 'anonymous'} filters=[${client.filters.join(',') || 'all'}] total=${clients.size}`,
  );

  return {
    clientId: client.id,
    unsubscribe: () => removeClient(client.id, 'unsubscribed'),
  };
};

/** Remove a client and release its resources. Safe to call twice. */
const removeClient = (clientId, reason = 'unknown') => {
  const client = clients.get(clientId);
  if (!client) return false;

  if (client.heartbeatTimer) clearInterval(client.heartbeatTimer);
  clients.delete(clientId);

  // End the response unless the socket is already gone.
  try {
    if (!client.res.writableEnded) client.res.end();
  } catch {
    // Socket already destroyed — nothing to end.
  }

  logger.info(
    `[sse] Client disconnected id=${clientId} reason=${reason} total=${clients.size}`,
  );
  return true;
};

/**
 * Publish an event to every connected client whose filters match.
 * When Redis is configured the event is also fanned out to other processes;
 * the `source` flag prevents re-delivery loops.
 *
 * @param {string} event - event name (e.g. 'payment.created')
 * @param {object} payload - JSON-serializable event body
 * @returns {{ id: string, delivered: number }} assigned event id and local delivery count
 */
const publishEvent = (event, payload) => {
  const id = newEventId();
  const chunk = serializeEvent(id, event, payload);
  const ts = Date.now();

  replayBuffer.push({ id, event, payload, chunk, seq: nextSeq(), ts });
  trimReplayBuffer();

  let delivered = 0;
  for (const client of clients.values()) {
    if (!eventMatchesFilters(event, client.filters)) continue;
    if (tryWrite(client, chunk)) {
      client.eventsSent += 1;
      client.lastSeenAt = ts;
      delivered += 1;
    }
  }

  if (redisPublisher && redisPublisher.isReady) {
    redisPublisher
      .publish(REDIS_CHANNEL, JSON.stringify({ event, payload }))
      .catch((err) =>
        logger.warn(`[sse] Redis publish failed: ${err.message}`),
      );
  }

  return { id, delivered };
};

// ---------------------------------------------------------------------------
// Reconnect replay (Last-Event-ID)
// ---------------------------------------------------------------------------

/**
 * Re-deliver buffered events that a reconnecting client missed.
 * `lastEventId` is the id the client last saw (EventSource sends it
 * automatically as the Last-Event-ID header after a disconnect).
 */
const replayEventsFor = (client, lastEventId) => {
  if (!lastEventId) return 0;

  const lastSeq = replayBuffer.find((e) => e.id === lastEventId)?.seq;
  const pending = lastSeq !== undefined
    ? replayBuffer.filter((e) => e.seq > lastSeq)
    : // The client's id has aged out of the buffer — send everything we have
      // that matches, which is still fresher than a silent stream.
      replayBuffer.slice();

  let sent = 0;
  for (const entry of pending) {
    if (!eventMatchesFilters(entry.event, client.filters)) continue;
    if (tryWrite(client, entry.chunk)) sent += 1;
  }
  return sent;
};

// ---------------------------------------------------------------------------
// Redis fan-out (multi-instance deployments)
// ---------------------------------------------------------------------------

/**
 * Enable cross-process fan-out. Uses the same Redis instance as the rest of
 * the platform via REDIS_URL. Failures never break the local stream — Redis
 * only extends reach, the in-process hub works standalone.
 */
const startRedisFanout = async () => {
  if (!process.env.REDIS_URL) return false;
  if (redisPublisher && redisSubscriber) return true;

  try {
    const { createRedisConnection } = require('../config/redis');
    redisPublisher = createRedisConnection();

    // Subscribers need a dedicated connection in subscriber mode.
    redisSubscriber = redisPublisher.duplicate();
    await redisSubscriber.subscribe(REDIS_CHANNEL, (message) => {
      try {
        const { event, payload } = JSON.parse(message);
        // Deliver to local clients only — never republish to Redis.
        deliverLocally(event, payload);
      } catch (err) {
        logger.warn(`[sse] Failed to process Redis message: ${err.message}`);
      }
    });

    redisPublisher.on('error', (err) =>
      logger.warn(`[sse] Redis publisher error: ${err.message}`),
    );
    redisSubscriber.on('error', (err) =>
      logger.warn(`[sse] Redis subscriber error: ${err.message}`),
    );

    logger.info('[sse] Redis fan-out enabled (multi-instance streaming)');
    return true;
  } catch (err) {
    logger.warn(`[sse] Redis fan-out unavailable, local-only streaming: ${err.message}`);
    redisPublisher = null;
    redisSubscriber = null;
    return false;
  }
};

/** Deliver to local clients without touching Redis (used by the subscriber). */
const deliverLocally = (event, payload) => {
  const chunk = serializeEvent(newEventId(), event, payload);
  let delivered = 0;
  for (const client of clients.values()) {
    if (!eventMatchesFilters(event, client.filters)) continue;
    if (tryWrite(client, chunk)) {
      client.eventsSent += 1;
      delivered += 1;
    }
  }
  return delivered;
};

// ---------------------------------------------------------------------------
// Lifecycle / introspection
// ---------------------------------------------------------------------------

/** Close every client connection (graceful shutdown). */
const closeAllClients = () => {
  const ids = [...clients.keys()];
  for (const id of ids) removeClient(id, 'server-shutdown');
  return ids.length;
};

/** Drop buffered replay events (used by tests and admin resets). */
const clearReplayBuffer = () => {
  replayBuffer.length = 0;
};

/** Tear down Redis connections (graceful shutdown / tests). */
const stopRedisFanout = async () => {
  const connections = [redisPublisher, redisSubscriber].filter(Boolean);
  redisPublisher = null;
  redisSubscriber = null;
  await Promise.all(
    connections.map(async (conn) => {
      try {
        await conn.quit();
      } catch {
        try {
          conn.disconnect();
        } catch {
          // Already gone.
        }
      }
    }),
  );
};

/** Current connection count, for metrics and health reporting. */
const getClientCount = () => clients.size;

/** Snapshot of hub internals, used by tests and admin diagnostics. */
const getStats = () => ({
  clients: clients.size,
  replayBufferSize: replayBuffer.length,
  redisEnabled: Boolean(redisPublisher && redisSubscriber),
  config: {
    heartbeatMs: SSE_HEARTBEAT_MS,
    replayBuffer: SSE_REPLAY_BUFFER,
    eventTtlMs: SSE_EVENT_TTL_MS,
    maxBufferedBytes: SSE_MAX_BUFFERED_BYTES,
  },
});

module.exports = {
  addClient,
  removeClient,
  publishEvent,
  deliverLocally,
  replayEventsFor,
  startRedisFanout,
  closeAllClients,
  clearReplayBuffer,
  stopRedisFanout,
  getClientCount,
  getStats,
  // Exported for tests and metrics wiring.
  REDIS_CHANNEL,
  SSE_HEARTBEAT_MS,
  SSE_REPLAY_BUFFER,
  SSE_EVENT_TTL_MS,
  SSE_MAX_BUFFERED_BYTES,
};
