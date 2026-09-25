const client = require('prom-client');
const { getPoolMetrics } = require('./db-pool-monitor');

// Collect default Node.js metrics (memory, CPU, event loop, etc.)
const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ prefix: 'stellar_tags_' });

// Custom counter: total HTTP requests by method, route, status
const httpRequestCounter = new client.Counter({
  name: 'stellar_tags_http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code'],
});

// Custom histogram: request duration in seconds, bucketed for SLO work
// (10ms, 50ms, 100ms, 500ms, 1s, 5s) so p50/p95/p99 latency can be derived.
const httpRequestDuration = new client.Histogram({
  name: 'stellar_tags_http_request_duration_seconds',
  help: 'HTTP request duration in seconds',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5],
});

// The Prisma and Redis clients are created after this module is imported, so
// they are registered later via setMetricsSources and read at scrape time.
let prismaSource = null;
let redisSource = null;

/**
 * Registers the live clients the connection gauges report on. Passing a null
 * client (e.g. Redis with no REDIS_URL) leaves its gauge reporting zero.
 */
function setMetricsSources({ prisma, redisClient } = {}) {
  if (prisma !== undefined) prismaSource = prisma;
  if (redisClient !== undefined) redisSource = redisClient;
}

const EMPTY_POOL = { active: 0, idle: 0, size: 0, waiters: 0 };

// Every pool gauge needs the same snapshot, and prom-client collects them
// concurrently within one scrape. Sharing the in-flight promise keeps that to a
// single $metrics.json() call per scrape instead of one per gauge.
let inFlightPoolRead = null;

function readPoolMetrics() {
  if (!prismaSource) {
    return Promise.resolve(EMPTY_POOL);
  }

  if (!inFlightPoolRead) {
    inFlightPoolRead = getPoolMetrics(prismaSource)
      .then((metrics) => metrics || EMPTY_POOL)
      .finally(() => {
        inFlightPoolRead = null;
      });
  }

  return inFlightPoolRead;
}

// Gauge: database connections currently open in the Prisma pool
const dbPoolConnectionsOpen = new client.Gauge({
  name: 'stellar_tags_db_pool_connections_open',
  help: 'Database connections currently open in the Prisma pool',
  async collect() {
    this.set((await readPoolMetrics()).size);
  },
});

// Gauge: open connections currently executing a query
const dbPoolConnectionsBusy = new client.Gauge({
  name: 'stellar_tags_db_pool_connections_busy',
  help: 'Database connections currently executing a query',
  async collect() {
    this.set((await readPoolMetrics()).active);
  },
});

// Gauge: open connections not currently in use
const dbPoolConnectionsIdle = new client.Gauge({
  name: 'stellar_tags_db_pool_connections_idle',
  help: 'Database connections open but not currently in use',
  async collect() {
    this.set((await readPoolMetrics()).idle);
  },
});

// Gauge: queries queued because every pooled connection is busy
const dbPoolQueriesWaiting = new client.Gauge({
  name: 'stellar_tags_db_pool_queries_waiting',
  help: 'Queries queued waiting for a free database connection',
  async collect() {
    this.set((await readPoolMetrics()).waiters);
  },
});

// Gauge: the shared Redis client holds a single connection, so this reports 1
// while that connection is ready for commands and 0 otherwise.
const redisConnectionsActive = new client.Gauge({
  name: 'stellar_tags_redis_connections_active',
  help: 'Redis connections ready to accept commands (0 when unconfigured or disconnected)',
  collect() {
    this.set(redisSource?.isReady ? 1 : 0);
  },
});

// #730 — Gauge: browsers currently holding an open SSE status stream. Spikes
// alongside rising RSS would indicate clients are not being released on
// disconnect; a healthy deployment tracks concurrent dashboard users.
let sseClientCountFn = null;

function setSseClientSource(fn) {
  sseClientCountFn = typeof fn === 'function' ? fn : null;
}

const sseClientsConnected = new client.Gauge({
  name: 'stellar_tags_sse_clients_connected',
  help: 'Number of currently connected SSE (Server-Sent Events) clients',
  collect() {
    this.set(sseClientCountFn ? sseClientCountFn() : 0);
  },
});

/**
 * Express middleware that tracks request count and latency.
 * Attach to app BEFORE route handlers.
 */
function metricsMiddleware(req, res, next) {
  const start = Date.now();

  res.on('finish', () => {
    const duration = (Date.now() - start) / 1000;
    // Normalize the route label: use the matched route pattern (never the raw
    // path, query string, or concrete ids), include the router mount prefix
    // (e.g. "/api/v1"), and collapse unmatched/404 paths to "unknown" so label
    // cardinality stays bounded even under arbitrary-path probing.
    const route = req.route
      ? `${req.baseUrl || ''}${req.route.path}`
      : 'unknown';
    const labels = {
      method: req.method,
      route,
      status_code: res.statusCode.toString(),
    };

    httpRequestCounter.inc(labels);
    httpRequestDuration.observe(labels, duration);
  });

  next();
}

/**
 * Returns Prometheus-format metrics as a string.
 */
async function getMetrics() {
  return client.register.metrics();
}

/**
 * Returns the content type for Prometheus scraping.
 */
function getContentType() {
  return client.register.contentType;
}

module.exports = {
  metricsMiddleware,
  getMetrics,
  getContentType,
  setMetricsSources,
  setSseClientSource,
  dbPoolConnectionsOpen,
  dbPoolConnectionsBusy,
  dbPoolConnectionsIdle,
  dbPoolQueriesWaiting,
  redisConnectionsActive,
  sseClientsConnected,
};
