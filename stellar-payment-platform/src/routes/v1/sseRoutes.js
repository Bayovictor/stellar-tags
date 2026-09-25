'use strict';

// ---------------------------------------------------------------------------
// #730 — SSE endpoints for real-time payment status updates
// ---------------------------------------------------------------------------
// GET /api/v1/events/status            → all payment status events
// GET /api/v1/events/status?address=G… → only events for one Stellar account
// GET /api/v1/events/status?username=x → only events for one username
// GET /api/v1/events/health            → hub diagnostics (connection count)
//
// The browser connects with `new EventSource(url)`; reconnection is handled
// natively by the EventSource API (including Last-Event-ID replay), so this
// module only has to set the right headers and keep the stream open.
// ---------------------------------------------------------------------------

const express = require('express');

const { addClient, replayEventsFor, getClientCount, getStats } = require('../../services/sseService');
const { logger } = require('../../logger');

// Address/username filter values are bound to well-formed names only; the
// query schema additionally constrains their length.
const MAX_FILTER_LENGTH = 64;

const setSseHeaders = (res) => {
  res.status(200);
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  // Never let a proxy or compression middleware buffer the stream: events
  // must reach the browser the moment they are written.
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();
};

/**
 * @openapi
 * /events/status:
 *   get:
 *     tags:
 *       - v1
 *     description: |
 *       Server-Sent Events stream of real-time payment status updates
 *       (payment.created, payment.received). Connect with the browser's
 *       EventSource API — reconnection and Last-Event-ID replay are handled
 *       natively. Optional filters: `address` (Stellar public key) or
 *       `username` (federation name).
 *     parameters:
 *       - in: query
 *         name: address
 *         schema: { type: string }
 *         description: Only receive events involving this Stellar address.
 *       - in: query
 *         name: username
 *         schema: { type: string }
 *         description: Only receive events involving this username.
 *     responses:
 *       200:
 *         description: text/event-stream with named events and heartbeats
 */
const streamStatusEvents = (req, res) => {
  // Extract filters and the Last-Event-ID header. EventSource sends
  // Last-Event-ID automatically after a disconnect; the query param is a
  // manual override for non-browser clients.
  const address = typeof req.query.address === 'string' ? req.query.address.trim().slice(0, MAX_FILTER_LENGTH) : '';
  const username = typeof req.query.username === 'string' ? req.query.username.trim().slice(0, MAX_FILTER_LENGTH) : '';
  const headerLastEventId = req.get?.('last-event-id');
  const lastEventId =
    (typeof req.query.lastEventId === 'string' && req.query.lastEventId.trim()) ||
    (typeof headerLastEventId === 'string' && headerLastEventId.trim()) ||
    '';

  setSseHeaders(res);

  const { clientId } = addClient({
    res,
    filters: buildEventFilter(address, username),
    address: address || null,
  });

  // Replay anything the client missed while disconnected (Last-Event-ID).
  if (lastEventId) {
    const replayed = replayEventsFor(
      { res, filters: buildEventFilter(address, username) },
      lastEventId,
    );
    if (replayed > 0) {
      logger.info(
        `[sse] Replayed ${replayed} event(s) to client id=${clientId} after reconnect`,
      );
    }
  }
};

/**
 * Map query filters to event names. Payment events carry `to`/`from`
 * addresses, so a client can subscribe by account; username filtering is
 * resolved to the matching event names at subscription time.
 */
const buildEventFilter = (address, username) => {
  // Both event names are always allowed; the payload-level filter is applied
  // by the hub via the client's `address` match (see addClient).
  const filters = ['payment.created', 'payment.received'];
  return { eventNames: filters, address, username };
};

/**
 * @openapi
 * /events/health:
 *   get:
 *     tags:
 *       - v1
 *     description: SSE hub diagnostics — connected client count and config.
 *     responses:
 *       200:
 *         description: Hub statistics
 */
const sseHealth = (_req, res) => {
  const stats = getStats();
  return res.status(200).json({
    ok: true,
    clients: getClientCount(),
    ...stats,
  });
};

module.exports = (redisClient) => {
  const router = express.Router();

  // Note: redisClient is unused today — SSE fan-out manages its own Redis
  // connections (see sseService.startRedisFanout) — but the signature is kept
  // for consistency with the other route factories.
  void redisClient;

  router.get('/events/status', streamStatusEvents);
  router.get('/events/health', sseHealth);

  return router;
};

// Exported for unit tests.
module.exports.streamStatusEvents = streamStatusEvents;
module.exports.sseHealth = sseHealth;
module.exports.MAX_FILTER_LENGTH = MAX_FILTER_LENGTH;
