'use strict';

/**
 * Route tests for src/routes/v1/sseRoutes.js (#730).
 *
 * SSE connections are intentionally never closed by the server, so the tests
 * read the raw stream for a fixed window and then destroy the socket,
 * asserting on whatever bytes arrived in that window.
 */

const http = require('http');
const request = require('supertest');
const express = require('express');

const sseRoutes = require('../src/routes/v1/sseRoutes');
const sseService = require('../src/services/sseService');
const { logger } = require('../src/logger');

jest.spyOn(logger, 'info').mockImplementation(() => {});
jest.spyOn(logger, 'warn').mockImplementation(() => {});

const buildApp = () => {
  const app = express();
  app.use(sseRoutes(null));
  return app;
};

/**
 * Open a stream against `app`, collect bytes for `windowMs`, then destroy the
 * socket and resolve with everything received. This avoids relying on server
 * initiated termination, which never happens for SSE.
 */
const readStreamFor = (app, path, { headers = {}, windowMs = 300 } = {}) =>
  new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      const req = http.request({ host: '127.0.0.1', port, path, headers }, (res) => {
        let body = '';
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('error', () => {
          /* expected when we destroy the socket mid-stream */
        });
        const timer = setTimeout(() => {
          res.destroy();
          req.destroy();
          server.close(() => resolve({ status: res.statusCode, headers: res.headers, body }));
        }, windowMs);
        timer.unref?.();
      });
      req.on('error', (err) => {
        server.close(() => reject(err));
      });
      req.end();
    });
  });

/** Poll until `predicate` is true or `timeoutMs` elapses. */
const waitFor = async (predicate, timeoutMs = 2000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await new Promise((r) => setTimeout(r, 25));
  }
  return predicate();
};

describe('GET /events/status', () => {
  beforeEach(() => {
    sseService.closeAllClients();
    sseService.clearReplayBuffer();
  });

  afterAll(() => {
    sseService.closeAllClients();
    sseService.clearReplayBuffer();
  });

  it('responds with SSE headers and a connected event', async () => {
    const { status, headers, body } = await readStreamFor(buildApp(), '/events/status');

    expect(status).toBe(200);
    expect(headers['content-type']).toBe('text/event-stream');
    expect(headers['cache-control']).toContain('no-cache');
    expect(body).toContain('event: connected');
    expect(body).toMatch(/id: [a-z0-9-]+/);
    expect(body.endsWith('\n\n') || body.includes('\n\n')).toBe(true);
  });

  it('registers the client with the hub while connected', async () => {
    const app = buildApp();
    const promise = readStreamFor(app, '/events/status', { windowMs: 250 });

    await waitFor(() => sseService.getClientCount() === 1);
    expect(sseService.getClientCount()).toBe(1);

    await promise;
  });

  it('delivers published payment events to connected clients', async () => {
    const app = buildApp();
    const promise = readStreamFor(app, '/events/status', { windowMs: 400 });

    // Wait until the route handler has registered the client with the hub.
    const registered = await waitFor(() => sseService.getClientCount() === 1);
    expect(registered).toBe(true);

    sseService.publishEvent('payment.received', { amount: '42', to: 'GABC' });

    const { body } = await promise;
    expect(body).toContain('event: payment.received');
    expect(body).toContain('"amount":"42"');
  });

  it('replays missed events for a reconnecting client via Last-Event-ID', async () => {
    const app = buildApp();

    // Seed the replay buffer before connecting.
    const first = sseService.publishEvent('payment.created', { seq: 1 });
    sseService.publishEvent('payment.created', { seq: 2 });

    const { body } = await readStreamFor(app, '/events/status', {
      headers: { 'Last-Event-ID': first.id },
      windowMs: 300,
    });

    expect(body).toContain('"seq":2');
    expect(body).not.toContain('"seq":1');
  });

  it('releases the hub client after the socket is destroyed', async () => {
    const app = buildApp();
    await readStreamFor(app, '/events/status', { windowMs: 150 });

    // Give the 'close' event a moment to propagate after destroy.
    const released = await waitFor(() => sseService.getClientCount() === 0);
    expect(released).toBe(true);
  });
});

describe('GET /events/health', () => {
  it('reports hub statistics', async () => {
    const res = await request(buildApp()).get('/events/health');

    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);
    expect(res.body).toHaveProperty('clients');
    expect(res.body).toHaveProperty('replayBufferSize');
    expect(res.body).toHaveProperty('redisEnabled');
    expect(res.body.config).toMatchObject({
      heartbeatMs: expect.any(Number),
      replayBuffer: expect.any(Number),
    });
  });
});
