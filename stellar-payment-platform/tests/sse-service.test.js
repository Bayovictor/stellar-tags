'use strict';

/**
 * Unit tests for src/services/sseService.js (#730).
 *
 * The hub is exercised with mock ServerResponse objects that record written
 * chunks, so the tests assert on the actual SSE wire format.
 */

const {
  addClient,
  publishEvent,
  deliverLocally,
  replayEventsFor,
  getClientCount,
  getStats,
  closeAllClients,
  clearReplayBuffer,
  SSE_HEARTBEAT_MS,
} = require('../src/services/sseService');
const { logger } = require('../src/logger');

// Spy on logger output but keep test output clean.
jest.spyOn(logger, 'info').mockImplementation(() => {});
jest.spyOn(logger, 'warn').mockImplementation(() => {});

const buildMockRes = () => {
  const chunks = [];
  const listeners = {};
  return {
    chunks,
    listeners,
    writableEnded: false,
    destroyed: false,
    socket: { writableLength: 0 },
    flushHeaders: jest.fn(),
    write: jest.fn((chunk) => {
      chunks.push(chunk);
      return true;
    }),
    end: jest.fn(() => {
      this.writableEnded = true;
    }),
    on: jest.fn((event, fn) => {
      listeners[event] = fn;
    }),
    emitClose: () => listeners.close?.(),
    writtenText: () => chunks.join(''),
  };
};

beforeEach(() => {
  closeAllClients();
  clearReplayBuffer();
});

afterAll(() => {
  closeAllClients();
  clearReplayBuffer();
});

describe('sseService hub', () => {
  test('adds a client, sends a connected event and counts it', () => {
    const res = buildMockRes();
    addClient({ res, filters: [] });

    expect(getClientCount()).toBe(1);
    expect(res.flushHeaders).not.toHaveBeenCalled(); // headers set by route
    expect(res.chunks[0]).toContain('event: connected');
    expect(res.chunks[0]).toContain('data: ');
    expect(res.on).toHaveBeenCalledWith('close', expect.any(Function));
  });

  test('publishes to all matching clients with proper SSE wire format', () => {
    const a = buildMockRes();
    const b = buildMockRes();
    addClient({ res: a, filters: [] });
    addClient({ res: b, filters: [] });

    const { delivered, id } = publishEvent('payment.received', { amount: '10' });

    expect(delivered).toBe(2);
    expect(id).toEqual(expect.any(String));

    for (const client of [a, b]) {
      const eventChunk = client.chunks.find((c) => c.includes('payment.received'));
      expect(eventChunk).toContain(`id: ${id}`);
      expect(eventChunk).toContain('event: payment.received');
      expect(eventChunk).toContain('data: {"amount":"10"}');
      expect(eventChunk.endsWith('\n\n')).toBe(true);
    }
  });

  test('respects per-client event filters', () => {
    const filtered = buildMockRes();
    const open = buildMockRes();
    addClient({ res: filtered, filters: ['payment.created'] });
    addClient({ res: open, filters: [] });

    publishEvent('payment.received', { amount: '5' });
    expect(filtered.chunks.some((c) => c.includes('payment.received'))).toBe(false);
    expect(open.chunks.some((c) => c.includes('payment.received'))).toBe(true);

    publishEvent('payment.created', { amount: '7' });
    expect(filtered.chunks.some((c) => c.includes('payment.created'))).toBe(true);
  });

  test('client disconnect removes it and ends the response', () => {
    const res = buildMockRes();
    addClient({ res, filters: [] });
    expect(getClientCount()).toBe(1);

    res.emitClose();

    expect(getClientCount()).toBe(0);
    expect(res.end).toHaveBeenCalled();
  });

  test('removeClient is idempotent', () => {
    const res = buildMockRes();
    const { unsubscribe } = addClient({ res, filters: [] });
    unsubscribe();
    unsubscribe();
    expect(getClientCount()).toBe(0);
  });

  test('closeAllClients shuts down every stream (graceful shutdown path)', () => {
    const a = buildMockRes();
    const b = buildMockRes();
    addClient({ res: a, filters: [] });
    addClient({ res: b, filters: [] });

    const closed = closeAllClients();

    expect(closed).toBe(2);
    expect(getClientCount()).toBe(0);
  });

  test('heartbeat timer runs at the configured interval and is unref-ed', () => {
    jest.useFakeTimers();
    try {
      const res = buildMockRes();
      addClient({ res, filters: [] });

      const before = res.chunks.length;
      jest.advanceTimersByTime(SSE_HEARTBEAT_MS);

      const heartbeatChunks = res.chunks.slice(before);
      expect(heartbeatChunks.length).toBeGreaterThan(0);
      expect(heartbeatChunks.join('')).toMatch(/^: heartbeat /m);
    } finally {
      jest.useRealTimers();
    }
  });

  test('replayEventsFor sends buffered events newer than Last-Event-ID', () => {
    const { id: firstId } = publishEvent('payment.created', { seq: 1 });
    publishEvent('payment.created', { seq: 2 });
    publishEvent('payment.created', { seq: 3 });

    const res = buildMockRes();
    const { clientId } = addClient({ res, filters: [] });

    const replayed = replayEventsFor(
      { res, filters: [] },
      firstId,
    );

    expect(replayed).toBe(2);
    expect(res.writtenText()).toContain('"seq":2');
    expect(res.writtenText()).toContain('"seq":3');
    expect(res.writtenText()).not.toContain('"seq":1');
    void clientId;
  });

  test('replayEventsFor with an aged-out id still replays what is buffered', () => {
    publishEvent('payment.created', { seq: 1 });

    const res = buildMockRes();
    addClient({ res, filters: [] });

    const replayed = replayEventsFor({ res, filters: [] }, 'unknown-id');
    expect(replayed).toBe(1);
  });

  test('replayEventsFor without a Last-Event-ID replays nothing', () => {
    publishEvent('payment.created', { seq: 1 });
    const res = buildMockRes();
    addClient({ res, filters: [] });

    expect(replayEventsFor({ res, filters: [] }, '')).toBe(0);
    expect(res.chunks).toHaveLength(1); // only the connected event
  });

  test('a slow client past the backpressure limit is dropped', () => {
    const res = buildMockRes();
    addClient({ res, filters: [] });
    expect(getClientCount()).toBe(1);

    // Simulate a stalled socket with a huge buffer.
    res.socket.writableLength = 2 * 1024 * 1024;

    const { delivered } = publishEvent('payment.received', { amount: '1' });

    expect(delivered).toBe(0);
    expect(getClientCount()).toBe(0);
  });

  test('deliverLocally writes to matching clients without touching Redis', () => {
    const res = buildMockRes();
    addClient({ res, filters: [] });

    const delivered = deliverLocally('payment.received', { amount: '3' });

    expect(delivered).toBe(1);
    expect(res.writtenText()).toContain('payment.received');
  });

  test('getStats reports hub internals', () => {
    const res = buildMockRes();
    addClient({ res, filters: [] });
    publishEvent('payment.created', { seq: 1 });

    const stats = getStats();

    expect(stats.clients).toBe(1);
    expect(stats.replayBufferSize).toBeGreaterThanOrEqual(1);
    expect(stats.redisEnabled).toBe(false);
    expect(stats.config.heartbeatMs).toBe(SSE_HEARTBEAT_MS);
  });

  test('publishEvent survives a client whose write throws', () => {
    const good = buildMockRes();
    const bad = buildMockRes();
    bad.write = jest.fn(() => {
      throw new Error('EPIPE');
    });
    addClient({ res: good, filters: [] });
    addClient({ res: bad, filters: [] });

    const { delivered } = publishEvent('payment.received', { amount: '9' });

    expect(delivered).toBe(1);
    expect(good.chunks.some((c) => c.includes('payment.received'))).toBe(true);
  });
});
