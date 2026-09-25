const express = require('express');
const { z } = require('zod');
const { prisma } = require('../../../prismaClient');
const { validateSchema } = require('../../middleware/validateSchema');
const { requireJson } = require('../../middleware/requireJson');
const { asyncHandler } = require('../../middleware/asyncHandler');
const { paymentIntentSchema, bulkPaymentSchema } = require('../../schemas/paymentSchema');
const { StrKey } = require('@stellar/stellar-sdk');
const { logger } = require('../../logger');
const { idempotencyMiddleware } = require('../../../middleware/idempotency');
// #730 — Push real-time payment status updates to SSE subscribers.
const { publishEvent } = require('../../services/sseService');

module.exports = (redisClient) => {
  const router = express.Router();
  
  router.use(idempotencyMiddleware(redisClient));

  // ── Idempotency protection for payment intent creation (POST /payments/bulk).
  // Duplicate submissions within 24h return the originally created intents. ──
  router.use(idempotencyMiddleware(redisClient));

  // POST /payments/bulk

/**
 * @openapi
 * /payments/bulk:
 *   post:
 *     tags:
 *       - v1
 *     description: POST /payments/bulk
 *     responses:
 *       200:
 *         description: Success
 */
  router.post('/payments/bulk', requireJson, validateSchema({ body: bulkPaymentSchema }), asyncHandler(async (req, res, next) => {
  const intents = req.body;

  // Additional per-item validation that requires runtime logic
  for (const idx in intents) {
    const intent = intents[idx];
    if (!StrKey.isValidEd25519PublicKey(intent.from)) {
      const err = new Error(`Invalid Stellar public key for 'from' at index ${idx}`);
      err.statusCode = 400;
      throw err;
    }
    if (!StrKey.isValidEd25519PublicKey(intent.to)) {
      const err = new Error(`Invalid Stellar public key for 'to' at index ${idx}`);
      err.statusCode = 400;
      throw err;
    }
  }

  try {
    const createOps = intents.map((intent) => prisma.paymentIntent.create({
      data: {
        externalId: intent.external_id,
        from: intent.from,
        to: intent.to,
        amount: intent.amount,
        asset: intent.asset,
        memoType: intent.memo_type,
        memo: intent.memo,
        metadata: intent.metadata,
      },
    }));

    const created = await prisma.$transaction(createOps);

    // #730 — Stream each newly registered payment intent to SSE subscribers
    // as a `payment.created` (pending) status event. Delivery failures are
    // contained inside the hub and never fail the HTTP request.
    for (const intent of created) {
      publishEvent('payment.created', {
        payment_id: intent.id,
        external_id: intent.externalId,
        from: intent.from,
        to: intent.to,
        amount: intent.amount,
        asset: intent.asset,
        memo_type: intent.memoType,
        memo: intent.memo,
        status: intent.status || 'pending',
        created_at: intent.createdAt instanceof Date
          ? intent.createdAt.toISOString()
          : intent.createdAt,
      });
    }

    return res.status(201).json({ ok: true, count: created.length, data: created.map((c) => ({ id: c.id, external_id: c.externalId })) });
  } catch (error) {
    logger.error('Bulk payment registration failed:', error);
    const dbErr = new Error('Failed to register payment intents');
    dbErr.statusCode = 500;
    return next(dbErr);
  }
}));

  return router;
};
