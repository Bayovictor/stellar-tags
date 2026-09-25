const express = require('express');

const userRoutes = require('./userRoutes');
const receiptRoutes = require('./receiptRoutes');
const webhookRoutes = require('./webhookRoutes');
const statsRoutes = require('./statsRoutes');
const historyRoutes = require('./historyRoutes');
const exportRoutes = require('./exportRoutes');
const paymentRoutes = require('./paymentRoutes');
const contractRoutes = require('./contractRoutes');
const federationRoutes = require('./federationRoutes');
// #730 — SSE endpoints for real-time payment status updates.
const sseRoutes = require('./sseRoutes');

module.exports = (redisClient) => {
  const router = express.Router();

  const adminRoutes = require('./adminRoutes')(redisClient);

  router.use('/', userRoutes);
  router.use('/', receiptRoutes);
  router.use('/', contractRoutes);
  router.use('/', historyRoutes);
  router.use('/', exportRoutes);

  router.use('/', webhookRoutes(redisClient));
  router.use('/', paymentRoutes(redisClient));
  router.use('/', statsRoutes(redisClient));
  router.use('/', federationRoutes(redisClient));
  router.use('/', adminRoutes);
  router.use('/', sseRoutes(redisClient));

  return router;
};
