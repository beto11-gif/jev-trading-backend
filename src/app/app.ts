import Fastify, { LogController } from 'fastify';
import cors from '@fastify/cors';
import rateLimit from '@fastify/rate-limit';
import websocket from '@fastify/websocket';
import WebSocket from 'ws';
import { BinanceUniverse, MarketAccessError, universeQuery, universeCandlesQuery } from '../services/binance/universe.js';
import type { Config } from '../config/env.js';
import { candlesQuerySchema, clientMessageSchema, serverEventSchema } from '../schemas/contracts.js';
import { BinanceRestClient, UpstreamError } from '../services/binance/rest.js';
import { BinanceMarketStream, type StreamFactory } from '../services/binance/stream.js';
import { MarketStateStore } from '../stores/market-state.js';
import { MarketProcessor } from '../services/market/processor.js';
import { DisabledJevAnalyzer, type JevAnalyzer } from '../services/jev/analyzer.js';
import { AnalysisScheduler } from '../services/analysis/scheduler.js';
import { SubscriptionManager, type Subscriber } from '../websocket/subscriptions.js';

export async function buildApp(config: Config, dependencies: { rest?: BinanceRestClient; universe?: BinanceUniverse; streamFactory?: StreamFactory; analyzer?: JevAnalyzer } = {}) {
  const app = Fastify({ logger: { level: config.LOG_LEVEL, redact: ['req.headers.authorization', 'req.headers.cookie'] }, bodyLimit: 8192, logController: new LogController({ disableRequestLogging: true }) });
  const rest = dependencies.rest ?? new BinanceRestClient(config.BINANCE_REST_URL);
  const universe = dependencies.universe ?? new BinanceUniverse(config.BINANCE_REST_URL);
  const store = new MarketStateStore();
  const analyzer = dependencies.analyzer ?? new DisabledJevAnalyzer();
  const scheduler = new AnalysisScheduler(analyzer, new MarketProcessor(store), store, config.analysisPeriods, app.log);
  const subscriptions = new SubscriptionManager(dependencies.streamFactory ?? ((symbol, interval, event, status) => new BinanceMarketStream(config.BINANCE_WS_URL, symbol, interval, event, status, app.log)), rest, store, scheduler, app.log);
  const clients = new Map<WebSocket, { subscriber: Subscriber; alive: boolean }>();
  await app.register(cors, { origin: config.origins, methods: ['GET'] });
  await app.register(rateLimit, { max: 60, timeWindow: '1 minute' });
  await app.register(websocket, { options: { maxPayload: 8192, perMessageDeflate: false } });
  app.addHook('onRequest', async (request, reply) => {
    const origin = request.headers.origin;
    if (origin && !config.origins.includes(origin)) return reply.code(403).send({ type: 'error', code: 'ORIGIN_FORBIDDEN', message: 'Origin not allowed' });
  });
  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof MarketAccessError) {
      app.log.warn({ event: 'binance_access_restricted', upstreamStatus: error.upstreamStatus });
      return reply.code(503).send({ type: 'error', code: 'MARKET_ACCESS_RESTRICTED', message: 'Binance denied market access from the server location. Check hosting region and Binance availability.', upstreamStatus: error.upstreamStatus });
    }
    const code = typeof error === 'object' && error !== null && 'statusCode' in error ? error.statusCode : undefined;
    const status = error instanceof UpstreamError ? error.status : typeof code === 'number' && code >= 400 && code < 500 ? code : 500;
    app.log.warn({ event: 'request_failed', status });
    void reply.code(status).send({ type: 'error', code: status === 429 ? 'RATE_LIMITED' : status >= 500 ? 'SERVICE_UNAVAILABLE' : 'BAD_REQUEST', message: status >= 500 ? 'Service temporarily unavailable' : 'Invalid or excessive request' });
  });
  app.get('/api/health', { config: { rateLimit: false } }, async () => ({ status: 'ok', service: 'jev-trading-backend', timestamp: new Date().toISOString(), uptime: process.uptime(), environment: config.NODE_ENV, jev: analyzer.enabled ? 'ENABLED' : 'DISABLED / NOT CONFIGURED', binance: subscriptions.statuses() }));
  app.get('/api/candles', async (request, reply) => {
    const query = candlesQuerySchema.safeParse(request.query);
    if (!query.success) return reply.code(400).send({ type: 'error', code: 'INVALID_QUERY', message: 'Expected allowed symbol, interval and integer limit 1..500' });
    const { symbol, interval, limit } = query.data;
    return { symbol, interval, candles: await rest.candles(symbol, interval, limit) };
  });
  app.get('/api/markets', async (request, reply) => {
    const query = universeQuery.safeParse(request.query);
    if (!query.success) return reply.code(400).send({ code: 'INVALID_QUERY' });
    return universe.symbols(query.data.market);
  });
  app.get('/api/market-snapshot', async (request, reply) => {
    const query = universeQuery.safeParse(request.query);
    if (!query.success) return reply.code(400).send({ code: 'INVALID_QUERY' });
    return universe.snapshot(query.data.market);
  });
  app.get('/api/market-candles', async (request, reply) => {
    const query = universeCandlesQuery.safeParse(request.query);
    if (!query.success) return reply.code(400).send({ code: 'INVALID_QUERY' });
    const { market, symbol, interval, limit } = query.data;
    return universe.candles(market, symbol, interval, limit);
  });
  app.get('/ws', { websocket: true }, socket => {
    if (clients.size >= 1000) { socket.close(1013, 'Server capacity reached'); return; }
    const subscriber: Subscriber = { send(event) {
      if (socket.readyState !== WebSocket.OPEN) return;
      if (socket.bufferedAmount > 1024 * 1024) { socket.terminate(); return; }
      socket.send(JSON.stringify(serverEventSchema.parse(event)));
    } };
    const client = { subscriber, alive: true }; clients.set(socket, client);
    app.log.info({ event: 'client_connected', clients: clients.size });
    let windowStart = Date.now(), messages = 0;
    socket.on('pong', () => { client.alive = true; });
    socket.on('error', () => { app.log.warn({ event: 'client_socket_error' }); });
    socket.on('message', (data, binary) => {
      if (Date.now() - windowStart > 10000) { windowStart = Date.now(); messages = 0; }
      if (++messages > 30) { socket.close(1008, 'Message rate exceeded'); return; }
      try {
        if (binary) throw new Error('Binary not supported');
        const parsed = clientMessageSchema.safeParse(JSON.parse(data.toString()));
        if (!parsed.success) throw new Error('Invalid subscription');
        const { type, symbol, interval } = parsed.data;
        if (type === 'subscribe') {
          subscriptions.subscribe(subscriber, symbol, interval);
          if (!analyzer.enabled) subscriber.send({ type: 'error', code: 'ANALYSIS_UNAVAILABLE', message: 'JEV DISABLED / NOT CONFIGURED' });
        } else subscriptions.unsubscribe(subscriber, symbol, interval);
      } catch { subscriber.send({ type: 'error', code: 'INVALID_MESSAGE', message: 'Expected subscribe/unsubscribe with allowed symbol and interval' }); }
    });
    socket.on('close', () => { subscriptions.disconnect(subscriber); clients.delete(socket); app.log.info({ event: 'client_disconnected', clients: clients.size }); });
    subscriber.send({ type: 'connection_status', status: 'connected', timestamp: Date.now() });
  });
  const heartbeat = setInterval(() => {
    for (const [socket, client] of clients) {
      if (!client.alive) { socket.terminate(); continue; }
      client.alive = false; socket.ping();
    }
  }, 30000);
  heartbeat.unref();
  // preClose runs before Fastify waits for open WebSocket clients.
  app.addHook('preClose', async () => {
    clearInterval(heartbeat); subscriptions.close();
    for (const socket of clients.keys()) socket.terminate();
    clients.clear();
  });
  return app;
}
