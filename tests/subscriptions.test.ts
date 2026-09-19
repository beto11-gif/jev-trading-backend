import { test } from 'node:test';
import assert from 'node:assert/strict';
import Fastify from 'fastify';
import { SubscriptionManager, type Subscriber } from '../src/websocket/subscriptions.js';
import { MarketStateStore } from '../src/stores/market-state.js';
import { BinanceRestClient } from '../src/services/binance/rest.js';
import { AnalysisScheduler } from '../src/services/analysis/scheduler.js';
import { DisabledJevAnalyzer } from '../src/services/jev/analyzer.js';
import { MarketProcessor } from '../src/services/market/processor.js';
import type { ServerEvent } from '../src/schemas/contracts.js';
import type { StreamStatus } from '../src/services/binance/stream.js';
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

test('subscriptions are idempotent, share upstream, cancel grace and clean up at zero references', async () => {
  const app = Fastify({ logger: false });
  const store = new MarketStateStore();
  let requests = 0, starts = 0, stops = 0;
  let status: ((status: StreamStatus) => void) | undefined;
  const rest = new BinanceRestClient('https://example.com', async () => { requests++; return new Response('[]'); });
  const scheduler = new AnalysisScheduler(new DisabledJevAnalyzer(), new MarketProcessor(store), store, { '1m': 10, '5m': 10, '15m': 10, '1h': 10, '4h': 10 }, app.log);
  const manager = new SubscriptionManager((_s, _i, _event, cb) => { status = cb; return { start() { starts++; cb('connected'); }, stop() { stops++; } }; }, rest, store, scheduler, app.log, 20);
  const messages: ServerEvent[] = [];
  const a: Subscriber = { send(event) { messages.push(event); } }, b: Subscriber = { send() {} };
  try {
    manager.subscribe(a, 'BTCUSDT', '1m'); manager.subscribe(a, 'BTCUSDT', '1m'); manager.subscribe(b, 'BTCUSDT', '1m');
    await sleep(10);
    assert.equal(starts, 1); assert.equal(requests, 1); assert.equal(manager.statuses()[0]?.subscribers, 2);
    status?.('reconnecting'); status?.('connected');
    assert.ok(messages.some(e => e.type === 'connection_status' && e.status === 'reconnecting'));
    manager.disconnect(a); assert.equal(stops, 0);
    manager.disconnect(b); await sleep(5); manager.subscribe(a, 'BTCUSDT', '1m');
    await sleep(30); assert.equal(stops, 0); assert.equal(starts, 1);
    manager.disconnect(a); await sleep(30); assert.equal(stops, 1); assert.equal(manager.statuses().length, 0);
  } finally { manager.close(); await app.close(); }
});

test('analysis failures are caught and do not publish invented signals', async () => {
  const app = Fastify({ logger: false });
  const store = new MarketStateStore();
  const time = Math.floor(Date.now() / 60000) * 60;
  store.seed('BTCUSDT', '1m', Array.from({ length: 60 }, (_, i) => ({ time: time - (60 - i) * 60, open: 100, high: 110, low: 90, close: 100, volume: 10 })));
  store.setSnapshot({ symbol: 'BTCUSDT', price: 100, change24h: 0, high24h: 110, low24h: 90, volume24h: 1000, timestamp: Date.now() });
  const events: ServerEvent[] = [];
  const scheduler = new AnalysisScheduler({ enabled: true, async analyze() { throw new Error('Provider unavailable'); } }, new MarketProcessor(store), store, { '1m': 5, '5m': 5, '15m': 5, '1h': 5, '4h': 5 }, app.log);
  const stop = scheduler.start('BTCUSDT', '1m', event => events.push(event));
  try { await sleep(30); assert.ok(events.some(e => e.type === 'error')); assert.equal(events.some(e => e.type === 'analysis_update'), false); }
  finally { stop(); await app.close(); }
});
