import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config/env.js';
import { candlesQuerySchema, clientMessageSchema, normalizeAnalysis } from '../src/schemas/contracts.js';
import { normalizeCandles, normalizeStream } from '../src/services/binance/normalization.js';
import { MarketStateStore } from '../src/stores/market-state.js';
import { IndicatorService } from '../src/services/indicators/indicators.js';
import { BinanceRestClient } from '../src/services/binance/rest.js';

export const candle = { time: 1700000000, open: 100, high: 110, low: 90, close: 105, volume: 10 };
test('environment validates ports, origins, protocols and production configuration', () => {
  assert.equal(loadConfig({}).PORT, 3000);
  for (const env of [{ PORT: '0' }, { PORT: 'abc' }, { NODE_ENV: 'production' }, { ALLOWED_ORIGINS: '*' }, { BINANCE_REST_URL: 'http://example.com' }, { BINANCE_WS_URL: 'wss://user:secret@example.com' }]) assert.throws(() => loadConfig(env));
  assert.deepEqual(loadConfig({ NODE_ENV: 'production', ALLOWED_ORIGINS: 'https://dashboard.example.com' }).origins, ['https://dashboard.example.com']);
});
test('queries and WS messages enforce whitelist, shape and bounded limits', () => {
  assert.equal(candlesQuerySchema.parse({ symbol: 'BTCUSDT', interval: '1m' }).limit, 500);
  for (const override of [{ symbol: 'DOGEUSDT' }, { interval: '1s' }, { limit: 501 }, { limit: 1.5 }, { limit: '' }]) assert.equal(candlesQuerySchema.safeParse({ symbol: 'BTCUSDT', interval: '1m', ...override }).success, false);
  assert.equal(clientMessageSchema.safeParse({ type: 'subscribe', symbol: 'BTCUSDT', interval: '1m', unexpected: true }).success, false);
});
test('Binance normalization uses seconds for candles and USDT quote volume for ticker', () => {
  assert.deepEqual(normalizeCandles([[1700000000000, '100', '110', '90', '105', '10', 1700000059999]]), [candle]);
  assert.throws(() => normalizeCandles([[1, 'NaN', '1', '1', '1', '1']]));
  const event = normalizeStream({ e: '24hrTicker', E: 1700000000000, s: 'BTCUSDT', c: '105', P: '1.5', h: '110', l: '90', q: '12345' });
  assert.equal(event.type, 'ticker');
  if (event.type === 'ticker') assert.equal(event.snapshot.volume24h, 12345);
  const update = normalizeStream({ e: 'kline', E: 1700000000000, s: 'BTCUSDT', k: { t: 1700000000000, s: 'BTCUSDT', i: '1m', o: '100', h: '110', l: '90', c: '105', v: '10', x: true } });
  assert.equal(update.type, 'candle');
  if (update.type === 'candle') { assert.deepEqual(update.candle, candle); assert.equal(update.closed, true); }
});
test('analysis normalizes scores, enforces signal, rejects malformed output', () => {
  const base = { signal: 'SELL', buy: 70, sell: 10, wait: 10, confidence: 80, summary: 'Example', model: 'test' };
  const normalized = normalizeAnalysis(base);
  assert.equal(normalized.signal, 'BUY');
  assert.ok(Math.abs(normalized.buy + normalized.sell + normalized.wait - 100) < 0.001);
  for (const override of [{ buy: -1 }, { buy: 101 }, { confidence: Infinity }, { summary: '' }, { buy: 0, sell: 0, wait: 0 }]) assert.throws(() => normalizeAnalysis({ ...base, ...override }));
});
test('state upserts a candle, rejects old events and bounds memory', () => {
  const store = new MarketStateStore();
  store.update('BTCUSDT', '1m', candle, 100, false);
  store.update('BTCUSDT', '1m', { ...candle, close: 106 }, 101, true);
  assert.equal(store.update('BTCUSDT', '1m', candle, 99, false), false);
  store.seed('BTCUSDT', '1m', [candle]);
  assert.equal(store.candles('BTCUSDT', '1m')[0]?.close, 106);
  for (let i = 1; i <= 600; i++) store.update('BTCUSDT', '1m', { ...candle, time: candle.time + i * 60 }, 101 + i, true);
  assert.equal(store.candles('BTCUSDT', '1m').length, 500);
});
test('indicators handle flat markets and insufficient warmup', () => {
  const indicators = new IndicatorService();
  assert.equal(indicators.calculate([]).ema20, null);
  const result = indicators.calculate(Array.from({ length: 60 }, (_, i) => ({ time: i * 60, open: 100, high: 100, low: 100, close: 100, volume: 10 })));
  assert.deepEqual(result, { ema20: 100, ema50: 100, rsi14: 50, atr14: 0, relativeVolume: 1 });
});
test('recovery history corrects the stale open candle from before a disconnection', () => {
  const store = new MarketStateStore();
  store.update('BTCUSDT', '1m', candle, 100, false);
  store.seed('BTCUSDT', '1m', [{ ...candle, close: 109 }], 200);
  assert.equal(store.candles('BTCUSDT', '1m')[0]?.close, 109);
  store.update('BTCUSDT', '1m', { ...candle, close: 110 }, 300, false);
  store.seed('BTCUSDT', '1m', [{ ...candle, close: 108 }], 250);
  assert.equal(store.candles('BTCUSDT', '1m')[0]?.close, 110);
});
test('REST shares requests, caches and respects upstream throttling', async () => {
  let calls = 0;
  const client = new BinanceRestClient('https://example.com', async () => { calls++; return new Response(JSON.stringify([[1700000000000, '100', '110', '90', '105', '10']])); });
  await Promise.all([client.candles('BTCUSDT', '1m', 10), client.candles('BTCUSDT', '1m', 20)]);
  await client.candles('BTCUSDT', '1m', 1);
  assert.equal(calls, 1);
  const limited = new BinanceRestClient('https://example.com', async () => { calls++; return new Response('', { status: 429, headers: { 'retry-after': '60' } }); });
  await assert.rejects(limited.candles('BTCUSDT', '1m', 10));
  await assert.rejects(limited.candles('ETHUSDT', '1m', 10));
  assert.equal(calls, 2);
});
