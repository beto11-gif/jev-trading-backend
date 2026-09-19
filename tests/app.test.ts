import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import WebSocket from 'ws';
import { buildApp } from '../src/app/app.js';
import { loadConfig } from '../src/config/env.js';
import { BinanceRestClient } from '../src/services/binance/rest.js';
import type { MarketEvent } from '../src/services/binance/normalization.js';
import { serverEventSchema, type ServerEvent } from '../src/schemas/contracts.js';

test('health, validation, CORS, shared WebSocket stream and shutdown', async () => {
  let starts = 0, stops = 0;
  let emit: ((event: MarketEvent) => void) | undefined;
  const rest = new BinanceRestClient('https://example.com', async () => new Response(JSON.stringify([[1700000000000, '100', '110', '90', '105', '10']])));
  const app = await buildApp(loadConfig({ LOG_LEVEL: 'silent' }), { rest, streamFactory: (_symbol, _interval, event, status) => {
    emit = event;
    return { start() { starts++; status('connected'); }, stop() { stops++; } };
  } });
  try {
    const health = await app.inject('/api/health'); assert.equal(health.statusCode, 200); assert.match(health.json().jev, /DISABLED/);
    assert.equal((await app.inject('/api/candles?symbol=BTCUSDT&interval=1m&limit=10')).json().candles.length, 1);
    assert.equal((await app.inject('/api/candles?symbol=BAD&interval=1m')).statusCode, 400);
    assert.equal((await app.inject({ url: '/api/health', headers: { origin: 'https://evil.example' } })).statusCode, 403);
    assert.equal((await app.inject({ url: '/api/health', headers: { origin: 'http://localhost:5173' } })).headers['access-control-allow-origin'], 'http://localhost:5173');
    await app.listen({ port: 0, host: '127.0.0.1' });
    const address = app.server.address(); assert.ok(address && typeof address !== 'string');
    const url = `ws://127.0.0.1:${address.port}/ws`;
    const a = new WebSocket(url), b = new WebSocket(url);
    const events: ServerEvent[] = [];
    a.on('message', data => events.push(serverEventSchema.parse(JSON.parse(data.toString()))));
    await Promise.all([once(a, 'open'), once(b, 'open')]);
    const subscribe = JSON.stringify({ type: 'subscribe', symbol: 'BTCUSDT', interval: '1m' });
    a.send(subscribe); b.send(subscribe); a.send(subscribe);
    await new Promise(resolve => setTimeout(resolve, 50)); assert.equal(starts, 1);
    emit?.({ type: 'ticker', snapshot: { symbol: 'BTCUSDT', price: 105, change24h: 1, high24h: 110, low24h: 90, volume24h: 2000, timestamp: Date.now() } });
    emit?.({ type: 'candle', symbol: 'BTCUSDT', interval: '1m', candle: { time: 1700000000, open: 100, high: 110, low: 90, close: 105, volume: 10 }, timestamp: Date.now(), closed: false });
    a.send('not-json');
    await new Promise(resolve => setTimeout(resolve, 50));
    assert.ok(events.some(e => e.type === 'market_update'));
    assert.ok(events.some(e => e.type === 'candle_update'));
    assert.ok(events.some(e => e.type === 'error' && e.code === 'INVALID_MESSAGE'));
    const closed = Promise.all([once(a, 'close'), once(b, 'close')]);
    await app.close(); await closed; assert.equal(stops, 1);
  } finally { await app.close(); }
});
