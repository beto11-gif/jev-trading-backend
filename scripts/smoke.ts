import assert from 'node:assert/strict';
import WebSocket from 'ws';
import { serverEventSchema } from '../src/schemas/contracts.js';

const base = process.env.SMOKE_URL ?? 'http://127.0.0.1:3000';
const health = await fetch(`${base}/api/health`, { signal: AbortSignal.timeout(10000) });
assert.equal(health.status, 200);
console.log('HEALTH', await health.json());
const response = await fetch(`${base}/api/candles?symbol=BTCUSDT&interval=1m&limit=10`, { signal: AbortSignal.timeout(15000) });
assert.equal(response.status, 200, `Candles HTTP ${response.status}`);
const result = await response.json() as { candles: unknown[] };
assert.equal(result.candles.length, 10);
console.log('REAL_CANDLES', result);
await new Promise<void>((resolve, reject) => {
  const url = new URL('/ws', base); url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  const socket = new WebSocket(url);
  const received = new Set<string>();
  const timer = setTimeout(() => { socket.terminate(); reject(new Error('Timed out waiting for real market_update and candle_update')); }, 45000);
  const finish = (error?: Error) => { clearTimeout(timer); socket.close(); if (error) reject(error); else resolve(); };
  socket.on('open', () => socket.send(JSON.stringify({ type: 'subscribe', symbol: 'BTCUSDT', interval: '1m' })));
  socket.on('error', finish);
  socket.on('message', raw => {
    try {
      const event = serverEventSchema.parse(JSON.parse(raw.toString()));
      if (event.type === 'market_update' || event.type === 'candle_update') {
        console.log('REAL_WS_EVENT', event); received.add(event.type);
        if (received.size === 2) finish();
      }
    } catch (error) { finish(error instanceof Error ? error : new Error('Invalid event')); }
  });
});
console.log('SMOKE PASSED: real Binance REST + WebSocket');
