import { test } from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import Fastify from 'fastify';
import WebSocket, { WebSocketServer } from 'ws';
import { BinanceMarketStream, type StreamStatus } from '../src/services/binance/stream.js';

test('Binance stream reconnects after a dropped connection, normalizes data and stops timers', async () => {
  const app = Fastify({ logger: false });
  const upstream = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await once(upstream, 'listening');
  const address = upstream.address(); assert.ok(address && typeof address !== 'string');
  let connections = 0;
  const statuses: StreamStatus[] = [];
  upstream.on('connection', socket => {
    connections++;
    if (connections === 1) socket.close();
    else socket.send(JSON.stringify({ stream: 'btcusdt@ticker', data: { e: '24hrTicker', E: Date.now(), s: 'BTCUSDT', c: '100', P: '1', h: '110', l: '90', q: '123' } }));
  });
  let resolveEvent: (() => void) | undefined;
  const received = new Promise<void>(resolve => { resolveEvent = resolve; });
  const stream = new BinanceMarketStream('wss://example.com/ws', 'BTCUSDT', '1m', event => { assert.equal(event.type, 'ticker'); resolveEvent?.(); }, status => statuses.push(status), app.log, () => new WebSocket(`ws://127.0.0.1:${address.port}`));
  const timeout = setTimeout(() => resolveEvent?.(), 5000);
  try {
    stream.start(); await received;
    assert.equal(connections, 2); assert.ok(statuses.includes('reconnecting')); assert.ok(statuses.includes('connected'));
  } finally {
    clearTimeout(timeout); stream.stop();
    for (const client of upstream.clients) client.terminate();
    await new Promise<void>(resolve => upstream.close(() => resolve())); await app.close();
  }
});
