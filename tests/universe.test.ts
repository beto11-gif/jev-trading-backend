import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BinanceUniverse } from '../src/services/binance/universe.js';
import { buildApp } from '../src/app/app.js';
import { loadConfig } from '../src/config/env.js';
const coin = { symbol: 'ADAUSDT', baseAsset: 'ADA', quoteAsset: 'USDT', marginAsset: 'USDT', status: 'TRADING', contractType: 'PERPETUAL', underlyingType: 'COIN', isSpotTradingAllowed: true };
const response = (v: unknown) => new Response(JSON.stringify(v));

test('universe discovers new coins and excludes inactive, noncrypto and delivery futures', async () => {
  const u = new BinanceUniverse('https://example.com', 'https://example.org', async () => response({ symbols: [coin,
    { ...coin, symbol: 'OLDUSDT', status: 'BREAK' }, { ...coin, symbol: 'STOCKUSDT', underlyingType: 'EQUITY' },
    { ...coin, symbol: 'ADAUSD', quoteAsset: 'USD' }, { ...coin, symbol: 'ADA_NEXT', contractType: 'NEXT_QUARTER' }] }));
  assert.deepEqual((await u.symbols('futures')).symbols.map(s => s.symbol), ['ADAUSDT']);
  assert.equal((await u.symbols('spot')).symbols.length, 4);
});
test('snapshot reports missing/stale data and shares concurrent upstream calls', async () => {
  let calls = 0;
  const u = new BinanceUniverse('https://example.com', 'https://example.org', async url => {
    calls++; await new Promise(r => setTimeout(r, 5));
    return String(url).includes('exchangeInfo') ? response({ symbols: [coin, { ...coin, symbol: 'SOLUSDT' }] }) : response([
      { symbol: 'ADAUSDT', lastPrice: '1', priceChangePercent: '2', quoteVolume: '500', closeTime: Date.now() },
      { symbol: 'SOLUSDT', lastPrice: '5', priceChangePercent: '2', quoteVolume: '500', closeTime: 1 }]);
  });
  const [a, b] = await Promise.all([u.snapshot('futures'), u.snapshot('futures')]);
  assert.equal(calls, 2); assert.equal(a.items.length, 1); assert.deepEqual(b.missingSymbols, ['SOLUSDT']);
});
test('new routes validate membership and keep legacy contracts unchanged', async () => {
  const u = new BinanceUniverse('https://example.com', 'https://example.org', async url => String(url).includes('exchangeInfo') ? response({ symbols: [coin] }) : response([[1700000000000, '1', '2', '0.5', '1.5', '100']]));
  const app = await buildApp(loadConfig({ LOG_LEVEL: 'silent' }), { universe: u });
  try {
    assert.equal((await app.inject('/api/markets?market=futures')).json().symbols[0].symbol, 'ADAUSDT');
    const r = await app.inject('/api/market-candles?market=futures&symbol=ADAUSDT&interval=15m&limit=1');
    assert.equal(r.statusCode, 200); assert.equal(r.json().market, 'futures'); assert.equal(r.json().candles[0].close, 1.5);
    assert.equal((await app.inject('/api/market-candles?symbol=FAKE&interval=15m')).statusCode, 400);
    assert.equal((await app.inject('/api/market-candles?symbol=ADAUSDT&interval=15m&limit=501')).statusCode, 400);
    assert.equal((await app.inject('/api/candles?symbol=ADAUSDT&interval=15m')).statusCode, 400);
    assert.equal((await app.inject({ url: '/api/markets', headers: { origin: 'https://evil.example' } })).statusCode, 403);
  } finally { await app.close(); }
});
test('rate limiting activates shared cooldown and prevents repeated upstream requests', async () => {
  let calls = 0;
  const u = new BinanceUniverse('https://example.com', 'https://example.org', async () => { calls++; return new Response('', { status: 429 }); });
  await assert.rejects(u.symbols('futures'), { status: 429 });
  await assert.rejects(u.symbols('futures'), { status: 429 }); assert.equal(calls, 1);
});
