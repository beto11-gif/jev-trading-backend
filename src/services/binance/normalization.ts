import { z } from 'zod';
import { candleSchema, intervalSchema, marketSnapshotSchema, symbolSchema } from '../../schemas/contracts.js';
const numeric = z.string().min(1).transform(Number).pipe(z.number().finite());
const row = z.tuple([z.number().int().nonnegative(), numeric, numeric, numeric, numeric, numeric]).rest(z.unknown());
export function normalizeCandles(input: unknown) {
  return z.array(row).max(500).parse(input).map(r => candleSchema.parse({ time: Math.floor(r[0] / 1000), open: r[1], high: r[2], low: r[3], close: r[4], volume: r[5] }));
}
const ticker = z.object({ e: z.literal('24hrTicker'), E: z.number().int().nonnegative(), s: symbolSchema, c: numeric, P: numeric, h: numeric, l: numeric, q: numeric });
const kline = z.object({ e: z.literal('kline'), E: z.number().int().nonnegative(), s: symbolSchema,
  k: z.object({ t: z.number().int().nonnegative(), s: symbolSchema, i: intervalSchema, o: numeric, h: numeric, l: numeric, c: numeric, v: numeric, x: z.boolean() }) });
export function normalizeStream(input: unknown) {
  const t = ticker.safeParse(input);
  if (t.success) {
    const v = t.data;
    return { type: 'ticker' as const, snapshot: marketSnapshotSchema.parse({ symbol: v.s, price: v.c, change24h: v.P, high24h: v.h, low24h: v.l, volume24h: v.q, timestamp: v.E }) };
  }
  const v = kline.parse(input);
  if (v.s !== v.k.s) throw new Error('Inconsistent symbol');
  return { type: 'candle' as const, symbol: v.s, interval: v.k.i, timestamp: v.E, closed: v.k.x,
    candle: candleSchema.parse({ time: Math.floor(v.k.t / 1000), open: v.k.o, high: v.k.h, low: v.k.l, close: v.k.c, volume: v.k.v }) };
}
export type MarketEvent = ReturnType<typeof normalizeStream>;
