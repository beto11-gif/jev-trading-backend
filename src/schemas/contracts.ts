import { z } from 'zod';

export const symbolSchema = z.enum(['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT']);
export const intervalSchema = z.enum(['1m', '5m', '15m', '1h', '4h']);
export type Symbol = z.infer<typeof symbolSchema>;
export type Interval = z.infer<typeof intervalSchema>;
export const candleSchema = z.object({
  time: z.number().int().nonnegative(), open: z.number().positive(), high: z.number().positive(),
  low: z.number().positive(), close: z.number().positive(), volume: z.number().nonnegative(),
}).refine(c => c.high >= Math.max(c.open, c.close, c.low) && c.low <= Math.min(c.open, c.close), 'Invalid OHLC');
export type Candle = z.infer<typeof candleSchema>;
export const candlesQuerySchema = z.object({ symbol: symbolSchema, interval: intervalSchema, limit: z.coerce.number().int().min(1).max(500).default(500) }).strict();
export const clientMessageSchema = z.object({ type: z.enum(['subscribe', 'unsubscribe']), symbol: symbolSchema, interval: intervalSchema }).strict();
export const marketSnapshotSchema = z.object({
  symbol: symbolSchema, price: z.number().positive(), change24h: z.number(), high24h: z.number().positive(),
  low24h: z.number().positive(), volume24h: z.number().nonnegative(), timestamp: z.number().int().nonnegative(),
});
export type MarketSnapshot = z.infer<typeof marketSnapshotSchema>;
const score = z.number().min(0).max(100);
export const analysisResultSchema = z.object({ signal: z.enum(['BUY', 'SELL', 'WAIT']), buy: score, sell: score, wait: score,
  confidence: score, summary: z.string().trim().min(1).max(2000), model: z.string().trim().min(1).max(100) }).strict();
export type AnalysisResult = z.infer<typeof analysisResultSchema>;
export function normalizeAnalysis(input: unknown): AnalysisResult {
  const result = analysisResultSchema.parse(input);
  const total = result.buy + result.sell + result.wait;
  if (total === 0) throw new Error('Analysis scores sum to zero');
  const buy = Math.floor(result.buy / total * 10000) / 100;
  const sell = Math.floor(result.sell / total * 10000) / 100;
  const wait = Math.round((100 - buy - sell) * 100) / 100;
  // Ties favor WAIT, then BUY, deterministically.
  const signal = wait >= buy && wait >= sell ? 'WAIT' : buy >= sell ? 'BUY' : 'SELL';
  return analysisResultSchema.parse({ ...result, buy, sell, wait, signal });
}
export const serverEventSchema = z.discriminatedUnion('type', [
  marketSnapshotSchema.extend({ type: z.literal('market_update') }),
  z.object({ type: z.literal('candle_update'), symbol: symbolSchema, interval: intervalSchema, candle: candleSchema, closed: z.boolean() }),
  analysisResultSchema.extend({ type: z.literal('analysis_update'), symbol: symbolSchema, interval: intervalSchema, timestamp: z.number() }),
  z.object({ type: z.literal('connection_status'), status: z.enum(['connected', 'connecting', 'reconnecting', 'disconnected']), timestamp: z.number(), symbol: symbolSchema.optional(), interval: intervalSchema.optional() }),
  z.object({ type: z.literal('error'), code: z.string(), message: z.string() }),
]);
export type ServerEvent = z.infer<typeof serverEventSchema>;
export type AnalysisUpdate = Extract<ServerEvent, { type: 'analysis_update' }>;
export const streamKey = (symbol: Symbol, interval: Interval) => `${symbol}:${interval}`;
