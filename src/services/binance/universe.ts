import { z } from 'zod';
import { normalizeCandles } from './normalization.js';
import { UpstreamError } from './rest.js';
import { intervalSchema } from '../../schemas/contracts.js';

export const marketSchema = z.enum(['spot', 'futures']);
export type Market = z.infer<typeof marketSchema>;
export const universeQuery = z.object({ market: marketSchema.default('futures') }).strict();
export const universeCandlesQuery = z.object({
  market: marketSchema.default('futures'), symbol: z.string().min(2).max(40),
  interval: intervalSchema, limit: z.coerce.number().int().min(1).max(500).default(100),
}).strict();
const instrument = z.object({
  symbol: z.string().min(2).max(40), status: z.string(), baseAsset: z.string(), quoteAsset: z.string(),
  isSpotTradingAllowed: z.boolean().optional(), contractType: z.string().optional(),
  marginAsset: z.string().optional(), underlyingType: z.string().optional(),
});
const exchange = z.object({ symbols: z.array(instrument).max(30000) });
const numeric = z.string().min(1).transform(Number).pipe(z.number().finite());
const ticker = z.object({ symbol: z.string(), lastPrice: numeric.pipe(z.number().positive()),
  priceChangePercent: numeric, quoteVolume: numeric.pipe(z.number().nonnegative()), closeTime: z.number().int().nonnegative() });

/** Independent read-only universe. Existing four-symbol Spot streams remain untouched. */
export class BinanceUniverse {
  private cache = new Map<string, { expires: number; value: unknown }>();
  private pending = new Map<string, Promise<unknown>>();
  private cooldown = new Map<Market, number>();
  private active = 0;
  constructor(private spotUrl: string, private futuresUrl = 'https://fapi.binance.com', private fetcher: typeof fetch = fetch) {}

  private async read<T>(market: Market, path: string, params: Record<string, string>, ttl: number, parse: (raw: unknown) => T): Promise<T> {
    const url = new URL(path, market === 'spot' ? this.spotUrl : this.futuresUrl);
    url.search = new URLSearchParams(params).toString();
    const key = `${market}:${url.href}`;
    const cached = this.cache.get(key);
    if (cached && cached.expires > Date.now()) return cached.value as T;
    const pending = this.pending.get(key);
    if (pending) return pending as Promise<T>;
    if ((this.cooldown.get(market) ?? 0) > Date.now()) throw new UpstreamError(429);
    // Bound work globally; callers retry later, never allocate an unlimited queue.
    if (this.active >= 4) throw new UpstreamError(503);
    this.active++;
    const work = (async () => {
      try {
        const response = await this.fetcher(url, { signal: AbortSignal.timeout(8000) });
        if ([418, 429].includes(response.status)) {
          const seconds = Number(response.headers.get('retry-after'));
          this.cooldown.set(market, Date.now() + Math.min(86400, Math.max(60, Number.isFinite(seconds) ? seconds : 60)) * 1000);
          throw new UpstreamError(429);
        }
        if (!response.ok) throw new UpstreamError();
        const value = parse(await response.json());
        if (this.cache.size >= 256) this.cache.delete(this.cache.keys().next().value!);
        this.cache.set(key, { value, expires: Date.now() + ttl });
        return value;
      } catch (error) { throw error instanceof UpstreamError ? error : new UpstreamError(); }
      finally { this.active--; this.pending.delete(key); }
    })();
    this.pending.set(key, work);
    return work;
  }

  async symbols(market: Market) {
    return this.read(market, market === 'spot' ? '/api/v3/exchangeInfo' : '/fapi/v1/exchangeInfo', {}, 300000, raw => {
      const symbols = exchange.parse(raw).symbols.filter(s => s.status === 'TRADING' &&
        (market === 'spot' ? s.isSpotTradingAllowed === true :
          s.contractType === 'PERPETUAL' && s.quoteAsset === 'USDT' && s.marginAsset === 'USDT' && s.underlyingType === 'COIN'))
        .map(s => ({ symbol: s.symbol, baseAsset: s.baseAsset, quoteAsset: s.quoteAsset }));
      return { market, fetchedAt: Date.now(), scope: market === 'spot' ? 'All active Spot pairs' : 'Active crypto USDT perpetual contracts', symbols };
    });
  }

  async snapshot(market: Market) {
    const universe = await this.symbols(market);
    const allowed = new Set(universe.symbols.map(s => s.symbol));
    const snapshot = await this.read(market, market === 'spot' ? '/api/v3/ticker/24hr' : '/fapi/v1/ticker/24hr', {}, 15000, raw => {
      const rows = z.array(z.unknown()).max(30000).parse(raw);
      const items: z.infer<typeof ticker>[] = [];
      for (const row of rows) { const parsed = ticker.safeParse(row); if (parsed.success) items.push(parsed.data); }
      return { fetchedAt: Date.now(), items };
    });
    const items = snapshot.items.filter(t => allowed.has(t.symbol) && Date.now() - t.closeTime <= 120000 && t.closeTime <= Date.now() + 5000);
    const present = new Set(items.map(t => t.symbol));
    return { market, fetchedAt: snapshot.fetchedAt, totalEligible: allowed.size, items,
      missingSymbols: [...allowed].filter(s => !present.has(s)), jev: 'NOT_USED', leverage20xVerified: false };
  }

  async candles(market: Market, symbol: string, interval: z.infer<typeof intervalSchema>, limit: number) {
    const universe = await this.symbols(market);
    if (!universe.symbols.some(s => s.symbol === symbol)) throw new UpstreamError(400);
    const candles = await this.read(market, market === 'spot' ? '/api/v3/klines' : '/fapi/v1/klines',
      { symbol, interval, limit: '500' }, 10000, normalizeCandles);
    return { market, symbol, interval, candles: candles.slice(-limit) };
  }
}
