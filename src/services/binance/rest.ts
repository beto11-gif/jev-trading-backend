import type { Interval, Symbol } from '../../schemas/contracts.js';
import { normalizeCandles } from './normalization.js';
export class UpstreamError extends Error {
  constructor(public readonly status = 502) { super('Market data temporarily unavailable'); }
}
export class BinanceRestClient {
  private inflight = new Map<string, Promise<ReturnType<typeof normalizeCandles>>>();
  private cache = new Map<string, { expires: number; candles: ReturnType<typeof normalizeCandles> }>();
  private cooldownUntil = 0;
  constructor(private baseUrl: string, private fetcher: typeof fetch = fetch) {}
  async candles(symbol: Symbol, interval: Interval, limit: number) {
    const key = `${symbol}:${interval}`;
    const cached = this.cache.get(key);
    if (cached && cached.expires > Date.now()) return cached.candles.slice(-limit);
    if (Date.now() < this.cooldownUntil) throw new UpstreamError(503);
    let pending = this.inflight.get(key);
    if (!pending) {
      pending = this.request(symbol, interval).then(candles => {
        this.cache.set(key, { candles, expires: Date.now() + 2000 });
        return candles;
      }).finally(() => this.inflight.delete(key));
      this.inflight.set(key, pending);
    }
    return (await pending).slice(-limit);
  }
  private async request(symbol: Symbol, interval: Interval) {
    const url = new URL('/api/v3/klines', this.baseUrl);
    url.search = new URLSearchParams({ symbol, interval, limit: '500' }).toString();
    try {
      const response = await this.fetcher(url, { signal: AbortSignal.timeout(8000) });
      if ([418, 429].includes(response.status)) {
        const seconds = Number(response.headers.get('retry-after'));
        this.cooldownUntil = Date.now() + (Number.isFinite(seconds) && seconds > 0 ? seconds : 60) * 1000;
      }
      if (!response.ok) throw new UpstreamError();
      return normalizeCandles(await response.json());
    } catch { throw new UpstreamError(); }
  }
}
