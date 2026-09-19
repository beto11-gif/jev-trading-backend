import type { Candle, Interval, Symbol } from '../../schemas/contracts.js';
import { MarketStateStore } from '../../stores/market-state.js';
import { IndicatorService } from '../indicators/indicators.js';
export interface AnalysisContext {
  symbol: Symbol; interval: Interval; timestamp: number; price: number; candles: Candle[];
  indicators: ReturnType<IndicatorService['calculate']>;
  marketStructure: { higherHigh: boolean; higherLow: boolean; lowerHigh: boolean; lowerLow: boolean } | null;
  volumeContext: { relativeVolume: number | null };
}
const seconds: Record<Interval, number> = { '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400 };
export class MarketProcessor {
  constructor(private store: MarketStateStore, private indicators = new IndicatorService()) {}
  context(symbol: Symbol, interval: Interval): AnalysisContext | null {
    const now = Date.now();
    const snapshot = this.store.snapshot(symbol);
    if (!snapshot || now - snapshot.timestamp > 15000) return null;
    // Indicators use closed candles only, and are computed only on a scheduled analysis.
    const candles = this.store.candles(symbol, interval).filter(c => (c.time + seconds[interval]) * 1000 <= now);
    if (candles.length < 50) return null;
    for (let i = 1; i < candles.length; i++) if (candles[i]!.time - candles[i - 1]!.time !== seconds[interval]) return null;
    if (now - (candles.at(-1)!.time + seconds[interval]) * 1000 > seconds[interval] * 1000 + 15000) return null;
    const indicators = this.indicators.calculate(candles);
    const last = candles.at(-1)!, prev = candles.at(-2)!;
    return { symbol, interval, timestamp: now, price: snapshot.price, candles: candles.map(c => ({ ...c })), indicators,
      marketStructure: { higherHigh: last.high > prev.high, higherLow: last.low > prev.low, lowerHigh: last.high < prev.high, lowerLow: last.low < prev.low },
      volumeContext: { relativeVolume: indicators.relativeVolume } };
  }
}
