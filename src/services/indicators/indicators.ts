import type { Candle } from '../../schemas/contracts.js';
function ema(values: number[], period: number): number | null {
  if (values.length < period) return null;
  let value = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (const price of values.slice(period)) value += (price - value) * 2 / (period + 1);
  return value;
}
export class IndicatorService {
  calculate(candles: readonly Candle[]) {
    const closes = candles.map(c => c.close);
    let rsi14: number | null = null;
    let atr14: number | null = null;
    if (candles.length >= 15) {
      let gain = 0, loss = 0, atr = 0;
      for (let i = 1; i < candles.length; i++) {
        const c = candles[i]!, previous = candles[i - 1]!;
        const delta = c.close - previous.close;
        const tr = Math.max(c.high - c.low, Math.abs(c.high - previous.close), Math.abs(c.low - previous.close));
        if (i <= 14) { gain += Math.max(0, delta) / 14; loss += Math.max(0, -delta) / 14; atr += tr / 14; }
        else { gain = (gain * 13 + Math.max(0, delta)) / 14; loss = (loss * 13 + Math.max(0, -delta)) / 14; atr = (atr * 13 + tr) / 14; }
      }
      rsi14 = loss === 0 ? gain === 0 ? 50 : 100 : 100 - 100 / (1 + gain / loss); atr14 = atr;
    }
    const baseline = candles.slice(-21, -1);
    const average = baseline.length === 20 ? baseline.reduce((n, c) => n + c.volume, 0) / 20 : 0;
    return { ema20: ema(closes, 20), ema50: ema(closes, 50), rsi14, atr14, relativeVolume: average > 0 ? candles.at(-1)!.volume / average : null };
  }
}
