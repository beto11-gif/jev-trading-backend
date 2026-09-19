import { streamKey, type AnalysisUpdate, type Candle, type Interval, type MarketSnapshot, type Symbol } from '../schemas/contracts.js';
type CandleState = { candles: Candle[]; lastEvent: number; closedTime: number };
export class MarketStateStore {
  private streams = new Map<string, CandleState>();
  private tickers = new Map<Symbol, MarketSnapshot>();
  private analyses = new Map<string, AnalysisUpdate[]>();
  candles(symbol: Symbol, interval: Interval) { return this.streams.get(streamKey(symbol, interval))?.candles ?? []; }
  snapshot(symbol: Symbol) { return this.tickers.get(symbol); }
  setSnapshot(snapshot: MarketSnapshot) {
    if ((this.tickers.get(snapshot.symbol)?.timestamp ?? 0) > snapshot.timestamp) return false;
    this.tickers.set(snapshot.symbol, snapshot); return true;
  }
  seed(symbol: Symbol, interval: Interval, history: Candle[], requestedAt = 0) {
    const key = streamKey(symbol, interval);
    const state = this.streams.get(key);
    const merged = new Map(history.map(c => [c.time, c]));
    // Correct candles missed during downtime; preserve only newer in-flight live data.
    const historyEnd = history.at(-1)?.time ?? -1;
    for (const candle of state?.candles ?? []) {
      if (candle.time > historyEnd || (candle.time === state?.candles.at(-1)?.time && state.lastEvent >= requestedAt)) merged.set(candle.time, candle);
    }
    this.streams.set(key, { candles: [...merged.values()].sort((a, b) => a.time - b.time).slice(-500), lastEvent: state?.lastEvent ?? 0, closedTime: state?.closedTime ?? -1 });
  }
  update(symbol: Symbol, interval: Interval, candle: Candle, timestamp: number, closed: boolean) {
    const key = streamKey(symbol, interval);
    const state = this.streams.get(key) ?? { candles: [], lastEvent: 0, closedTime: -1 };
    const last = state.candles.at(-1);
    if (timestamp < state.lastEvent || (last && candle.time < last.time) || (candle.time <= state.closedTime && !closed)) return false;
    if (last?.time === candle.time) state.candles[state.candles.length - 1] = candle;
    else { state.candles.push(candle); if (state.candles.length > 500) state.candles.shift(); }
    state.lastEvent = timestamp;
    if (closed) state.closedTime = candle.time;
    this.streams.set(key, state); return true;
  }
  addAnalysis(analysis: AnalysisUpdate) {
    const key = streamKey(analysis.symbol, analysis.interval);
    const history = this.analyses.get(key) ?? [];
    history.push(analysis); if (history.length > 50) history.shift(); this.analyses.set(key, history);
  }
  recentAnalyses(symbol: Symbol, interval: Interval) { return this.analyses.get(streamKey(symbol, interval)) ?? []; }
  remove(symbol: Symbol, interval: Interval) {
    this.streams.delete(streamKey(symbol, interval)); this.analyses.delete(streamKey(symbol, interval));
    if (![...this.streams.keys()].some(key => key.startsWith(`${symbol}:`))) this.tickers.delete(symbol);
  }
}
