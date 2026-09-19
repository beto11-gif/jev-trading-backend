import type { FastifyBaseLogger } from 'fastify';
import { normalizeAnalysis, type Interval, type ServerEvent, type Symbol } from '../../schemas/contracts.js';
import type { MarketStateStore } from '../../stores/market-state.js';
import type { JevAnalyzer } from '../jev/analyzer.js';
import type { MarketProcessor } from '../market/processor.js';
export class AnalysisScheduler {
  constructor(private analyzer: JevAnalyzer, private processor: MarketProcessor, private store: MarketStateStore,
    private periods: Record<Interval, number>, private log: FastifyBaseLogger) {}
  start(symbol: Symbol, interval: Interval, emit: (event: ServerEvent) => void) {
    if (!this.analyzer.enabled) return () => {};
    let busy = false, stopped = false;
    let controller: AbortController | undefined;
    const timer = setInterval(() => {
      if (busy || stopped) return;
      const context = this.processor.context(symbol, interval);
      if (!context) return;
      busy = true;
      controller = new AbortController();
      const timeout = setTimeout(() => controller?.abort(), 8000);
      this.log.info({ event: 'analysis_started', symbol, interval });
      const signal = controller.signal;
      void Promise.resolve().then(() => this.analyzer.analyze(context, signal)).then(raw => {
        if (stopped || controller?.signal.aborted) return;
        const result = normalizeAnalysis(raw);
        const event = { ...result, type: 'analysis_update' as const, symbol, interval, timestamp: Date.now() };
        this.store.addAnalysis(event); emit(event);
        this.log.info({ event: 'analysis_completed', symbol, interval });
      }).catch(() => {
        this.log.warn({ event: 'analysis_failed', symbol, interval });
        if (!stopped) emit({ type: 'error', code: 'ANALYSIS_UNAVAILABLE', message: 'Analysis temporarily unavailable' });
      }).finally(() => { clearTimeout(timeout); busy = false; });
    }, this.periods[interval]);
    return () => { stopped = true; clearInterval(timer); controller?.abort(); };
  }
}
