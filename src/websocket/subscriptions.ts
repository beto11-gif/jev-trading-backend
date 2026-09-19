import type { FastifyBaseLogger } from 'fastify';
import { streamKey, type Interval, type ServerEvent, type Symbol } from '../schemas/contracts.js';
import type { BinanceRestClient } from '../services/binance/rest.js';
import type { MarketStream, StreamFactory, StreamStatus } from '../services/binance/stream.js';
import type { MarketStateStore } from '../stores/market-state.js';
import type { AnalysisScheduler } from '../services/analysis/scheduler.js';
export interface Subscriber { send(event: ServerEvent): void }
type Entry = { symbol: Symbol; interval: Interval; clients: Set<Subscriber>; stream: MarketStream; status: StreamStatus;
  timer?: NodeJS.Timeout; stopAnalysis: () => void; loading: boolean; ready: boolean; retrySeed?: NodeJS.Timeout };
export class SubscriptionManager {
  private entries = new Map<string, Entry>();
  constructor(private factory: StreamFactory, private rest: BinanceRestClient, private store: MarketStateStore,
    private scheduler: AnalysisScheduler, private log: FastifyBaseLogger, private graceMs = 5000) {}
  statuses() { return [...this.entries.values()].map(e => ({ symbol: e.symbol, interval: e.interval, status: e.status, subscribers: e.clients.size })); }
  private emit(entry: Entry, event: ServerEvent) { for (const client of entry.clients) client.send(event); }
  private seed(entry: Entry) {
    if (entry.loading) return;
    entry.loading = true;
    clearTimeout(entry.retrySeed);
    const requestedAt = Date.now();
    void this.rest.candles(entry.symbol, entry.interval, 500).then(candles => {
      if (this.entries.get(streamKey(entry.symbol, entry.interval)) !== entry) return;
      this.store.seed(entry.symbol, entry.interval, candles, requestedAt); entry.ready = true;
    }).catch(() => {
      if (this.entries.get(streamKey(entry.symbol, entry.interval)) !== entry) return;
      this.log.warn({ event: 'history_unavailable', symbol: entry.symbol, interval: entry.interval });
      this.emit(entry, { type: 'error', code: 'HISTORY_UNAVAILABLE', message: 'Historical candles temporarily unavailable' });
      entry.retrySeed = setTimeout(() => this.seed(entry), 30000);
    }).finally(() => { entry.loading = false; });
  }
  subscribe(client: Subscriber, symbol: Symbol, interval: Interval) {
    const key = streamKey(symbol, interval);
    let entry = this.entries.get(key);
    if (!entry) {
      const created: Entry = { symbol, interval, clients: new Set(), status: 'connecting', stream: { start() {}, stop() {} }, stopAnalysis() {}, loading: false, ready: false };
      created.stream = this.factory(symbol, interval, event => {
        if (event.type === 'ticker') {
          if (this.store.setSnapshot(event.snapshot)) this.emit(created, { type: 'market_update', ...event.snapshot });
        } else if (this.store.update(symbol, interval, event.candle, event.timestamp, event.closed)) {
          this.emit(created, { type: 'candle_update', symbol, interval, candle: event.candle, closed: event.closed });
        }
      }, status => {
        const previous = created.status;
        if (status === previous) return;
        created.status = status;
        this.emit(created, { type: 'connection_status', status, symbol, interval, timestamp: Date.now() });
        if (status === 'connected' && previous === 'reconnecting') { created.ready = false; this.seed(created); }
      });
      created.stopAnalysis = this.scheduler.start(symbol, interval, event => { if (created.ready && created.status === 'connected') this.emit(created, event); });
      this.entries.set(key, created); entry = created;
      entry.clients.add(client); entry.stream.start(); this.seed(entry);
      this.log.info({ event: 'subscription_created', symbol, interval });
    } else { clearTimeout(entry.timer); entry.clients.add(client); }
    client.send({ type: 'connection_status', status: entry.status, symbol, interval, timestamp: Date.now() });
    const ticker = this.store.snapshot(symbol);
    if (ticker && Date.now() - ticker.timestamp < 15000) client.send({ type: 'market_update', ...ticker });
    const latest = this.store.recentAnalyses(symbol, interval).at(-1);
    if (latest) client.send(latest);
  }
  unsubscribe(client: Subscriber, symbol: Symbol, interval: Interval) {
    const entry = this.entries.get(streamKey(symbol, interval));
    if (!entry || !entry.clients.delete(client) || entry.clients.size) return;
    entry.timer = setTimeout(() => this.remove(entry), this.graceMs);
  }
  disconnect(client: Subscriber) { for (const entry of this.entries.values()) this.unsubscribe(client, entry.symbol, entry.interval); }
  private remove(entry: Entry) {
    this.entries.delete(streamKey(entry.symbol, entry.interval));
    clearTimeout(entry.timer); clearTimeout(entry.retrySeed); entry.stopAnalysis(); entry.stream.stop();
    this.store.remove(entry.symbol, entry.interval);
    this.log.info({ event: 'subscription_removed', symbol: entry.symbol, interval: entry.interval });
  }
  close() { for (const entry of this.entries.values()) this.remove(entry); }
}
