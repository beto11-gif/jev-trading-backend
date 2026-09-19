import WebSocket from 'ws';
import type { FastifyBaseLogger } from 'fastify';
import type { Interval, Symbol } from '../../schemas/contracts.js';
import { normalizeStream, type MarketEvent } from './normalization.js';
export type StreamStatus = 'connecting' | 'connected' | 'reconnecting' | 'disconnected';
export interface MarketStream { start(): void; stop(): void }
export type StreamFactory = (symbol: Symbol, interval: Interval, event: (event: MarketEvent) => void, status: (status: StreamStatus) => void) => MarketStream;

// One combined connection per logical pair, shared by every browser watching it.
export class BinanceMarketStream implements MarketStream {
  private socket?: WebSocket;
  private retry?: NodeJS.Timeout;
  private watchdog?: NodeJS.Timeout;
  private stopped = true;
  private attempt = 0;
  private lastSeen = 0;
  constructor(private url: string, private symbol: Symbol, private interval: Interval,
    private event: (event: MarketEvent) => void, private status: (status: StreamStatus) => void,
    private log: FastifyBaseLogger,
    private openSocket: (url: URL) => WebSocket = url => new WebSocket(url, { handshakeTimeout: 10000, maxPayload: 65536 })) {}
  start() { if (!this.stopped) return; this.stopped = false; this.connect(); }
  private connect() {
    if (this.stopped) return;
    this.status(this.attempt ? 'reconnecting' : 'connecting');
    const url = new URL(this.url);
    url.pathname = '/stream';
    url.searchParams.set('streams', `${this.symbol.toLowerCase()}@ticker/${this.symbol.toLowerCase()}@kline_${this.interval}`);
    const socket = this.socket = this.openSocket(url);
    this.lastSeen = Date.now();
    this.watchdog = setInterval(() => { if (Date.now() - this.lastSeen > 60000) socket.terminate(); }, 15000);
    socket.on('ping', () => { this.lastSeen = Date.now(); }); // ws automatically pongs with the original payload.
    socket.on('open', () => { this.log.info({ event: 'binance_connected', symbol: this.symbol, interval: this.interval }); });
    socket.on('message', data => {
      this.lastSeen = Date.now();
      try {
        const envelope: unknown = JSON.parse(data.toString());
        const raw = typeof envelope === 'object' && envelope !== null && 'data' in envelope ? envelope.data : envelope;
        if (typeof raw === 'object' && raw !== null && 'e' in raw && raw.e === 'serverShutdown') { socket.close(); return; }
        const event = normalizeStream(raw);
        if ((event.type === 'ticker' ? event.snapshot.symbol : event.symbol) !== this.symbol) return;
        if (event.type === 'candle' && event.interval !== this.interval) return;
        this.attempt = 0;
        this.status('connected');
        this.event(event);
      } catch { this.log.warn({ event: 'binance_invalid_message', symbol: this.symbol }); }
    });
    socket.on('error', () => { this.log.warn({ event: 'binance_socket_error', symbol: this.symbol }); });
    socket.on('close', () => {
      clearInterval(this.watchdog);
      this.log.info({ event: 'binance_disconnected', symbol: this.symbol });
      if (this.stopped) return;
      this.status('reconnecting');
      const backoff = [1000, 2000, 5000, 10000, 30000];
      const delay = Math.min(30000, backoff[Math.min(this.attempt++, 4)]! * (1 + Math.random() * 0.2));
      this.log.info({ event: 'binance_reconnecting', delay, symbol: this.symbol });
      this.retry = setTimeout(() => this.connect(), delay);
    });
  }
  stop() { this.stopped = true; clearTimeout(this.retry); clearInterval(this.watchdog); this.socket?.terminate(); this.status('disconnected'); }
}
