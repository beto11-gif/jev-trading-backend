import { z } from 'zod';
const secureUrl = (protocol: string) => z.url().refine(value => {
  const url = new URL(value);
  return url.protocol === protocol && !url.username && !url.password && !url.search && !url.hash;
}, `Expected ${protocol} URL without credentials/query/hash`);
const duration = (fallback: number) => z.coerce.number().int().min(1000).max(3600000).default(fallback);
const envSchema = z.object({
  PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  BINANCE_REST_URL: secureUrl('https:').default('https://data-api.binance.vision'),
  BINANCE_WS_URL: secureUrl('wss:').default('wss://data-stream.binance.vision/ws'),
  ALLOWED_ORIGINS: z.string().default('http://localhost:5173,http://localhost:3000'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  JEV_API_KEY: z.string().optional(), JEV_MODEL: z.string().optional(),
  ANALYSIS_1M_MS: duration(10000), ANALYSIS_5M_MS: duration(20000), ANALYSIS_15M_MS: duration(30000),
  ANALYSIS_1H_MS: duration(60000), ANALYSIS_4H_MS: duration(120000),
});
export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const value = envSchema.parse(env);
  if (value.NODE_ENV === 'production' && !env.ALLOWED_ORIGINS?.trim()) throw new Error('Production requires ALLOWED_ORIGINS');
  const origins = value.ALLOWED_ORIGINS.split(',').map(s => s.trim()).filter(Boolean);
  if (!origins.length || origins.some(origin => {
    try { const url = new URL(origin); return !['http:', 'https:'].includes(url.protocol) || url.origin !== origin; } catch { return true; }
  })) throw new Error('ALLOWED_ORIGINS must contain explicit HTTP origins');
  return { ...value, origins, analysisPeriods: { '1m': value.ANALYSIS_1M_MS, '5m': value.ANALYSIS_5M_MS, '15m': value.ANALYSIS_15M_MS, '1h': value.ANALYSIS_1H_MS, '4h': value.ANALYSIS_4H_MS } };
}
export type Config = ReturnType<typeof loadConfig>;
