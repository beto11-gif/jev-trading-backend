import 'dotenv/config';
import { loadConfig } from './config/env.js';
import { buildApp } from './app/app.js';

async function main() {
  let config;
  try { config = loadConfig(); } catch { console.error('Invalid environment configuration. Check .env.example; values omitted for security.'); process.exitCode = 1; return; }
  const app = await buildApp(config);
  let stopping = false;
  const shutdown = async (fatal: boolean) => {
    if (stopping) return;
    stopping = true;
    app.log.info({ event: 'server_stopping', fatal });
    const deadline = setTimeout(() => process.exit(1), 10000); deadline.unref();
    try { await app.close(); process.exitCode = fatal ? 1 : 0; } catch { process.exitCode = 1; }
    finally { clearTimeout(deadline); }
  };
  process.once('SIGINT', () => { void shutdown(false); });
  process.once('SIGTERM', () => { void shutdown(false); });
  process.once('uncaughtException', () => { app.log.fatal({ event: 'uncaught_exception' }); void shutdown(true); });
  process.once('unhandledRejection', () => { app.log.fatal({ event: 'unhandled_rejection' }); void shutdown(true); });
  try { await app.listen({ port: config.PORT, host: '0.0.0.0' }); app.log.info({ event: 'server_started', port: config.PORT, jev: 'DISABLED / NOT CONFIGURED' }); }
  catch { app.log.fatal({ event: 'startup_failed' }); await shutdown(true); }
}
void main().catch(() => { console.error('Server initialization failed'); process.exitCode = 1; });
