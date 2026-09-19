# jev-trading-backend

Backend único de **market data Binance Spot + análise**, em Node.js 24 LTS, TypeScript e Fastify. Estado em memória, sem banco. Frontend Lovable não está neste repositório.

**JEV DISABLED / NOT CONFIGURED.** Não foi fornecida documentação oficial identificável do serviço JEV. Nenhum endpoint/modelo foi inventado. Definir `JEV_API_KEY` e `JEV_MODEL` ainda não ativa a integração. `DisabledJevAnalyzer` nunca produz sinais simulados. Substitua esse adapter quando houver documentação confirmada.

## Executar

```powershell
npm install
Copy-Item .env.example .env
npm run dev
```

Requer Node 24.x e acesso de rede aos endpoints públicos da Binance. `.env` é opcional em desenvolvimento; os padrões já funcionam. Nunca versionar secrets.

```powershell
npm run typecheck
npm run lint
npm test
npm run build
npm start
```

Com o servidor rodando, `npm run smoke` verifica health, 10 candles reais e os dois eventos reais por WebSocket. Não usa mocks nem substitui dados indisponíveis. `SMOKE_URL` permite outro endereço. Os testes de unidade/integração usam fixtures e não dependem da rede.

## Configuração

| Variável | Padrão / uso |
|---|---|
| `PORT` | `3000`, bind `0.0.0.0` |
| `NODE_ENV` | `development`, `test` ou `production` |
| `BINANCE_REST_URL` | `https://data-api.binance.vision` |
| `BINANCE_WS_URL` | `wss://data-stream.binance.vision/ws`; o serviço usa `/stream` combinado nesse host |
| `ALLOWED_ORIGINS` | `http://localhost:5173,http://localhost:3000`; lista separada por vírgula |
| `LOG_LEVEL` | `info` |
| `JEV_API_KEY`, `JEV_MODEL` | opcionais, reservadas, integração desativada |
| `ANALYSIS_1M_MS` | `10000` |
| `ANALYSIS_5M_MS` | `20000` |
| `ANALYSIS_15M_MS` | `30000` |
| `ANALYSIS_1H_MS` | `60000` |
| `ANALYSIS_4H_MS` | `120000` |

Em produção, `ALLOWED_ORIGINS` é obrigatória e deve conter as origens exatas do Lovable/domínio publicado. Não aceita `*`, caminhos ou barra final. Requisições sem Origin são aceitas para clientes de servidor e health checks; CORS não é autenticação. Rate limit REST: 60/minuto por endereço de conexão. Proxy headers não são confiados automaticamente: no Railway, clientes podem compartilhar o limite do proxy; ajustar somente após confirmar a topologia de proxies.

## HTTP

- `GET http://localhost:3000/api/health`: saúde do processo, uptime, JEV e status conhecido dos streams; não consulta provedores. Lista Binance vazia significa nenhum stream ativo.
- `GET http://localhost:3000/api/candles?symbol=BTCUSDT&interval=1m&limit=10`: candles normalizados; limite padrão/máximo 500.

Símbolos: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`. Intervalos: `1m`, `5m`, `15m`, `1h`, `4h`. Inputs inválidos retornam 400; indisponibilidade externa retorna 502/503. Cache REST de 2 segundos, requisições concorrentes agrupadas por par/intervalo e cooldown ao receber 418/429.

## WebSocket

Endpoint: `ws://localhost:3000/ws` (`wss://` em produção). Exemplo no console de uma página cuja origem esteja permitida:

```javascript
const ws = new WebSocket('ws://localhost:3000/ws');
ws.onopen = () => ws.send(JSON.stringify({
  type: 'subscribe', symbol: 'BTCUSDT', interval: '1m'
}));
ws.onmessage = event => console.log(JSON.parse(event.data));
// Para sair ou trocar de timeframe:
ws.send(JSON.stringify({ type: 'unsubscribe', symbol: 'BTCUSDT', interval: '1m' }));
```

Execute a última linha apenas quando quiser cancelar uma subscription, após a conexão abrir. O frontend deve buscar o histórico HTTP e aplicar atualizações por `candle.time`, sem duplicar timestamps. Ao reconectar deve renovar subscriptions e buscar o histórico para preencher lacunas. Não marcar LIVE só pela conexão local: aguardar `connection_status` com symbol/interval e dados recentes.

Contratos e schemas completos em `src/schemas/contracts.ts`:

- `market_update`: `symbol`, `price`, `change24h` (percentual), `high24h`, `low24h`, `volume24h` (**volume de cotação, USDT**), `timestamp` (ms).
- `candle_update`: `symbol`, `interval`, `candle: { time, open, high, low, close, volume }`, `closed`. `time` em segundos UTC; `volume` na moeda base. Candle aberto é atualizado no mesmo timestamp; novo candle entra somente quando recebido.
- `connection_status`: `status`, `timestamp`, `symbol?`, `interval?`. Sem par identifica a conexão local; com par identifica o upstream. Estados: connecting/connected/reconnecting/disconnected.
- `analysis_update`: signal, buy/sell/wait, confidence, summary, model, symbol, interval, timestamp. **Não é emitido enquanto JEV estiver desativado.** Scores somam 100 e não representam garantia de acerto.
- `error`: `code`, `message`. `ANALYSIS_UNAVAILABLE` não interrompe mercado; outros exemplos: INVALID_MESSAGE, HISTORY_UNAVAILABLE.

Máximo 8 KiB/mensagem, 30 mensagens por 10s por conexão, heartbeat, limite de 1000 clientes e desconexão de consumidores lentos. Whitelists limitam a 20 pares de symbol/interval. Não há autenticação nesta fase.

## Arquitetura

REST/WS Binance → normalização → store → subscriptions → WS frontend. `MarketProcessor` gera contexto de candles fechados para `AnalysisScheduler` → `JevAnalyzer` → validação/normalização → histórico/análise.

- Uma conexão Binance combinada ticker+kline por par/intervalo, compartilhada entre navegadores. Diferentes intervalos podem duplicar ticker do mesmo símbolo (limite 20 conexões).
- Grace period de 5s após último subscriber; reconexão 1/2/5/10/30s com jitter, watchdog e tratamento da desconexão periódica da Binance. Ao reconectar, histórico é recarregado; falha de histórico tenta novamente após 30s.
- Até 500 candles e 50 análises por par; tudo se perde no restart. EMA20/50, RSI14/ATR14 Wilder e volume relativo calculados apenas em análises programadas, sobre candles fechados; mínimo 50 candles e rejeição de lacunas/dados obsoletos. Estrutura compara máximas/mínimas dos dois últimos candles, não pivôs sofisticados.
- Analyzer deve respeitar `AbortSignal` (timeout de 8s); chamadas não se sobrepõem por stream. Falhas não interrompem market data. Sem análise real, scheduler permanece inativo.
- Logs estruturados sem payload de secrets ou spam por tick. SIGINT/SIGTERM fecham clientes, streams e timers; erros fatais encerram com código não zero.

## Railway

Conecte este repositório ao Railway quando estiver no GitHub. `railway.json` define build `npm ci && npm run build`, start `npm start` e health `/api/health`. Use Node 24 (engines/.nvmrc), `NODE_ENV=production` e `ALLOWED_ORIGINS` explícita. Railway fornece PORT. Não precisa Dockerfile. Nenhum deploy ou push foi executado nesta implementação.

Uma única réplica é suficiente nesta fase: estado, subscriptions e limites são locais ao processo. Restrições regionais da Binance podem impedir REST/WS no local de deploy; validar com smoke nessa região. Health do processo permanece OK em falhas externas; o status do upstream é informado separadamente.

Referências oficiais consultadas: [Binance market data público](https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md), [streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md), [REST](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md), [Railway health checks](https://docs.railway.com/deployments/healthchecks).
