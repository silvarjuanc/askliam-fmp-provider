# AskLiam V6.5.1 FMP Enrichment Provider

Python HTTP/2 API for FMP cache-first acquisition, SQLite quota ledger (240 requests per UTC day), historical data and local technical calculations. **It does not write Google Sheets or execute trades.** Integrate `/enrich` output with a separate validated Stage 0 writer.

## Render

- Runtime: Python 3.11+
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Environment: `FMP_API_KEY`, `ASKLIAM_SERVICE_TOKEN`, `ASKLIAM_DATA_DIR=/var/data/askliam`
- Attach a **persistent disk mounted at `/var/data`** or use a persistent external transactional store. A Render ephemeral filesystem is **not sufficient** for the hard-cap guarantee across restarts. The SQLite quota guarantee assumes a single writer deployment sharing the same persistent database; do not scale to multiple independent disks.
- Protect `/enrich` and `/quota` using `X-AskLiam-Token: <ASKLIAM_SERVICE_TOKEN>`.
- `GET /health` reports whether a key is configured but never reveals its value.

## Example

```bash
curl -X POST https://YOUR-SERVICE.onrender.com/enrich \
  -H 'Content-Type: application/json' \
  -H 'X-AskLiam-Token: YOUR_SERVICE_TOKEN' \
  -d '{"symbols":["AAPL","MSFT"],"include_profiles":true,"include_historical":true,"historical_budget":2}'
```

## Boundaries

- Historical data is returned for incremental `Raw_OHLCV` UPSERT by a separate Sheets writer.
- FMP endpoint entitlements and response schemas must be verified on the actual account. HTTP 429 blocks the UTC day.
- `ACQUIRED_NOT_SCORED` is not `SCORED`; canonical Data_Requirements coverage and scoring are downstream.
- Never use this service's quote as a replacement for non-equity official source requirements.
- No API key is embedded in repository code, cache keys, or response bodies.
