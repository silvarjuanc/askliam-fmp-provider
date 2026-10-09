# AskLiam V6.5.1 — FMP Enrichment Provider

Python HTTP/2 FMP adapter for **Render Free** with **Upstash Redis REST** as the only authoritative persistent state (UTC quota counter, TTL cache, continuation cursor). **No SQLite / persistent disk required.** This adapter does **not** write Google Sheets or trade; separate Stage 0 must validate Data_Requirements, perform verified Sheets UPSERT and only then advance the cursor.

## Deploy

- Render Python web service on branch main (automatic deploy on push).
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Required Render Environment variables (do not paste secrets into GitHub): `FMP_API_KEY`, `ASKLIAM_SERVICE_TOKEN`, `UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`.
- Optional: `FMP_BASE_URL=https://financialmodelingprep.com`. Legacy `ASKLIAM_DATA_DIR` is unused.

## Endpoints

- `GET /health`: public, reports configuration flags only (not live provider validation).
- `GET /quota`: requires `X-Askliam-Token`, reads Upstash authoritative UTC quota.
- `POST /enrich`: requires `X-Askliam-Token`. JSON: `{"symbols":["AAPL","MSFT"],"include_profiles":true,"include_historical":true,"historical_budget":2}`.
- `GET /cursor`: requires token, reads last Stage 0 cursor.
- `PUT /cursor`: requires token, JSON `{"value":"YOUR_VERIFIED_CURSOR"}`; call only after Sheets UPSERT **and** read-back verify.

## Strict constraints

- FMP internal hard cap **240 requests/day UTC** (vs specified provider limit 250) enforced via Redis atomic Lua EVAL before HTTP calls; a failed attempt still consumes a reservation. If Redis fails, no FMP requests are sent.
- FMP HTTP 429 blocks the UTC day in Redis, and blocks the active process immediately. Already-reserved concurrent HTTP requests may be in flight.
- Persistent cache first: quote 15m, profile 7d, historical 24h. A fresh cache hit uses zero FMP calls. Redis cache values include source, fetched_at and sanitized source URL template.
- Quote/profile fetches are batched in chunks of 20 using comma-separated FMP symbol routes; availability depends on your FMP account entitlements. Historical requests are per symbol and explicitly budgeted.
- FMP secrets never appear in output, cache keys, or source URLs. `ACQUIRED_NOT_SCORED` is **not** an AskLiam analytical status; mandatory fields, model coverage, macro scoring and Stage 0 Sheets writing occur downstream.

## Verify after Render deploy

1. `GET /health`: check `redis_configured` and `fmp_configured` booleans.
2. Authorized `GET /quota`: should show the persistent ledger.
3. Use a **small** authorized `POST /enrich` with 1–2 symbols to verify plan access and quota increment.
4. Repeat the request while TTL is valid to verify a cache hit consumes 0 requests.
