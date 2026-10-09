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


## Stage 0 — Google Sheets persistence (V6.5.1)

The new `stage0_ingestor.py` implements authenticated, sparse UPSERT (changed
cells only; append new composite keys only) and a **separate authenticated
read-back** of exactly these three written tabs:

- `Raw_OHLCV`: `canonical_symbol + trading_date + source`
- `Market_Data_Snapshot`: `ranking_run_id + canonical_symbol`
- `FMP_Runtime_State`: `Setting`

Only these three tabs are modified. `Universe` and existing snapshots are
read-only inputs to the FMP adapter. No `.clear()`, full-sheet replacement,
automatic fake rollback, or alteration to the FMP provider's internals.

The protected `POST /stage0/run` endpoint is **disabled by default**.
To enable, set these Render environment variables (secret values only in
Render, **not** in GitHub or chat):

```text
ASKLIAM_SPREADSHEET_ID=13mIMIafc3AYSRBMApCKjpMKq5DIQFZhXEs7ooxSepsw
GOOGLE_SERVICE_ACCOUNT_JSON=<service-account JSON or base64(JSON)>
ASKLIAM_STAGE0_ENABLED=true
```

The service account's `client_email` must be shared as an **Editor** on the
master workbook. Never paste the private key into chat, README or logs.
Existing `ASKLIAM_SERVICE_TOKEN` protects invocation. `ASKLIAM_PROVIDER_URL`
is optional and defaults to this Render service's HTTPS URL.

Small smoke run (requires configured credentials and a completed deployment):

```powershell
$payload = @{max_symbols=1; historical_budget=1} | ConvertTo-Json
Invoke-RestMethod -Method Post `
    -Uri "https://askliam-fmp-provider.onrender.com/stage0/run" `
    -Headers $headers -ContentType "application/json" -Body $payload
```

The endpoint responds `COMMITTED`/`PASS` only if all three sheets match
the calculated target after independent read-back. Do not run Ranking unless
both gates pass. `PARTIAL_COMMIT_UNVERIFIED` or `VERIFICATION_FAILED`
require reconciliation: the Google Sheets API does not offer cross-tab
atomic rollback.

**Current scope:** A deliberately bounded, equity-focused FMP adapter. Other
providers can be injected as DataFrame-returning callables without modifying
their implementation. The canonical macro/model-scoring transformations,
159-asset breadth across asset classes, distributed locking, and the
verified continuation-cursor handshake must be completed before enabling a
full daily production run. This route does **not** trigger Ranking, Trades,
Lifecycle or a Run_Manifest commit.
