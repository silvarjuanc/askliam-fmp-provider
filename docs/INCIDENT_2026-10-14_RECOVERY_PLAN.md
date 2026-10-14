# AskLiam V6.5.1 — Production incident, 2026-10-14

## Verified state (not a success declaration)
- 169 enabled / 159 ranking-eligible; 22 acquired in today's Stage0 snapshot, including 2 Coinbase-USD spot assets.
- 20 FMP acquisitions lack mandatory equity/index analytical fields; BTC/ETH have complete mandatory data but stale FAST_MARKET quotes at the 16:00 Europe/Stockholm analysis.
- No 2026-10-14 global committed ranking / Run_Manifest / lifecycle decision.
- Upstash checkpoint and three Stage0 Sheets writes are **not** interchangeable with analytical completion.
- The live Render cron was scheduled at `0 20,21 * * 1-5` UTC and the Python driver enforced a New-York-local guard. This was wrong for the required Swedish analysis.
- Render logs show repeated deploys/restarts around 12:17–13:29 UTC and repeating startup-triggered Stage0 FMP drain / recovery attempts. HTTP 503 errors at 12:12 and 12:43 UTC may coincide with this churn but exact upstream cause remains unproven. Date-only startup flags did not enforce once-per-day execution; a new explicit `ASKLIAM_STARTUP_ONESHOTS_ENABLED=true` gate, disabled by default, protects against restart-triggered work. Keep the override unset in routine production.
- `stage0_fmp_adapter.py` explicitly avoids profile/fundamentals and does not calculate canonical macro factors. `stage0_ranking_preview.py` is intentionally read-only and publication-blocked. There is no completed Stage1/Stage2 production pipeline in this repo.

## Correct wall-clock contract
- Version remains **V6.5.1**.
- The authoritative analysis start is **16:00 Europe/Stockholm**, fixed by Swedish civil time. The NY cash open may differ around DST changeover; never rebase the Swedish schedule to 16:00 New York.
- Stage0 acquisition windows: **09:00, 12:00, 14:00, 15:00 Europe/Stockholm**. The last phase has a strict 20-minute wall budget. No data acquisition after 15:45 via this driver.
- Update the existing Render cron schedule **manually** to `0 7,8,10,11,12,13,14 * * 1-5` (UTC). The Python guard ignores the extra DST-offset slots. **The GitHub branch alone does not update this Render setting.**
- Do not keep both old and new cron schedules armed. Do not run startup-triggered FMP preclose drains concurrently with this coordinator.

## Critical engineering backlog (separate from the scheduling patch)
1. **Historical read amplification**: `stage0_ingestor._capture()` reads the complete `Raw_OHLCV`, `Market_Data_Snapshot` and `FMP_Runtime_State` tabs for every single-asset transaction. With over 70k grid rows this creates a large Sheets-read workload. Instrument read latency and error class; migrate raw time series to a transactional DB/object store, retaining Sheets as materialized audit/report views. Any optimization must preserve composite UPSERT keys and independent read-back. Never purge historical rows.
2. **Canonical factors**: Build Stage1 from validated historical OHLCV, 253+ sessions and strictly sourced fields. Compute returns/volatility/momentum/trend/relative strength locally; only compute volume_score with verified volumes. Data completeness uses actual Data_Requirements per asset model.
3. **Provider routing**: FMP HTTP 402 is an entitlement failure, not quota exhaustion. Stop 402 retry loops, evaluate licensed official/provider fallbacks and a provider coverage matrix for the 63 non-FMP assets. Never infer unsupported fields or substitute Hyperliquid derivative prices for cash underlying facts.
4. **Fundamental and macro mapping**: Implement validated equity fundamental factors and official macro datasets / model-specific transforms; keep null when unavailable. Equity `fundamental_factor_count>=1`, INDEX breadth and macro requirements must be measured from evidence, not the existence of a price row.
5. **Snapshot freshness**: Cache DAILY_HISTORY and 7d fundamentals across runs. Refresh only FAST_MARKET fields near analysis. Separate a source's `data_timestamp` (market observation) from `fetched_at` (download); enforce TTL against the latter and verify market-session status. Do not force a completed-market daily OHLCV refresh every 15 minutes.
6. **Stage1/Stage2**: Implement read-only computation then transactional `CALCULATE → VALIDATE → COMMIT → READBACK` for Ranking_Staging, Top30/deep, Lifecycle, Report and Run_Manifest; define replay-safe idempotency and dual status `ANALYTICAL_STATUS` / `EXECUTION_STATUS`. Do not publish completion before commit verification.
7. **Hyperliquid safety**: Execute lazy market, L2, collateral, spread, funding, oracle and order sizing gates **after** Lifecycle emits a viable action. No invented orders or synthetic cash Entry/SL/TP reuse.

## Acceptance criteria — no false positives
- Scheduled Stage0 triggers follow Stockholm time through European and US DST changes.
- No concurrent writer / unresolved Redis journal; transient errors do not advance the cursor.
- Full 159-asset status reconciles `SCORED + PROVIDER_DEFERRED + DATA_WAIT + other documented states = 159`, without presenting old run data as current.
- Each SCORED asset has model mandatory fields, freshness and provenance PASS; historical bars are structurally validated and immutable by composite key.
- Global Run_Manifest is COMMITTED only after the actual per-tab readback; Top30 count is min(30, actually SCORED), not populated by legacy scores.
- `trades[]` only includes executable true actions after Lifecycle + Hyperliquid execution gates. With no executable edge, NO_TRADE is a valid successful analysis outcome.
- Reject/redact any leaked credential in logs, Sheets or market source URL.
