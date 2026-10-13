# V6.5.1 — Phase 2/3 implementation status

**Scope:** branch-level, safety-first modules. Does not yet unlock production
BROAD_SCAN_READY or enable lifecycle/trading. Canonical authorities:
Drive Master Prompt (updated Oct 11, 2026) and Data_Requirements /
Data_Source_Map / Cache_Policy from the Master Portfolio Engine.

## Implemented in this branch

- Validated CoinGecko independent daily USD spot price/volume samples
  (explicit coin IDs; 253 minimum observations; no interpolation, no OHLC
  fabrication; fail closed when source does not supply 365d history).
- Independent CryptoCompare CCCAGG daily aggregated spot OHLCV probe,
  requesting 300 daily history, USD direct quote, no BTC pair conversion;
  reject gaps, incomplete current day and mathematically invalid candles.
- Canonical close-based factors: returns expressed in percentage points,
  annualized log-return volatility, MA20/50/200 distances, momentum_score,
  trend_score, relative strength against an independently validated benchmark,
  explicit source-quality guard for volume_score, normalized score less risk.
  Source: canonical Master Prompt section DETERMINISTIC OHLCV CALCULATIONS
  and AVAILABLE-WEIGHT NORMALIZATION.
- NY cash-session gate for FMP history; excludes partial current-session
  daily volume bars before they enter the planner.
- Generic Data_Requirements mandatory-input diagnostic for all score
  models. Must never equate mere factor presence with quality or freshness.
- Read-only Broad Scan calculation from a frozen/explicit Universe,
  snapshot for the same run, required factors and evidence; stable sort
  with Top10/Top30 tiers. **Publication deliberately blocked.**
- Pure Run_Manifest validator: prevents falsely claiming a complete run
  without independent read-back and coherent counts.

## Still needed before V6.5.1 is operational end-to-end

1. Validate historical spot provider entitlements and responses live for
   all eight cryptos. CoinGecko Demo API key may be required:
   store COINGECKO_DEMO_API_KEY in Render, not source control. Test and
   select the independent canonical provider with adequate coverage.
2. Design transactional, audited, cursor-based spot history writer with
   correct source-specific UPSERT key and snapshot collision semantics.
   Stage0 crypto perpetual snapshot must not overwrite independent spot.
3. Integrate verified underlying spot + validated derivatives OI/funding
   (15m freshness) into one canonical CRYPTO_V1 snapshot. Enforce both
   mandatory factor coverage and freshness; promote only after readback.
4. Provide licensed and compliant equity quotes/history/fundamentals for
   all FMP_HTTP_402 candidates; avoid unproductive FMP 402 retries.
5. Implement the remaining 63 non-FMP provider routes including FX,
   commodities, official rates/macro and alternatives; macro scores need
   the approved source model, never assumed from market prices.
6. Versioned Stage1 analytical pipeline with complete Ranking_Staging
   snapshot, score calibration, deep-dive Top30, quality and Lifecycle,
   as specified by the canonical Master Prompt. Full
   CALCULATE->VALIDATE->COMMIT->READBACK->Run_Manifest + rollback.
7. Run an end-to-end controlled dry-run (without trading), obtain nonzero
   BROAD_SCAN_READY with verified source evidence, and preserve the
   ANALYTICAL_STATUS / EXECUTION_STATUS separation.
8. Only after full analytical acceptance, evaluate lazy Hyperliquid
   execution for genuine actionable lifecycle signals. No automatic
   orders introduced by this branch.

## Data integrity decisions

CoinGecko daily market_chart price samples are not actual daily OHLCV.
Do not create fabricated OHLC fields from them. CryptoCompare CCCAGG is
an independent aggregated spot source, but availability, exact trading-pair
route and permissions must be validated before production writes.

The existing 150 native Hyperliquid perp daily bars remain separately
identified as derivative context. A SUCCESSFUL cron run is not a successful
analytical run.

## Release gates

CI must pass. Both new preview endpoints are authenticated, read-only
and explicitly block ranking. Never merge or activate writes solely to
increase the SCORED count. Score only complete, validated inputs.
