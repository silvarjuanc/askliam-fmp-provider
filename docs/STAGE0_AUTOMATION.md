# AskLiam V6.5.1 — unattended daily Stage0 (PROPOSED / disabled by default)

This release does **not** enable autonomous market data writes merely by deploying
code. Stage0 is the only scope: it never scores assets, touches Lifecycle, places
trades, or converts perpetual/futures data into independent spot quotes.

## Execution contract

**Single production writer: Render Cron** in the Frankfurt region.

- Cron UTC expression: `0 20,21 * * 1-5`.
- The Python scheduler checks `America/New_York` and executes only at
  16:00–16:44 local New York time on weekdays, avoiding a double-run across
  summer/winter time changes.
- Cron source: `https://github.com/silvarjuanc/askliam-fmp-provider`,
  branch `main`, Python, `pip install -r requirements.txt`.
- Command: `python scripts/stage0_daily_scheduler.py`.
- Recommended Render region: Frankfurt.
- New Render Cron services can be billable. Confirm the selected plan and cost
  before creation; do not create multiple cron instances.
- Cron service requires only one manually provisioned **secret**
  `ASKLIAM_SERVICE_TOKEN` matching the existing web service's token, stored
  in Render environment settings. Never commit, print, or send this value in chat.
  `ASKLIAM_BASE_URL` is optional and accepts only the trusted default.
- Cron is *not fully operational* until all required configuration is set.

## Web service opt-ins (set ONCE, after the supervised bootstrap has ended)

1. Verify `SUI`/`XRP` imports and all historical keys in Sheets.
2. **Close manual crypto writes**: `ASKLIAM_CRYPTO_WRITE_ENABLED=false`.
3. Keep the manual scope variables harmless: they never authorize auto mode.
4. Add `ASKLIAM_STAGE0_AUTO_ENABLED=true` and
   `ASKLIAM_CRYPTO_AUTO_ENABLED=true` to the **web service only**. Both
   default to false in code. Keep existing `ASKLIAM_STAGE0_ENABLED=true`
   for FMP and working Google Sheets / Upstash credentials.
5. Confirm **Live** deploy and authenticated GET `/stage0/auto/config`:
   all 5 required readiness booleans true, manual crypto write false.
6. Add the cron service's token secret and deploy. A schedule trial at the
   next 16:00 NY ET must show an actual verified checkpoint. Do not claim
   unattended success until that independent check has passed.

## Pipeline

1. Require correct New York weekday and local hour; verify Redis pending
   reconciliation is empty and all opt-in flags are on.
2. Read the current crypto UTC checkpoint. For ADA, BTC, DOGE, ETH, LINK, SOL,
   SUI, XRP, skip already verified symbols. Acquire current native Hyperliquid
   daily perp bars, funding and context. For each missing symbol,
   server-side guarded `POST /stage0/auto/crypto-step` writes strictly via
   the existing `Stage0_Daily_Ingestor`, which captures real workbook state,
   plans keyed UPSERTs, commits, independently reads back, then atomically
   advances the **crypto-specific** Redis checkpoint.
3. A provider deferral detected before Sheets writing is recorded and skipped;
   any uncertain Sheet result, pending journal, Redis CAS failure, or
   checkpoint mismatch **halts**. Never retry an uncertain commit.
4. Run FMP `POST /stage0/daily/step` repeatedly, following the dynamic
   Universe-derived plan until no candidates, 40 quota requests reserved,
   FMP provider stop, or elapsed runtime budget. No fixed 40-symbol batch.
   Existing provider 402 cooldowns are respected. Read the verified
   FMP checkpoint after every successful step.
5. Machine-readable log summary of verified, skipped, deferred and errors
   appears in Render job logs. The job fails nonzero on blocking errors.
   Render can send its standard failure notification.
6. Ranking, trading and Lifecycle remain disabled in the scheduler and in
   server response contracts, including after a successful Stage0 ingest.

## History and model readiness limitations

Hyperliquid's first-pass native perpetual history currently spans **150**
complete daily sessions per request. Day-to-day `Raw_OHLCV` is keyed
`canonical_symbol|trading_date|source`; the existing planner does not
rewrite unchanged OHLCV bars merely because the run/fetch timestamp changed.
It inserts new days and updates genuinely corrected observations.

This is **not** 252–300 independently sourced underlying spot sessions.
The `CRYPTO_V1` mandatory fields are still incomplete. Do not rank or
trade from these derivative-only snapshots, do not invent 252-day indicators,
and do not treat a successful Stage0 commit as a production trading go-live.

Unsupported FX, commodity and other provider routes do not have adapters
in this PR. They remain explicitly pending, never silently treated as
complete.

## Safety, recovery and rollout

- Protected by `X-Askliam-Token`, server-side automation flags, Redis
  per-symbol checkpoints, the cross-provider exclusive lease, global
  pending-reconciliation journal, and readback/atomic CAS.
- Manual `POST /stage0/crypto/commit-one` keeps its separate original
  symbol-scoped and UTC-date guards; automation never toggles those.
- For a retryable pre-write failure, allow the next scheduled run/check.
  For an uncertain write/reconciliation journal, stop and inspect:
  `GET /stage0/reconciliation/inspect`. Do not reset Redis or retry POST.
- Endpoints `GET /stage0/auto/config` and
  `POST /stage0/auto/crypto-step` are authenticated and fail closed.
- There must be **only one enabled external schedule**. The existing
  ChatGPT AskLiam Daily Run can deliver reports, but must not separately
  perform conflicting write operations at the same time.
- No explicit `Run_Manifest` worksheet changes are included; this initial
  production cron release provides verified per-provider Redis checkpoints
  and machine-readable job logs. Full engine manifest and mandatory model
  coverage remain separate release gates.
