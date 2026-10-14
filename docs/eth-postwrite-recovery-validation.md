# AskLiam V6.5.1 — ETH postwrite recovery check

Temporary CI-only validation branch. Production recovery requires the exact-day protected Redis journal, the original Coinbase historical SHA-256 digest, all 299 stored Coinbase USD spot candles, a CRYPTO_V1 snapshot with nine mandatory inputs and an independently authenticated Google Sheets readback. Only then may the global journal transition atomically to an ETH spot verified checkpoint. Neither financial orders nor ranking are authorized by reconciliation.
