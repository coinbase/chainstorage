-- +goose Up
-- INF-2023. robinhood-mainnet auto consolidation wedged on 2026-10-08: the
-- promotion statement drove its join from idx_block_metadata_unconsolidated
-- with no index condition, so every attempt scanned every unpromoted
-- block_metadata row and hit the 60s statement_timeout, and each failure left
-- a larger backlog for the next one. The queries now bound block_metadata by
-- height (promotionInvalidShadowQuery in block_storage.go), which fixes the
-- wedge without this migration.
--
-- This migration keeps block_metadata's statistics current in every chain
-- database. The planner estimated that index at one row because the last
-- autoanalyze sampled none of the thin, clustered sliver of unpromoted rows
-- and recorded null_frac(byte_length) = 0, and at the cluster's 5% analyze
-- scale factor the next autoanalyze was days away on robinhood (83.8M rows,
-- ~70,000 inserted and promoted rows an hour) and weeks away on solana. A
-- fresh ANALYZE cannot prevent a missed sliver; it ends the stale estimate
-- once a backlog is big enough to sample.
--
-- An absolute threshold, as INF-1646 chose for the retention tables, because a
-- scale factor still leaves the largest chain days behind: 250,000 changed
-- rows is about 3.5 hours on robinhood, 9 on solana and 18 on arc or tempo.
-- ANALYZE samples 30,000 rows regardless of table size. Lowering the threshold
-- makes autovacuum analyze any table already past it on its next pass, so no
-- ANALYZE runs here, and autovacuum's vacuum thresholds are left alone.
--
-- ALTER TABLE ... SET takes SHARE UPDATE EXCLUSIVE, which an anti-wraparound
-- autovacuum holds without yielding. The lock timeout fails this migration in
-- 30s rather than holding the release's pre-upgrade Job for its full deadline;
-- re-run the deploy to retry.
SET LOCAL lock_timeout = '30s';

ALTER TABLE block_metadata
    SET (autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 250000);

-- +goose Down
SET LOCAL lock_timeout = '30s';

ALTER TABLE block_metadata
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
