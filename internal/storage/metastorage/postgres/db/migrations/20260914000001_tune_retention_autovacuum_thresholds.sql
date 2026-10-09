-- +goose Up
-- INF-1646. Solana single-block retention stopped for 27 hours because
-- autovacuum's own trigger on block_single_block_retention had become
-- unreachable, and the resulting stall was self-locking.
--
-- idx_block_single_block_retention_pending is partial on the pending
-- retirement states. Every block that finishes retirement updates its row out
-- of that predicate, leaving behind an index entry that only VACUUM can
-- remove. The retention probe's two NOT EXISTS clauses plan as Nested Loop
-- Anti Joins whose inner side carries `Index Cond: (tag = N)` alone -- height
-- and block_metadata_id are demoted to a Join Filter -- so that index is
-- rescanned once per candidate row. Measured on solana-mainnet prod at 430,504
-- dead entries and zero live ones:
--
--     Index Only Scan using idx_block_single_block_retention_pending
--         (actual time=0.502..0.502 rows=0 loops=2000)
--         Buffers: shared hit=3060000        -- 1,530 buffers per loop
--
-- At 249,889 candidate rows per probe that is ~125s against a 60s
-- statement_timeout. After VACUUM the same scan costs 1 buffer / 0.003ms and
-- the planner wraps both inners in Materialize (loops=1); the probe went from
-- over 300s to 0.84s. The work was never the problem -- the same window with
-- both NOT EXISTS clauses removed ran in 1.23s throughout.
--
-- The default scale factor is what made this unrecoverable rather than merely
-- slow. Autovacuum wanted 0.1 * 7,147,766 + 50 = 714,827 dead tuples;
-- production froze at 598,776, because retention -- this table's only writer --
-- had already stopped. Stopped retention produces no new dead tuples, so the
-- threshold could never be crossed, so the bloat could never clear, so the
-- probe kept timing out.
--
-- scale_factor 0 makes the threshold absolute and therefore reachable at any
-- table size. 50,000 bounds the worst case: the anti-join costs ~0.117ms per
-- outer row per 100k dead entries, so the pre-vacuum peak is ~15s of a 60s
-- budget. Solana retires enough rows for roughly six vacuums a day, and the
-- one run during the incident took 33.8s.
ALTER TABLE block_single_block_retention
    SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 50000);

-- block_consolidation_shadow is analyzed rather than vacuumed here, for a
-- different reason: height rises monotonically, so a stale histogram's upper
-- bound falls behind the frontier and range estimates over recent heights
-- collapse. At 0.05 * 425M rows the next autoanalyze was ~21.2M modifications
-- away -- about 20 days -- while solana advances ~360k heights a day. During
-- the incident the histogram topped out at 445,768,901 against a true max of
-- 446,851,999, and the probe's outer estimate fell to 1 row against 249,889
-- actual.
--
-- This did not cause the outage, and refreshing it did not change the probe's
-- plan: the residual error is cross-column, since
-- single_block_storage_generation = 'v2' is 1.6% of the table globally but
-- 100% of any recent height range, which a single-column histogram cannot
-- express. It is set anyway because an estimate off by five orders of
-- magnitude is a standing hazard for every other planner decision on this
-- table, and ANALYZE samples 30,000 rows regardless of table size, so the cost
-- does not grow with the table.
ALTER TABLE block_consolidation_shadow
    SET (autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 2000000);

-- +goose Down
ALTER TABLE block_single_block_retention
    RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold);

ALTER TABLE block_consolidation_shadow
    RESET (autovacuum_analyze_scale_factor, autovacuum_analyze_threshold);
