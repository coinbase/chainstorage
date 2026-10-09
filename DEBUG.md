# Debug: Solana p50 historical backfill exhausts retries on the CSCB repair tag lock

**Date:** 2026-08-29
**Reporter:** INF-1444 / Boyang Xu
**Environment:** production evidence, local experiment
**Method:** debug-hypothesis

## Scope

**Services involved:**

- Chainstorage `batch_consolidator` workflow and activity — runs 50 independent historical height windows.
- PostgreSQL metadata storage — persists block metadata and consolidation shadows, then promotes validated shadows.
- CSCB repair/rehome fencing — excludes placement writers during repair transitions.
- Monorepo Solana Helm config and Chronosphere monitors — supply retries and failure notifications.

**Dependencies confirmed with user:** yes; the request covers the Solana historical backfill, retry mitigation, and notifications.

## Observations

- r1 failed after the durable boundary at height 172,400,000.
- r2 resumed at 172,400,000, reached the durable boundary at 195,400,000, and then failed.
- Three r2 activities exhausted all three attempts with `pq: canceling statement due to statement timeout` while acquiring `cscb_repair_tag/2`.
- The terminal failed window was `[195458000,195459000)`; the other terminal failures occurred at the same time and lock stage.
- All 50 production backfill workers were ready, on the same image, and had zero restarts when r3 was launched.
- `PersistBlockMetas`, `PersistBlockConsolidationShadows`, and `PromoteBlockConsolidationShadows` take the same exclusive per-tag transaction advisory lock as repair and generation rehome.
- Historical activities operate on disjoint 1,000-height windows but share tag 2.
- The deployed workflow policy has a 2-hour schedule-to-close timeout, 1-hour start-to-close timeout, 10-second exponential backoff capped at 3 minutes, and 3 maximum attempts.
- The existing `chainstorage_temporal_workflow_failed` monitor groups by environment, network, and workflow type. It routes CSCB failures as Slack warnings, but cannot distinguish `historical_backfill` from other batch-consolidator modes.
- r3 was started at the exact durable boundary `[195400000,360000000)`; no range was skipped.

## Hypotheses

### H1: Exclusive tag-wide writer locks serialize p50 activities (ROOT HYPOTHESIS)

- **Supports:** all 50 activities use tag 2; every normal placement writer takes an exclusive tag lock; the exact failure is a statement timeout acquiring that lock; multiple activities exhausted retries together; workers remained healthy.
- **Conflicts:** earlier windows and the start of r3 can complete without retries, so contention is workload-sensitive rather than deterministic on every window.
- **Test:** in isolated PostgreSQL, hold one exclusive transaction lock and prove a second same-key exclusive request times out; repeat with two shared requests and prove both coexist.

### H2: Aurora connection or compute capacity is exhausted

- **Supports:** p50 creates substantially more concurrent database demand than the earlier p10/p20 runs.
- **Conflicts:** the terminal error names one advisory-lock acquisition, not pool exhaustion, connection refusal, or resource exhaustion; all workers remained ready and connected.
- **Test:** compare the failure type and stage with connection-pool errors and worker health; reject if the only terminal failures are same-key lock waits.

### H3: S3 upload or CSCB encoding is overrunning activity deadlines

- **Supports:** Solana objects are large and encoding/uploading dominate normal activity duration.
- **Conflicts:** heartbeats were current, the activities reached the PostgreSQL persistence stage, and the terminal error is a database statement timeout rather than an activity, heartbeat, or S3 timeout.
- **Test:** inspect terminal failure chains and heartbeat stages; reject if all terminal causes are repair-tag lock acquisition.

### H4: A concurrent repair or generation-rehome workflow held the exclusive fence

- **Supports:** repair and rehome intentionally use the same exclusive tag lock.
- **Conflicts:** the one-time rehome had completed, and no repair/rehome workflow was active when the historical run failed; ordinary writers also conflict with one another under the current exclusive API.
- **Test:** correlate open workflow types at failure time and reproduce same-key contention with ordinary exclusive holders only.

## Experiments

### E1: PostgreSQL advisory-lock conflict matrix

- **Setup:** isolated local PostgreSQL 13 container; no application source change and no production connection.
- **Prediction:** same-key exclusive transaction locks conflict and hit a bounded statement timeout, while same-key shared transaction locks coexist.
- **Result:** confirmed. A second `pg_advisory_xact_lock(42)` failed with `canceling statement due to statement timeout` while the first transaction held the lock. A second `pg_advisory_xact_lock_shared(42)` acquired immediately while another transaction held the shared lock.

### E2: Production failure-stage discrimination

- **Setup:** compare the r2 terminal failure chains, worker health, and activity heartbeats with the four hypotheses.
- **Prediction:** H1 requires same-key advisory-lock failures with healthy workers; H2/H3 require connection, resource, activity, heartbeat, or S3 errors; H4 requires an overlapping exclusive repair/rehome transition.
- **Result:** H1 confirmed. The terminal failures were same-key advisory-lock statement timeouts, all 50 workers were ready with zero restarts, heartbeats reached the CSCB persistence path, and no repair/rehome workflow overlapped the failure.

## Root Cause

Ordinary tag-2 metadata and consolidation writers use the repair fence's exclusive transaction advisory lock, so p50 disjoint-height activities serialize on one tag-wide lock and can exhaust the 30-second PostgreSQL statement timeout in synchronized retry waves.

## Fix

- Add shared transaction advisory-lock APIs for ordinary placement writers.
- Keep repair and generation rehome on the existing exclusive API.
- Prove shared/shared compatibility and shared/exclusive exclusion in PostgreSQL integration coverage.
- Add the effective consolidation mode to workflow metrics.
- Raise the Solana deployment's activity attempts from 3 to 8 as a bounded stopgap and add a focused alert for terminal Solana batch-consolidator failures. The current Temporal lifecycle metric has no request-mode label, so the alert intentionally covers both `historical_backfill` and `auto_consolidate`.
- Emit a completion counter only when the batch consolidator reaches its true terminal return, so historical backfill completion can notify without firing on Continue-As-New checkpoints.
