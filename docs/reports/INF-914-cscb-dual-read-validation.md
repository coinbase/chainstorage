# CSCB Dual-Read Validation Report

- generated_at: `2026-06-22T05:28:49Z`
- target: `production/BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET`
- tag: `2`
- canary_range: `[428058000, 428059000)`
- chunk_blocks: `25`
- server_address_override: `localhost:19090`
- grpc_transport_override: `true`
- client_id: `INF-914-cscb-dual-read-validation`
- passed: `true`
- cases: `18 passed / 18 total`
- mismatch_heights: `0`
- observed_fallback_blocks: `1`

## Canary Object and Expected Profile

- consolidated_object: `BLOCKCHAIN_SOLANA/NETWORK_SOLANA_MAINNET/consolidated/v=2/shard=00000000000428050000-00000000000428060000/00000000000428058000-00000000000428059000-20a1b269fb75137d829d4ade11b3cdf67bab5c3a8c9be046fd8069396607b906.cscb.zstd`
- legacy_baseline: `1000 objects`, `907486583 bytes`, `865.45 MiB`
- consolidated_object_size: `567346484 bytes`, `541.06 MiB`, `37.48% saving`
- selected_config: `max_blocks=1000`, `compression_chunk_blocks=25`, `zstd_level=12`, `long_window_log=27`, `memory_budget=256MiB`

## Production Execution Evidence

- connection: read-only gRPC validation through `kubectl --context prod port-forward -n chainstorage-prod svc/chainstorage-solana-mainnet-prod-server-svc 19090:9090`
- target_pod: `chainstorage-prod/chainstorage-solana-mainnet-prod-server-6f7bd7b5bb-9k7pr`
- safety: prod operations were read-only (`get`, `top`, `logs`, `port-forward`, and Chainstorage read APIs); no database writes or object mutations were performed.
- pre_validation_resources: `chainstorage-server CPU=2m RSS=15Mi restarts=0 ready=true`; `linkerd-proxy CPU=1m RSS=3Mi restarts=0 ready=true`
- post_validation_resources: `chainstorage-server CPU=2m RSS=23Mi restarts=0 ready=true`; `linkerd-proxy CPU=1m RSS=3Mi restarts=0 ready=true`
- post_validation_error_logs: no matching `error|panic|fatal|fallback|cscb|shadow|INF-914` lines in `chainstorage-server` logs over the validation window.
- full_object_result: legacy `1000` HTTP reads / `907486583` bytes / p50 `103929.96ms`; consolidated `43` HTTP reads / `567346484` bytes / p50 `39572.80ms`; canonical hashes matched for all `1000` blocks.

## Case Results

| case | range | pass | fallback | legacy p50/p95 ms | consolidated p50/p95 ms | legacy bytes | consolidated bytes | mismatches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| single_first_block | `[428058000,428058001)` | true | false | 1389.39 / 1728.34 | 553.34 / 571.62 | 2847939 | 2882226 |  |
| single_last_block | `[428058999,428059000)` | true | false | 218.82 / 1013.84 | 399.04 / 521.44 | 2247348 | 3777807 |  |
| single_chunk_boundary_428058024 | `[428058024,428058025)` | true | false | 207.55 / 288.99 | 1039.39 / 1052.37 | 3084087 | 39109401 |  |
| single_chunk_boundary_428058025 | `[428058025,428058026)` | true | false | 681.19 / 688.68 | 417.14 / 441.67 | 2469318 | 2451210 |  |
| single_chunk_boundary_428058049 | `[428058049,428058050)` | true | false | 190.73 / 710.16 | 872.05 / 897.71 | 2358117 | 33572286 |  |
| single_chunk_boundary_428058050 | `[428058050,428058051)` | true | false | 658.69 / 698.49 | 452.18 / 471.37 | 2188680 | 2255577 |  |
| single_chunk_boundary_428058074 | `[428058074,428058075)` | true | false | 208.26 / 766.69 | 912.41 / 1308.56 | 3051528 | 36464772 |  |
| single_chunk_boundary_428058075 | `[428058075,428058076)` | true | false | 684.62 / 1262.18 | 398.14 / 501.07 | 2627094 | 2465205 |  |
| single_random_428058100 | `[428058100,428058101)` | true | false | 718.61 / 1282.68 | 421.26 / 423.05 | 2908002 | 2756445 |  |
| single_random_428058118 | `[428058118,428058119)` | true | false | 1272.78 / 1463.85 | 3170.19 / 6313.94 | 1939224 | 30511797 |  |
| single_random_428058141 | `[428058141,428058142)` | true | false | 984.13 / 1626.60 | 3797.58 / 4665.43 | 3889650 | 36896916 |  |
| single_random_428058339 | `[428058339,428058340)` | true | false | 913.41 / 1296.78 | 1415.48 / 2200.59 | 2794089 | 29017683 |  |
| single_random_428058828 | `[428058828,428058829)` | true | false | 1492.80 / 1633.39 | 922.26 / 978.44 | 3594321 | 11397528 |  |
| range_within_one_chunk | `[428058005,428058015)` | true | false | 2996.34 / 3001.57 | 2013.97 / 2089.99 | 22951683 | 24104910 |  |
| range_across_chunk_boundary | `[428058020,428058030)` | true | false | 2977.53 / 3042.11 | 3097.04 / 5206.64 | 21927276 | 45903672 |  |
| range_two_chunks | `[428058000,428058050)` | true | false | 4055.68 / 4705.36 | 4934.94 / 5072.74 | 118865385 | 72384303 |  |
| range_full_object | `[428058000,428059000)` | true | false | 103929.96 / 103929.96 | 39572.80 / 39572.80 | 907486583 | 567346484 |  |
| single_consolidated_miss_fallback | `[428057999,428058000)` | true | true | 447.05 / 2794.87 | 421.77 / 805.60 | 3351267 | 3351267 |  |

## Source Summary

### single_first_block

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2847939`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `2882226`
- canonical hashes compared: `1`

### single_last_block

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2247348`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `3777807`
- canonical hashes compared: `1`

### single_chunk_boundary_428058024

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3084087`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `39109401`
- canonical hashes compared: `1`

### single_chunk_boundary_428058025

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2469318`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `2451210`
- canonical hashes compared: `1`

### single_chunk_boundary_428058049

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2358117`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `33572286`
- canonical hashes compared: `1`

### single_chunk_boundary_428058050

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2188680`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `2255577`
- canonical hashes compared: `1`

### single_chunk_boundary_428058074

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3051528`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `36464772`
- canonical hashes compared: `1`

### single_chunk_boundary_428058075

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2627094`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `2465205`
- canonical hashes compared: `1`

### single_random_428058100

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2908002`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `2756445`
- canonical hashes compared: `1`

### single_random_428058118

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `1939224`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `30511797`
- canonical hashes compared: `1`

### single_random_428058141

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3889650`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `36896916`
- canonical hashes compared: `1`

### single_random_428058339

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `2794089`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `29017683`
- canonical hashes compared: `1`

### single_random_428058828

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3594321`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `11397528`
- canonical hashes compared: `1`

### range_within_one_chunk

- legacy files: `10`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `30`, bytes: `22951683`
- consolidated files: `10`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `9`, bytes: `24104910`
- canonical hashes compared: `10`

### range_across_chunk_boundary

- legacy files: `10`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `30`, bytes: `21927276`
- consolidated files: `10`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `12`, bytes: `45903672`
- canonical hashes compared: `10`

### range_two_chunks

- legacy files: `50`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `150`, bytes: `118865385`
- consolidated files: `50`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `12`, bytes: `72384303`
- canonical hashes compared: `50`

### range_full_object

- legacy files: `1000`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `1000`, bytes: `907486583`
- consolidated files: `1000`, formats: `BLOCK_OBJECT_FORMAT_CSCB_BATCH`, HTTP requests: `43`, bytes: `567346484`
- canonical hashes compared: `1000`

### single_consolidated_miss_fallback

- legacy files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3351267`
- consolidated files: `1`, formats: `BLOCK_OBJECT_FORMAT_LEGACY_SINGLE_BLOCK`, HTTP requests: `3`, bytes: `3351267`
- canonical hashes compared: `1`
