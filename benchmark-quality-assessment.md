# Benchmark Quality Assessment

## Verdict

The benchmark is good enough to support a constrained claim:

Manual sharding can win local successful-throughput and latency when the app owns routing, but distributed SQL removes routing, fan-out, and transaction coordination from application code.

The previous single-tier benchmark was not good enough to support the broad claim "NewSQL beats manual partitioning" as a performance statement. The corrected runner now tests multiple concurrency tiers, and the completed rerun gives enough evidence for a local scaling comparison across 8, 24, 64, and 128 workers.

## Evidence That Is Usable

- Same logical schema: `accounts` and `transfers`.
- Same seeded keyspace: 100000 tenants.
- Same workload mix: 60% increment, 20% read, 10% transfer, 10% range report.
- Same warmup and measurement shape: 3 warmups, 5 measured runs, 20 seconds per measured run, across 8, 24, 64, and 128 worker concurrency tiers.
- The completed rerun produced 60 raw measurement runs: 3 targets x 4 concurrency tiers x 5 runs.
- All targets completed the rerun with 0% errors.
- Manual Postgres averaged about 3589 ops/s across tiers and peaked around 4717 ops/s at 128 workers.
- TiDB averaged about 1099 ops/s across tiers and peaked around 1414 ops/s at 128 workers.
- CockroachDB averaged about 455 ops/s across tiers and peaked around 525 ops/s at 64 workers before dipping at 128.
- Manual routing and cross-shard transfer code provide strong qualitative evidence for application-owned complexity.

## Prior Evidence That Was Not Usable

- The first TiDB report had about 59.9% mean errors.
- The error share matches the 60% increment share, and successful TiDB operation stats exclude increment successes.
- The runner used PostgreSQL `ON CONFLICT` syntax in the shared SQL increment path, which is not valid for TiDB/MySQL.
- That old TiDB performance result should not be used. The corrected rerun replaces it.

## Improvements Made

- Updated the TiDB insert path to use `INSERT IGNORE` while preserving `ON CONFLICT` for CockroachDB.
- Updated the runner to record per-operation error counts.
- Corrected throughput metadata so higher throughput is treated as better.
- Added report helper logic for derived metrics and quality flags.
- Added unit coverage for the SQL dialect-specific insert behavior.

## Regenerated Artifacts

The full benchmark rerun regenerated:

- `artifacts/benchmark-results.json`
- `benchmark-report.json`
- `visual-datasets/*.json`
- `artifacts/verdict.json`

## Benchmark Expansion Added

The runner now includes concurrency tiers:

- 8 workers
- 24 workers
- 64 workers
- 128 workers

The final claim should use the rerun artifacts, not the old single-concurrency output.

## Safe Video Claim

Use this framing:

Manual sharding won the local sprint, but the victory comes by moving routing and coordination into application code. Native distributed SQL is not free, but it keeps that complexity inside the database layer. Sharding should be a last resort after simpler database scaling options are exhausted.
