# Sharding Is A Last Resort Benchmark

This output directory contains the benchmark spec, the fairness policy, the Go harness, and the artifacts for comparing:

- manual PostgreSQL shard routing
- CockroachDB native distribution
- TiDB native distribution

Run order:

1. `runner` builds and executes the benchmark harness.
2. `artifacts/` stores raw logs and summaries.
3. `visual-datasets/` stores chart-ready data.
4. `benchmark-report.json` stores the canonical report.

