# Sharding Is a Last Resort Benchmark

This repository contains the benchmark harness and result package for comparing:

- manual PostgreSQL shard routing
- CockroachDB native distributed SQL
- TiDB native distributed SQL

The benchmark topic is narrow on purpose: a mixed ledger workload run locally under the same logical schema, the same seed, and the same request mix.

## Included

- `spec.yaml`: benchmark contract and workload definition
- `runner/`: Go harness that seeds data, executes load, and emits artifacts
- `implementations/`: target-specific notes for manual Postgres, CockroachDB, and TiDB
- `artifacts/`: compact result artifacts and verdict files
- `visual-datasets/`: chart-ready datasets used for analysis
- `fairness-policy.md`: scope and claim boundaries
- `benchmark-quality-assessment.md`: benchmark validity analysis

## Excluded

This extracted repo intentionally excludes:

- video production files
- narration and audio assets
- rendered MP4 output
- TiDB runtime state directories
- oversized raw benchmark dumps that are not needed to understand or rerun the benchmark

## Key Result

Manual sharding won local throughput in this setup, but that did not justify a broad claim that manual sharding is categorically better than distributed SQL. The benchmark also showed where complexity moved, and the final claim was narrowed after a TiDB harness SQL dialect bug was fixed and the benchmark was rerun cleanly.

## Repro Notes

1. Read `spec.yaml`.
2. Read `fairness-policy.md`.
3. Run the Go harness from `runner/`.
4. Inspect `artifacts/verdict.json` and `artifacts/summary.md`.

## Structure Notes

`README.source.md` is the original output-directory README from the video workspace. `README.md` is the cleaned standalone repo entry point.
