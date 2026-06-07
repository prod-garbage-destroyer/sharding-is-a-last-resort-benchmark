# Fairness Policy

## Same functionality
- All targets implement the same logical model: `accounts` and `transfers`.
- All targets support the same operations: increment, read balance, transfer, and range report.
- All targets seed the same keyspace: 100000 tenants with balance 100.
- All targets return the same logical outcomes and success criteria.

## Same external dependencies
- Manual sharding uses three PostgreSQL 16 containers.
- CockroachDB uses a three-node Cockroach cluster.
- TiDB uses the official TiDB quick-start compose topology with PD and TiKV.
- The benchmark harness uses the same host and the same Docker runtime for every target.

## Same workload
- Every target receives the same keyspace, seed, request mix, warmup duration, and measurement duration.
- Every target uses the same worker concurrency and the same random seed.
- Every target receives the same operation mix:
  - 60% increment
  - 20% read_balance
  - 10% transfer
  - 10% range_report

## Same hardware constraints
- All containers are run on the same machine.
- Every database node is started with the same per-container CPU and memory limit where the runtime supports it.
- Resource samples are taken from the running containers rather than from the host alone.

## Same warmup rules
- Warmup runs happen before measurement for every target.
- Warmup traffic is not included in the reported metrics.
- Schema creation and seeding happen before warmup and are not counted as benchmark throughput.

## Same retry policy
- Infrastructure startup can be retried once if a container fails to become healthy.
- Measurement retries are recorded, not hidden.
- Any retry is written into the environment log and the run metadata.

## Optimization policy
- Idiomatic database features are allowed if they are available to the target engine.
- Manual Postgres may use explicit client-side routing and fan-out because that is the behavior under test.
- CockroachDB and TiDB may use their native transactional and distributed query behavior because that is the comparison being made.
- No target gets an extra caching layer or a different query mix.

## Important asymmetry
- The manual Postgres target must coordinate cross-shard work in the application layer.
- CockroachDB and TiDB do not require that application-layer coordination.
- That asymmetry is the subject of the benchmark, not a fairness violation.
