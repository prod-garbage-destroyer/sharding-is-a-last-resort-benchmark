# Manual PostgreSQL Sharding

Three PostgreSQL 16 containers act as shard owners.

Routing rules:

- `tenant_id % 3 == 0` -> shard 0
- `tenant_id % 3 == 1` -> shard 1
- `tenant_id % 3 == 2` -> shard 2

Cross-shard transfers are coordinated in the benchmark harness with application-level fan-out and compensating writes.

