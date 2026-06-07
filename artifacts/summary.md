# Summary

This benchmark compares manual PostgreSQL shard routing against native distributed SQL in CockroachDB and TiDB.

The workload is a mixed transactional ledger:
- increment balance
- read balance
- transfer between tenants
- aggregate a tenant range

Manual sharding carries the routing and fan-out burden in the application layer. CockroachDB and TiDB absorb that burden in the database layer.