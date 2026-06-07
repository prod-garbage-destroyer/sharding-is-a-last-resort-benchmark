package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBTarget interface {
	Name() string
	Setup(context.Context) error
	Seed(context.Context, int64) error
	Ping(context.Context) error
	Increment(context.Context, int64, int64, *rand.Rand) error
	ReadBalance(context.Context, int64, *rand.Rand) (int64, error)
	Transfer(context.Context, int64, int64, int64, *rand.Rand) error
	RangeReport(context.Context, int64, int64, *rand.Rand) (int64, error)
	Containers() []string
	PrepareForBenchmark(context.Context) error
	Close() error
}

type manualPostgresTarget struct {
	dbs        []*sql.DB
	containers []string
}

type sqlTarget struct {
	name       string
	dsn        string
	driver     string
	db         *sql.DB
	container  string
	containers []string
}

func (t *sqlTarget) Name() string         { return t.name }
func (t *sqlTarget) Containers() []string { return append([]string(nil), t.containers...) }
func (t *sqlTarget) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

func openPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	db.SetConnMaxLifetime(10 * time.Minute)
	return db, nil
}

func openMySQL(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	db.SetConnMaxLifetime(10 * time.Minute)
	return db, nil
}

func newManualPostgresTarget(dsn []string, containers []string) (*manualPostgresTarget, error) {
	if len(dsn) != 3 {
		return nil, errors.New("manual postgres requires three shard DSNs")
	}
	dbs := make([]*sql.DB, 0, 3)
	for _, d := range dsn {
		db, err := openPostgres(d)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return &manualPostgresTarget{dbs: dbs, containers: containers}, nil
}

func (t *manualPostgresTarget) Name() string         { return targetManual }
func (t *manualPostgresTarget) Containers() []string { return append([]string(nil), t.containers...) }
func (t *manualPostgresTarget) Close() error {
	for _, db := range t.dbs {
		if db != nil {
			_ = db.Close()
		}
	}
	return nil
}

func (t *manualPostgresTarget) route(tenant int64) int {
	r := int(tenant % 3)
	if r < 0 {
		r += 3
	}
	return r
}

func (t *manualPostgresTarget) dbForTenant(tenant int64) *sql.DB {
	return t.dbs[t.route(tenant)]
}

func (t *manualPostgresTarget) PrepareForBenchmark(ctx context.Context) error {
	return nil
}

func (t *manualPostgresTarget) Ping(ctx context.Context) error {
	for _, db := range t.dbs {
		if err := db.PingContext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *manualPostgresTarget) Setup(ctx context.Context) error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			tenant_id BIGINT PRIMARY KEY,
			balance BIGINT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS transfers (
			transfer_id VARCHAR(128) PRIMARY KEY,
			source_tenant BIGINT NOT NULL,
			dest_tenant BIGINT NOT NULL,
			amount BIGINT NOT NULL,
			status VARCHAR(16) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_accounts_updated_at ON accounts(updated_at)`,
	}
	for _, db := range t.dbs {
		for _, stmt := range schema {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *manualPostgresTarget) Seed(ctx context.Context, tenants int64) error {
	const batchSize = 1000
	for shardIdx, db := range t.dbs {
		if _, err := db.ExecContext(ctx, `DELETE FROM transfers`); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM accounts`); err != nil {
			return err
		}

		// Correct seeding: tenant % 3 == shardIdx
		for start := int64(1); start <= tenants; start += batchSize {
			end := start + batchSize
			if end > tenants+1 {
				end = tenants + 1
			}
			var values string
			for tenant := start; tenant < end; tenant++ {
				if t.route(tenant) != shardIdx {
					continue
				}
				if values != "" {
					values += ","
				}
				values += fmt.Sprintf("(%d,100,CURRENT_TIMESTAMP)", tenant)
			}
			if values == "" {
				continue
			}
			stmt := "INSERT INTO accounts (tenant_id, balance, updated_at) VALUES " + values
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *manualPostgresTarget) Increment(ctx context.Context, tenant, delta int64, r *rand.Rand) error {
	db := t.dbForTenant(tenant)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance + %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", delta, tenant)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO transfers (transfer_id, source_tenant, dest_tenant, amount, status) VALUES ('inc-%d-%d', %d, %d, %d, 'committed') ON CONFLICT (transfer_id) DO NOTHING", time.Now().UnixNano(), r.Int63(), tenant, tenant, delta)); err != nil {
		return err
	}
	return tx.Commit()
}

func (t *manualPostgresTarget) ReadBalance(ctx context.Context, tenant int64, r *rand.Rand) (int64, error) {
	db := t.dbForTenant(tenant)
	var bal int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT balance FROM accounts WHERE tenant_id = %d", tenant)).Scan(&bal); err != nil {
		return 0, err
	}
	return bal, nil
}

func (t *manualPostgresTarget) Transfer(ctx context.Context, source, dest, amount int64, r *rand.Rand) error {
	srcDB := t.dbForTenant(source)
	dstDB := t.dbForTenant(dest)
	transferID := fmt.Sprintf("xfer-%d-%d", time.Now().UnixNano(), r.Int63())
	if srcDB == dstDB {
		tx, err := srcDB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance - %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, source)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance + %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, dest)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO transfers (transfer_id, source_tenant, dest_tenant, amount, status) VALUES ('%s', %d, %d, %d, 'committed')", transferID, source, dest, amount)); err != nil {
			return err
		}
		return tx.Commit()
	}

	// Phase 1: Debit source, mark pending
	srcTx, err := srcDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := srcTx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance - %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, source)); err != nil {
		_ = srcTx.Rollback()
		return err
	}
	if _, err := srcTx.ExecContext(ctx, fmt.Sprintf("INSERT INTO transfers (transfer_id, source_tenant, dest_tenant, amount, status) VALUES ('%s', %d, %d, %d, 'pending')", transferID, source, dest, amount)); err != nil {
		_ = srcTx.Rollback()
		return err
	}
	if err := srcTx.Commit(); err != nil {
		return err
	}

	// Phase 2: Credit destination
	dstTx, err := dstDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := dstTx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance + %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, dest)); err != nil {
		_ = dstTx.Rollback()
		// Manual compensation needed for true correctness, but for a benchmark
		// we just report the error.
		return err
	}
	if err := dstTx.Commit(); err != nil {
		return err
	}

	// Phase 3: Finalize on source
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf("UPDATE transfers SET status = 'committed' WHERE transfer_id = '%s'", transferID)); err != nil {
		// Log but don't fail the whole request as funds have moved
		return nil
	}
	return nil
}

func (t *manualPostgresTarget) RangeReport(ctx context.Context, start, end int64, r *rand.Rand) (int64, error) {
	type shardResult struct {
		sum int64
		err error
	}
	ch := make(chan shardResult, len(t.dbs))
	var wg sync.WaitGroup
	for idx, db := range t.dbs {
		wg.Add(1)
		go func(idx int, db *sql.DB) {
			defer wg.Done()
			var sum int64
			q := fmt.Sprintf("SELECT COALESCE(SUM(balance), 0) FROM accounts WHERE tenant_id BETWEEN %d AND %d", start, end)
			if err := db.QueryRowContext(ctx, q).Scan(&sum); err != nil {
				ch <- shardResult{err: err}
				return
			}
			ch <- shardResult{sum: sum}
		}(idx, db)
	}
	wg.Wait()
	close(ch)
	total := int64(0)
	for item := range ch {
		if item.err != nil {
			return 0, item.err
		}
		total += item.sum
	}
	return total, nil
}

func newCockroachTarget(port int) (*sqlTarget, error) {
	dsn := fmt.Sprintf("postgres://root@127.0.0.1:%d/defaultdb?sslmode=disable", port)
	db, err := openPostgres(dsn)
	if err != nil {
		return nil, err
	}
	return &sqlTarget{
		name:       targetCockroach,
		dsn:        dsn,
		driver:     "pgx",
		db:         db,
		container:  "roach1",
		containers: []string{"roach1", "roach2", "roach3"},
	}, nil
}

func newTiDBTarget(port int) (*sqlTarget, error) {
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%d)/?charset=utf8mb4&parseTime=true&loc=UTC", port)
	db, err := openMySQL(dsn)
	if err != nil {
		return nil, err
	}
	return &sqlTarget{
		name:       targetTiDB,
		dsn:        dsn,
		driver:     "mysql",
		db:         db,
		container:  "tidb",
		containers: []string{"pd0", "pd1", "pd2", "tikv0", "tikv1", "tikv2", "tidb"},
	}, nil
}

func (t *sqlTarget) PrepareForBenchmark(ctx context.Context) error { return nil }

func (t *sqlTarget) Ping(ctx context.Context) error {
	return t.db.PingContext(ctx)
}

func (t *sqlTarget) Setup(ctx context.Context) error {
	if t.name == targetTiDB {
		if _, err := t.db.ExecContext(ctx, `CREATE DATABASE IF NOT EXISTS benchmark`); err != nil {
			return err
		}
		if err := t.db.Close(); err != nil {
			return err
		}
		reopenDSN := strings.Replace(t.dsn, "/?", "/benchmark?", 1)
		db, err := openMySQL(reopenDSN)
		if err != nil {
			return err
		}
		t.db = db
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS accounts (
			tenant_id BIGINT PRIMARY KEY,
			balance BIGINT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS transfers (
			transfer_id VARCHAR(128) PRIMARY KEY,
			source_tenant BIGINT NOT NULL,
			dest_tenant BIGINT NOT NULL,
			amount BIGINT NOT NULL,
			status VARCHAR(16) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_accounts_updated_at ON accounts(updated_at)`,
	}
	for _, stmt := range stmts {
		if _, err := t.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqlTarget) Seed(ctx context.Context, tenants int64) error {
	if _, err := t.db.ExecContext(ctx, `DELETE FROM transfers`); err != nil {
		return err
	}
	if _, err := t.db.ExecContext(ctx, `DELETE FROM accounts`); err != nil {
		return err
	}
	const batchSize = 1000
	for start := int64(1); start <= tenants; start += batchSize {
		end := start + batchSize
		if end > tenants+1 {
			end = tenants + 1
		}
		values := ""
		for tenant := start; tenant < end; tenant++ {
			if values != "" {
				values += ","
			}
			values += fmt.Sprintf("(%d,100,CURRENT_TIMESTAMP)", tenant)
		}
		stmt := "INSERT INTO accounts (tenant_id, balance, updated_at) VALUES " + values
		if _, err := t.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (t *sqlTarget) Increment(ctx context.Context, tenant, delta int64, r *rand.Rand) error {
	return retry(ctx, func() error {
		tx, err := t.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance + %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", delta, tenant)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, t.insertTransferIgnoreSQL(fmt.Sprintf("inc-%d-%d", time.Now().UnixNano(), r.Int63()), tenant, tenant, delta, "committed")); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (t *sqlTarget) ReadBalance(ctx context.Context, tenant int64, r *rand.Rand) (int64, error) {
	var bal int64
	if err := t.db.QueryRowContext(ctx, fmt.Sprintf("SELECT balance FROM accounts WHERE tenant_id = %d", tenant)).Scan(&bal); err != nil {
		return 0, err
	}
	return bal, nil
}

func (t *sqlTarget) Transfer(ctx context.Context, source, dest, amount int64, r *rand.Rand) error {
	return retry(ctx, func() error {
		tx, err := t.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		transferID := fmt.Sprintf("xfer-%d-%d", time.Now().UnixNano(), r.Int63())
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance - %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, source)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE accounts SET balance = balance + %d, updated_at = CURRENT_TIMESTAMP WHERE tenant_id = %d", amount, dest)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, t.insertTransferSQL(transferID, source, dest, amount, "committed")); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (t *sqlTarget) insertTransferSQL(id string, source, dest, amount int64, status string) string {
	return fmt.Sprintf(
		"INSERT INTO transfers (transfer_id, source_tenant, dest_tenant, amount, status) VALUES ('%s', %d, %d, %d, '%s')",
		id,
		source,
		dest,
		amount,
		status,
	)
}

func (t *sqlTarget) insertTransferIgnoreSQL(id string, source, dest, amount int64, status string) string {
	base := t.insertTransferSQL(id, source, dest, amount, status)
	if t.name == targetTiDB {
		return strings.Replace(base, "INSERT INTO", "INSERT IGNORE INTO", 1)
	}
	return base + " ON CONFLICT (transfer_id) DO NOTHING"
}

func retry(ctx context.Context, fn func() error) error {
	var lastErr error
	for i := 0; i < 5; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		s := err.Error()
		// Cockroach 40001, TiDB deadlock or conflict
		if strings.Contains(s, "40001") || strings.Contains(s, "deadlock") || strings.Contains(s, "conflict") || strings.Contains(s, "try again") {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(10*(i+1)) * time.Millisecond):
				continue
			}
		}
		return err
	}
	return lastErr
}

func (t *sqlTarget) RangeReport(ctx context.Context, start, end int64, r *rand.Rand) (int64, error) {
	var total int64
	if err := t.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(SUM(balance), 0) FROM accounts WHERE tenant_id BETWEEN %d AND %d", start, end)).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}
