package main

import (
	"math/rand"
	"strings"
	"testing"
)

func TestPercentile(t *testing.T) {
	got := percentile([]float64{1, 2, 3, 4, 5}, 95)
	if got < 4.8 || got > 5.0 {
		t.Fatalf("unexpected percentile: %v", got)
	}
}

func TestChooseOp(t *testing.T) {
	mix := map[string]int{opIncrement: 60, opReadBalance: 20, opTransfer: 10, opRangeReport: 10}
	for i := 0; i < 20; i++ {
		op := chooseOp(randSource(int64(i)), mix)
		switch op {
		case opIncrement, opReadBalance, opTransfer, opRangeReport:
		default:
			t.Fatalf("unexpected op %q", op)
		}
	}
}

func TestInsertTransferIgnoreSQLUsesTargetDialect(t *testing.T) {
	tidb := &sqlTarget{name: targetTiDB}
	tidbSQL := tidb.insertTransferIgnoreSQL("id-1", 1, 1, 1, "committed")
	if !strings.Contains(tidbSQL, "INSERT IGNORE INTO") {
		t.Fatalf("TiDB insert should use INSERT IGNORE: %s", tidbSQL)
	}
	if strings.Contains(tidbSQL, "ON CONFLICT") {
		t.Fatalf("TiDB insert must not use PostgreSQL ON CONFLICT syntax: %s", tidbSQL)
	}

	roach := &sqlTarget{name: targetCockroach}
	roachSQL := roach.insertTransferIgnoreSQL("id-1", 1, 1, 1, "committed")
	if !strings.Contains(roachSQL, "ON CONFLICT (transfer_id) DO NOTHING") {
		t.Fatalf("Cockroach insert should preserve ON CONFLICT syntax: %s", roachSQL)
	}
}

func randSource(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}
