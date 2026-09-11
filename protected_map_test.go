package dalgo2ingitdb

import (
	"context"
	"github.com/dal-go/record/update"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/record"
)

func TestProtectedMapBatchReceiptsMatchCombinedFinalFile(t *testing.T) {
	tx, _, _ := makeMapOfRecordsRWTx(t)
	ctx := context.Background()
	for key, score := range map[string]int{"alice": 1, "bob": 2} {
		if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("scores", key), map[string]any{"score": score})); err != nil {
			t.Fatal(err)
		}
	}
	storage := &protectedStorage{db: tx.db, secret: []byte("01234567890123456789012345678901")}
	ops := make([]access.ProtectedOperation, 0, 2)
	for i, item := range []struct {
		key   string
		score int
	}{{"alice", 10}, {"bob", 20}} {
		op, err := access.NewProtectedSet(string(rune('a'+i)), record.NewKeyWithID("scores", item.key), map[string]any{"score": item.score}, "")
		if err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
	}
	evidence, candidates, err := storage.prepare(ctx, tx.readonlyTx, ops)
	if err != nil {
		t.Fatal(err)
	}
	for i, op := range ops {
		if err := tx.Set(ctx, record.NewRecordWithData(op.Key(), candidates[i])); err != nil {
			t.Fatal(err)
		}
	}
	after, _, err := storage.prepare(ctx, tx.readonlyTx, ops)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ops {
		if evidence[i].CandidateRevision != after[i].DataRevision {
			t.Fatalf("op %d receipt does not match combined final file", i)
		}
	}
}

func TestProtectedMapSiblingChangeInvalidatesRevision(t *testing.T) {
	tx, _, _ := makeMapOfRecordsRWTx(t)
	ctx := context.Background()
	for key, score := range map[string]int{"alice": 1, "bob": 2} {
		if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("scores", key), map[string]any{"score": score})); err != nil {
			t.Fatal(err)
		}
	}
	storage := &protectedStorage{db: tx.db, secret: []byte("01234567890123456789012345678901")}
	op, err := access.NewProtectedSet("alice", record.NewKeyWithID("scores", "alice"), map[string]any{"score": 10}, "")
	if err != nil {
		t.Fatal(err)
	}
	before, _, err := storage.prepare(ctx, tx.readonlyTx, []access.ProtectedOperation{op})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("scores", "bob"), map[string]any{"score": 99, "hidden": "changed"})); err != nil {
		t.Fatal(err)
	}
	after, _, err := storage.prepare(ctx, tx.readonlyTx, []access.ProtectedOperation{op})
	if err != nil {
		t.Fatal(err)
	}
	if before[0].DataRevision == after[0].DataRevision {
		t.Fatal("sibling-only hidden change did not invalidate whole-file revision")
	}
}

func TestProtectedMapNestedCandidatesDoNotMutateEvidence(t *testing.T) {
	tx, _, _ := makeMapOfRecordsRWTx(t)
	ctx := context.Background()
	key := record.NewKeyWithID("scores", "victim")
	original := map[string]any{"score": 1, "meta": map[string]any{"ownerID": "victim", "hidden": "keep"}, "items": []any{map[string]any{"value": "original"}}}
	if err := tx.Set(ctx, record.NewRecordWithData(key, original)); err != nil {
		t.Fatal(err)
	}
	storage := &protectedStorage{db: tx.db, secret: []byte("01234567890123456789012345678901")}
	for _, changes := range [][]update.Update{
		{update.ByFieldName("meta.ownerID", "attacker")},
		{update.ByFieldName("meta.ownerID", update.DeleteField)},
		{update.ByFieldName("meta", map[string]any{"ownerID": "attacker"})},
	} {
		op, err := access.NewProtectedUpdate("nested", key, changes, "")
		if err != nil {
			t.Fatal(err)
		}
		evidence, candidates, err := storage.prepare(ctx, tx.readonlyTx, []access.ProtectedOperation{op})
		if err != nil {
			t.Fatal(err)
		}
		if evidence[0].PreImage["meta"].(map[string]any)["ownerID"] != "victim" {
			t.Fatal("candidate rewrote authorization pre-image")
		}
		copy := cloneEvidence(evidence)
		copy[0].PreImage["items"].([]any)[0].(map[string]any)["value"] = "changed"
		candidates[0]["meta"].(map[string]any)["hidden"] = "changed"
		if evidence[0].PreImage["items"].([]any)[0].(map[string]any)["value"] != "original" || evidence[0].PreImage["meta"].(map[string]any)["hidden"] != "keep" {
			t.Fatal("evidence shares mutable descendants")
		}
		stored := record.NewRecordWithData(key, map[string]any{})
		if err := tx.Get(ctx, stored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(stored.Data().(map[string]any)["meta"], original["meta"]) {
			t.Fatal("preparation changed backing map record")
		}
	}
}
