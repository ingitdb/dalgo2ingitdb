package dalgo2ingitdb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

func TestQueryOffsetAfterFilteringBeforeLimit(t *testing.T) {
	tx, _, _ := makeMapOfRecordsRWTx(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := tx.Set(ctx, record.NewRecordWithData(record.NewKeyWithID("scores", fmt.Sprintf("%02d", i)), map[string]any{"score": i})); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		offset int
		want   []string
	}{{0, []string{"03", "04", "05"}}, {2, []string{"05", "06", "07"}}, {7, nil}, {100, nil}} {
		q := dal.From(dal.NewRootCollectionRef("scores", "")).NewQuery().
			WhereField("score", dal.GreaterThen, 2).OrderBy(dal.AscendingField("score")).
			Offset(tc.offset).Limit(3).SelectIntoRecord(func() record.Record {
			return record.NewRecordWithData(record.NewKeyWithID("scores", ""), map[string]any{})
		})
		reader, err := executeQueryToRecordsReader(ctx, tx.readonlyTx, q)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for {
			rec, err := reader.Next()
			if err == dal.ErrNoMoreRecords {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, rec.Key().ID.(string))
		}
		_ = reader.Close()
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("offset %d: got %v, want %v", tc.offset, got, tc.want)
		}
	}
}

func TestProtectedQueryRejectsUnresolvedPredicateOnEmptyCollection(t *testing.T) {
	tx, _, _ := makeMapOfRecordsRWTx(t)
	tx.db = &Database{storedOnlyReads: true}
	query := dal.From(dal.NewRootCollectionRef("scores", "")).NewQuery().
		WhereField("score", dal.Equal, dal.NewParam("unresolved")).SelectKeysOnly(reflect.String)
	if _, err := executeQueryToRecordsReader(context.Background(), tx.readonlyTx, query); !errors.Is(err, dal.ErrNotSupported) {
		t.Fatalf("unresolved predicate on empty collection must fail closed: %v", err)
	}
}
