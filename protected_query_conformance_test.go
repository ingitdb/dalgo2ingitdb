package dalgo2ingitdb_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/dal-go/dalgo/access"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/ingitdb/dalgo2ingitdb"
	"github.com/ingitdb/ingitdb-go/ingitdb/validator"
)

// The policy predicate is pushed into a real storage query. Query membership
// must match protected point authorization, not the legacy query comparator.
func TestProtectedQueryPredicateMatchesPointAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name, rejected, accepted, field string
		op                              dal.Operator
		value                           any
	}{
		{"number-versus-text", "ownerID: '1'\n", "ownerID: 1\n", "ownerID", dal.Equal, 1},
		{"boolean-versus-text", "ownerID: 'true'\n", "ownerID: true\n", "ownerID", dal.Equal, true},
		{"missing-versus-null", "name: absent\n", "ownerID: null\n", "ownerID", dal.Equal, nil},
		{"nested-field", "meta: {owner: other}\n", "meta: {owner: mine}\n", "meta.owner", dal.Equal, "mine"},
		{"in", "ownerID: '1'\n", "ownerID: 1\n", "ownerID", dal.In, []any{1, 2}},
		{"ordered-types", "ownerID: '9'\n", "ownerID: 9\n", "ownerID", dal.GreaterThen, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, root := setupProtectedCountries(t)
			writeYAMLRecord(t, root, "countries", "one", tc.rejected)
			writeYAMLRecord(t, root, "countries", "two", tc.accepted)
			writeOwnerPolicy(t, root, "countries", `"*"`)
			raw, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader(), dalgo2ingitdb.WithProtectedProfile())
			if err != nil {
				t.Fatal(err)
			}
			var right dal.Expression = dal.Constant{Value: tc.value}
			if tc.op == dal.In {
				right = dal.Array{Value: tc.value}
			}
			predicate := dal.Comparison{Left: dal.NewFieldRef("", tc.field), Operator: tc.op, Right: right}
			policy := access.MustPolicy("typed-owner", access.Collection("countries", access.Allow(access.Query).Where(predicate)), access.Scope("countries", access.AnyID, access.Allow(access.Get).Where(predicate)))
			participant, err := access.NewStaticParticipant("typed", policy)
			if err != nil {
				t.Fatal(err)
			}
			db, _, err := raw.(dalgo2ingitdb.ProtectedAccessConfigurer).ConfigureProtectedAccess(participant)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			for _, key := range []string{"one", "two"} {
				rec := record.NewRecordWithData(record.NewKeyWithID("countries", key), map[string]any{})
				err := db.Get(ctx, rec)
				if key == "one" && !errors.Is(err, access.ErrAccessDenied) {
					t.Fatalf("rejected point: %v", err)
				}
				if key == "two" && err != nil {
					t.Fatalf("allowed point: %v", err)
				}
			}
			query := dal.From(dal.NewRootCollectionRef("countries", "")).NewQuery().OrderBy(dal.AscendingField("$id")).Limit(1).SelectKeysOnly(reflect.String)
			reader, err := db.ExecuteQueryToRecordsReader(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			rec, err := reader.Next()
			if err != nil || rec.Key().ID != "two" {
				t.Fatalf("query must skip denied row before limit: %v, %v", rec, err)
			}
			if _, err = reader.Next(); !errors.Is(err, dal.ErrNoMoreRecords) {
				t.Fatalf("extra query row: %v", err)
			}
		})
	}
}

func TestProtectedQueryRejectsSyntheticIdentityPolicy(t *testing.T) {
	for _, stored := range []string{"name: Two\n", "name: Two\n$id: conflicting\n"} {
		t.Run(stored, func(t *testing.T) {
			_, _, root := setupProtectedCountries(t)
			writeYAMLRecord(t, root, "countries", "two", stored)
			writeOwnerPolicy(t, root, "countries", `"*"`)
			raw, err := dalgo2ingitdb.NewDatabase(root, validator.NewCollectionsReader(), dalgo2ingitdb.WithProtectedProfile())
			if err != nil {
				t.Fatal(err)
			}
			condition := dal.WhereField("$id", dal.Equal, "two")
			policy := access.MustPolicy("identity", access.Collection("countries", access.Allow(access.Query).Where(condition)), access.Scope("countries", access.AnyID, access.Allow(access.Get).Where(condition)))
			participant, err := access.NewStaticParticipant("identity", policy)
			if err != nil {
				t.Fatal(err)
			}
			db, _, err := raw.(dalgo2ingitdb.ProtectedAccessConfigurer).ConfigureProtectedAccess(participant)
			if err != nil {
				t.Fatal(err)
			}
			point := record.NewRecordWithData(record.NewKeyWithID("countries", "two"), map[string]any{})
			if err := db.Get(context.Background(), point); !errors.Is(err, access.ErrAccessDenied) {
				t.Fatalf("point must not use synthetic identity: %v", err)
			}
			query := dal.From(dal.NewRootCollectionRef("countries", "")).NewQuery().SelectKeysOnly(reflect.String)
			if _, err := db.ExecuteQueryToRecordsReader(context.Background(), query); !errors.Is(err, dal.ErrNotSupported) {
				t.Fatalf("identity policy query must fail unsupported: %v", err)
			}
		})
	}
}
