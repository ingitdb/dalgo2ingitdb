package dalgo2ingitdb

import (
	"context"
	"errors"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
	"strings"
	"testing"
)

type failingProjectedReader struct{}

func (failingProjectedReader) Next() (record.Record, error) {
	return nil, errors.New("projected read failed")
}
func (failingProjectedReader) Cursor() (string, error) { return "", nil }
func (failingProjectedReader) Close() error            { return nil }

type oneProjectedReader struct{ sent bool }

type failingProjectedColumn struct{ *anyColumn }

func (failingProjectedColumn) SetValue(int, any) error { return errors.New("projected value rejected") }

func (r *oneProjectedReader) Next() (record.Record, error) {
	if r.sent {
		return nil, dal.ErrNoMoreRecords
	}
	r.sent = true
	return record.NewRecordWithData(record.NewKeyWithID("items", "1"), map[string]any{"name": "one"}), nil
}
func (*oneProjectedReader) Cursor() (string, error) { return "", nil }
func (*oneProjectedReader) Close() error            { return nil }

func TestFillProjectedRowsErrors(t *testing.T) {
	rs := recordset.NewColumnarRecordset("items", &anyColumn{name: "name"})
	if err := fillProjectedRows(failingProjectedReader{}, rs, []string{"name"}); err == nil || err.Error() != "projected read failed" {
		t.Fatalf("reader error: %v", err)
	}
	if _, err := projectRecordsFromReader(failingProjectedReader{}, []dal.Column{{Expression: dal.Field("name")}}); err == nil || err.Error() != "projected read failed" {
		t.Fatalf("projected query error: %v", err)
	}
	rejected := recordset.NewColumnarRecordset("items", failingProjectedColumn{&anyColumn{name: "name"}})
	if err := fillProjectedRows(&oneProjectedReader{}, rejected, []string{"name"}); err == nil || err.Error() != "projected value rejected" {
		t.Fatalf("projected value error: %v", err)
	}
}

func TestDatabaseRecordsetQueryPropagatesDefinitionError(t *testing.T) {
	db := &Database{projectPath: t.TempDir(), reader: errReader{}}
	if _, err := db.ExecuteQueryToRecordsetReader(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "broken reader") {
		t.Fatalf("definition error: %v", err)
	}
}

func TestRecordsetProjectionAndGroupErrorPaths(t *testing.T) {
	tx, _ := recordsetQueryTx(t)
	ref := dal.NewRootCollectionRef("people", "")
	group := mockStructuredQuery{customFrom: dal.From(ref), customGroupBy: []dal.Expression{dal.Field("first_name")}}
	if _, err := tx.ExecuteQueryToRecordsetReader(context.Background(), group); err == nil {
		t.Fatal("grouped recordset must refuse dynamic shape")
	}
	if _, err := tx.ExecuteQueryToRecordsReader(context.Background(), group); err != nil {
		t.Fatalf("grouped records query: %v", err)
	}
	wildcard := mockStructuredQuery{customFrom: dal.From(ref), customColumns: []dal.Column{dal.AllColumnsExcept("last_name")}}
	if _, err := tx.ExecuteQueryToRecordsetReader(context.Background(), wildcard); err != nil {
		t.Fatalf("wildcard recordset: %v", err)
	}
	badProjection := mockStructuredQuery{customFrom: dal.From(ref), customColumns: []dal.Column{{Expression: mockUnsupportedExpr{}}}}
	if _, err := tx.ExecuteQueryToRecordsetReader(context.Background(), badProjection); err == nil {
		t.Fatal("unsupported projection accepted")
	}
}
