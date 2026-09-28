package dalgo2ingitdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dal-go/dalgo/dal"
	dalrecord "github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

type mockAggFunc struct {
	name string
	args []dal.Expression
}

func (m mockAggFunc) String() string             { return m.name }
func (m mockAggFunc) FuncName() string           { return m.name }
func (m mockAggFunc) FuncArgs() []dal.Expression { return m.args }

type mockUnsupportedExpr struct{}

func (m mockUnsupportedExpr) String() string { return "unsupported_expr" }

type mockUnsupportedCond struct{}

func (m mockUnsupportedCond) String() string { return "unsupported_cond" }

type mockStepCancelContext struct {
	context.Context
	calls    int
	cancelAt int
}

func (m *mockStepCancelContext) Err() error {
	m.calls++
	if m.calls >= m.cancelAt {
		return context.Canceled
	}
	return nil
}

type mockStructuredQuery struct {
	dal.StructuredQuery
	customFrom    dal.FromSource
	customWhere   dal.Condition
	customGroupBy []dal.Expression
	customHaving  dal.Condition
	customOrderBy []dal.OrderExpression
	customLimit   int
	customOffset  int
	customColumns []dal.Column
}

func (m mockStructuredQuery) From() dal.FromSource           { return m.customFrom }
func (m mockStructuredQuery) Where() dal.Condition           { return m.customWhere }
func (m mockStructuredQuery) GroupBy() []dal.Expression      { return m.customGroupBy }
func (m mockStructuredQuery) Having() dal.Condition          { return m.customHaving }
func (m mockStructuredQuery) OrderBy() []dal.OrderExpression { return m.customOrderBy }
func (m mockStructuredQuery) Limit() int                     { return m.customLimit }
func (m mockStructuredQuery) Offset() int                    { return m.customOffset }
func (m mockStructuredQuery) Columns() []dal.Column          { return m.customColumns }

func newBaseQuery(collection string) dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef(collection, "")).NewQuery().
		SelectIntoRecord(func() dalrecord.Record {
			return dalrecord.NewRecordWithData(dalrecord.NewKeyWithID(collection, ""), map[string]any{})
		})
}

func TestQuery_EvalAggregate(t *testing.T) {
	rows := []map[string]any{
		{"a": 10.0, "b": "x"},
		{"a": 20.0, "b": nil},
		{"a": "non-num", "b": "y"},
	}

	// COUNT(*)
	res, err := evalAggregate(mockAggFunc{name: dal.COUNT, args: []dal.Expression{mockUnsupportedExpr{}}}, rows)
	if err != nil || res != 3 {
		t.Fatalf("COUNT(*): want 3, got %v, err: %v", res, err)
	}

	// COUNT(field)
	res, err = evalAggregate(mockAggFunc{name: dal.COUNT, args: []dal.Expression{dal.Field("b")}}, rows)
	if err != nil || res != 2 {
		t.Fatalf("COUNT(b): want 2, got %v, err: %v", res, err)
	}

	// COUNT(nonFieldRef)
	res, err = evalAggregate(mockAggFunc{name: dal.COUNT, args: []dal.Expression{}}, rows)
	if err != nil || res != 0 {
		t.Fatalf("COUNT(): want 0, got %v, err: %v", res, err)
	}
	res, err = evalAggregate(mockAggFunc{name: dal.COUNT, args: []dal.Expression{mockUnsupportedExpr{}}}, rows[:0])
	if err != nil || res != 0 {
		t.Fatalf("COUNT() empty: want 0, got %v, err: %v", res, err)
	}
	// COUNT with multiple non-FieldRef args (line 286: continue)
	res, err = evalAggregate(mockAggFunc{name: dal.COUNT, args: []dal.Expression{mockUnsupportedExpr{}, mockUnsupportedExpr{}}}, rows)
	if err != nil || res != 0 {
		t.Fatalf("COUNT(multiple non-FieldRef): want 0, got %v", res)
	}

	// SUM and AVERAGE
	// len(args) == 0
	res, err = evalAggregate(mockAggFunc{name: dal.SUM, args: nil}, rows)
	if err != nil || res != nil {
		t.Fatalf("SUM no args: want nil, got %v", res)
	}
	// non-field arg
	_, err = evalAggregate(mockAggFunc{name: dal.SUM, args: []dal.Expression{mockUnsupportedExpr{}}}, rows)
	if err == nil {
		t.Fatal("SUM non-field arg: want error")
	}
	// cnt == 0
	res, err = evalAggregate(mockAggFunc{name: dal.SUM, args: []dal.Expression{dal.Field("missing")}}, rows)
	if err != nil || res != nil {
		t.Fatalf("SUM cnt 0: want nil, got %v", res)
	}
	// SUM valid
	res, err = evalAggregate(mockAggFunc{name: dal.SUM, args: []dal.Expression{dal.Field("a")}}, rows)
	if err != nil || res != 30.0 {
		t.Fatalf("SUM: want 30.0, got %v", res)
	}
	// AVERAGE valid
	res, err = evalAggregate(mockAggFunc{name: dal.AVERAGE, args: []dal.Expression{dal.Field("a")}}, rows)
	if err != nil || res != 15.0 {
		t.Fatalf("AVERAGE: want 15.0, got %v", res)
	}

	// MIN and MAX
	// len(args) == 0
	res, err = evalAggregate(mockAggFunc{name: dal.MIN, args: nil}, rows)
	if err != nil || res != nil {
		t.Fatalf("MIN no args: want nil, got %v", res)
	}
	// non-field arg
	_, err = evalAggregate(mockAggFunc{name: dal.MIN, args: []dal.Expression{mockUnsupportedExpr{}}}, rows)
	if err == nil {
		t.Fatal("MIN non-field arg: want error")
	}
	// !found
	res, err = evalAggregate(mockAggFunc{name: dal.MIN, args: []dal.Expression{dal.Field("missing")}}, rows)
	if err != nil || res != nil {
		t.Fatalf("MIN !found: want nil, got %v", res)
	}
	// MIN valid
	res, err = evalAggregate(mockAggFunc{name: dal.MIN, args: []dal.Expression{dal.Field("b")}}, rows)
	if err != nil || res != "x" {
		t.Fatalf("MIN: want x, got %v", res)
	}
	// MAX valid
	res, err = evalAggregate(mockAggFunc{name: dal.MAX, args: []dal.Expression{dal.Field("b")}}, rows)
	if err != nil || res != "y" {
		t.Fatalf("MAX: want y, got %v", res)
	}

	// unsupported aggregate
	_, err = evalAggregate(mockAggFunc{name: "MEDIAN"}, rows)
	if err == nil {
		t.Fatal("unsupported aggregate: want error")
	}
}

func TestQuery_MatchesHavingCondition(t *testing.T) {
	out := map[string]any{"cnt": 5, "alias_val": 10}
	rows := []map[string]any{{"orig_val": 20}}

	// Comparisons
	ops := []struct {
		op   dal.Operator
		r    any
		want bool
	}{
		{dal.Equal, 5, true},
		{dal.Equal, 6, false},
		{dal.GreaterThen, 4, true},
		{dal.GreaterThen, 5, false},
		{dal.GreaterOrEqual, 5, true},
		{dal.GreaterOrEqual, 6, false},
		{dal.LessThen, 6, true},
		{dal.LessThen, 5, false},
		{dal.LessOrEqual, 5, true},
		{dal.LessOrEqual, 4, false},
	}
	for _, tc := range ops {
		cmp := dal.Comparison{
			Left:     dal.Field("cnt"),
			Operator: tc.op,
			Right:    dal.Constant{Value: tc.r},
		}
		got, err := matchesHavingCondition(cmp, out, rows)
		if err != nil {
			t.Fatalf("matchesHavingCondition %v: %v", tc.op, err)
		}
		if got != tc.want {
			t.Fatalf("matchesHavingCondition %v: want %v, got %v", tc.op, tc.want, got)
		}
	}

	// Unsupported comparison operator
	cmpBadOp := dal.Comparison{
		Left:     dal.Field("cnt"),
		Operator: "LIKE",
		Right:    dal.Constant{Value: 5},
	}
	if _, err := matchesHavingCondition(cmpBadOp, out, rows); err == nil {
		t.Fatal("unsupported operator: want error")
	}

	// Comparison Left/Right error
	cmpBadLeft := dal.Comparison{
		Left:     mockUnsupportedExpr{},
		Operator: dal.Equal,
		Right:    dal.Constant{Value: 5},
	}
	if _, err := matchesHavingCondition(cmpBadLeft, out, rows); err == nil {
		t.Fatal("bad left: want error")
	}
	cmpBadRight := dal.Comparison{
		Left:     dal.Field("cnt"),
		Operator: dal.Equal,
		Right:    mockUnsupportedExpr{},
	}
	if _, err := matchesHavingCondition(cmpBadRight, out, rows); err == nil {
		t.Fatal("bad right: want error")
	}

	// GroupCondition
	// OR true
	gcOr := dal.NewGroupCondition(dal.Or, dal.Comparison{Left: dal.Field("cnt"), Operator: dal.Equal, Right: dal.Constant{Value: 5}})
	if ok, err := matchesHavingCondition(gcOr, out, rows); err != nil || !ok {
		t.Fatalf("GroupCondition OR true: want true, got %v, err: %v", ok, err)
	}

	// OR false
	gcOrFalse := dal.NewGroupCondition(dal.Or, dal.Comparison{Left: dal.Field("cnt"), Operator: dal.Equal, Right: dal.Constant{Value: 99}})
	if ok, err := matchesHavingCondition(gcOrFalse, out, rows); err != nil || ok {
		t.Fatalf("GroupCondition OR false: want false, got %v", ok)
	}

	// AND false
	gcAndFalse := dal.NewGroupCondition(dal.And, dal.Comparison{Left: dal.Field("cnt"), Operator: dal.Equal, Right: dal.Constant{Value: 99}})
	if ok, err := matchesHavingCondition(gcAndFalse, out, rows); err != nil || ok {
		t.Fatalf("GroupCondition AND false: want false, got %v", ok)
	}

	// AND true
	gcAndTrue := dal.NewGroupCondition(dal.And, dal.Comparison{Left: dal.Field("cnt"), Operator: dal.Equal, Right: dal.Constant{Value: 5}})
	if ok, err := matchesHavingCondition(gcAndTrue, out, rows); err != nil || !ok {
		t.Fatalf("GroupCondition AND true: want true, got %v", ok)
	}

	// GroupCondition sub error
	gcSubErr := dal.NewGroupCondition(dal.And, mockUnsupportedCond{})
	if _, err := matchesHavingCondition(gcSubErr, out, rows); err == nil {
		t.Fatal("GroupCondition sub error: want error")
	}

	// Unsupported condition
	if _, err := matchesHavingCondition(mockUnsupportedCond{}, out, rows); err == nil {
		t.Fatal("unsupported condition: want error")
	}
}

func TestQuery_ResolveHavingExpr(t *testing.T) {
	out := map[string]any{"alias": 42}
	rows := []map[string]any{{"field": 100}}

	// AggregateFunc
	val, err := resolveHavingExpr(mockAggFunc{name: dal.COUNT, args: []dal.Expression{dal.Field("field")}}, out, rows)
	if err != nil || val != 1 {
		t.Fatalf("AggregateFunc: want 1, got %v", val)
	}

	// FieldRef in out
	val, err = resolveHavingExpr(dal.Field("alias"), out, rows)
	if err != nil || val != 42 {
		t.Fatalf("FieldRef in out: want 42, got %v", val)
	}

	// FieldRef in rows
	val, err = resolveHavingExpr(dal.Field("field"), out, rows)
	if err != nil || val != 100 {
		t.Fatalf("FieldRef in rows: want 100, got %v", val)
	}

	// FieldRef in empty rows
	val, err = resolveHavingExpr(dal.Field("field"), out, nil)
	if err != nil || val != nil {
		t.Fatalf("FieldRef empty rows: want nil, got %v", val)
	}

	// Constant
	val, err = resolveHavingExpr(dal.Constant{Value: 99}, out, rows)
	if err != nil || val != 99 {
		t.Fatalf("Constant: want 99, got %v", val)
	}

	// Unsupported
	_, err = resolveHavingExpr(mockUnsupportedExpr{}, out, rows)
	if err == nil {
		t.Fatal("unsupported having expr: want error")
	}
}

func TestQuery_ResolveSimpleExprAndColumnOutKey(t *testing.T) {
	data := map[string]any{"foo": "bar"}

	// resolveSimpleExpr
	// $id
	v, err := resolveSimpleExpr(dal.Field("$id"), data, "k1")
	if err != nil || v != "k1" {
		t.Fatalf("resolve $id: want k1, got %v", v)
	}
	// Constant
	v, err = resolveSimpleExpr(dal.Constant{Value: 123}, data, "k1")
	if err != nil || v != 123 {
		t.Fatalf("resolve Constant: want 123, got %v", v)
	}
	// Unsupported
	_, err = resolveSimpleExpr(mockUnsupportedExpr{}, data, "k1")
	if err == nil {
		t.Fatal("resolve unsupported: want error")
	}

	// columnOutKey
	// alias
	if key := columnOutKey(dal.Column{Alias: "my_alias"}); key != "my_alias" {
		t.Fatalf("columnOutKey alias: want my_alias, got %s", key)
	}
	// FieldRef
	if key := columnOutKey(dal.Column{Expression: dal.Field("f1")}); key != "f1" {
		t.Fatalf("columnOutKey FieldRef: want f1, got %s", key)
	}
	// fallback Expression.String()
	if key := columnOutKey(dal.Column{Expression: mockUnsupportedExpr{}}); key != "unsupported_expr" {
		t.Fatalf("columnOutKey String: want unsupported_expr, got %s", key)
	}
}

func TestQuery_ResolveGroupExpr(t *testing.T) {
	// FieldRef with empty rows
	v, err := resolveGroupExpr(dal.Field("f"), nil)
	if err != nil || v != nil {
		t.Fatalf("resolveGroupExpr empty rows: want nil, got %v", v)
	}

	// Constant
	v, err = resolveGroupExpr(dal.Constant{Value: "const"}, nil)
	if err != nil || v != "const" {
		t.Fatalf("resolveGroupExpr Constant: want const, got %v", v)
	}

	// AggregateFunc (line 256)
	v, err = resolveGroupExpr(mockAggFunc{name: dal.COUNT, args: []dal.Expression{mockUnsupportedExpr{}}}, []map[string]any{{"a": 1}})
	if err != nil || v != 1 {
		t.Fatalf("resolveGroupExpr AggregateFunc: want 1, got %v", v)
	}

	// Unsupported
	_, err = resolveGroupExpr(mockUnsupportedExpr{}, nil)
	if err == nil {
		t.Fatal("resolveGroupExpr unsupported: want error")
	}
}

func TestQuery_EvaluateGroupCondition(t *testing.T) {
	// Unsupported group operator (line 787)
	gc := dal.NewGroupCondition(dal.Operator("UNKNOWN_OP"))
	_, err := evaluateGroupCondition(gc, map[string]any{}, "k")
	if err == nil {
		t.Fatal("evaluateGroupCondition unsupported: want error")
	}
}

func TestQuery_ApplyProtectedWhereContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), map[string]any{"a": 1})

	// ctx.Err()
	if _, err := applyProtectedWhereContext(ctx, []dalrecord.Record{rec}, nil); err == nil {
		t.Fatal("applyProtectedWhereContext canceled: want error")
	}

	// condeval.ToMap error
	badRec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("c", "1"), unmappableType{Ch: make(chan int)})
	cond := dal.Comparison{Left: dal.Field("a"), Operator: dal.Equal, Right: dal.Constant{Value: 1}}
	if _, err := applyProtectedWhereContext(context.Background(), []dalrecord.Record{badRec}, cond); err == nil {
		t.Fatal("applyProtectedWhereContext unmappable: want error")
	}

	// condeval.Match error
	badCond := dal.Comparison{Left: mockUnsupportedExpr{}, Operator: dal.Equal, Right: dal.Constant{Value: 1}}
	if _, err := applyProtectedWhereContext(context.Background(), []dalrecord.Record{rec}, badCond); err == nil {
		t.Fatal("applyProtectedWhereContext badCond: want error")
	}

	// applyWhereContext canceled ctx
	if _, err := applyWhereContext(ctx, []dalrecord.Record{rec}, cond); err == nil {
		t.Fatal("applyWhereContext canceled: want error")
	}
}

func TestQuery_ApplyGroupByContext_Advanced(t *testing.T) {
	rec1 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k1"), map[string]any{"grp": "A", "val": 10, "sub": 1})
	rec2 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k2"), map[string]any{"grp": "B", "val": 20, "sub": 2})
	rec3 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k3"), map[string]any{"grp": "C", "val": 20, "sub": 3})
	rec4 := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k4"), map[string]any{"grp": "D", "val": 20, "sub": 3})
	records := []dalrecord.Record{rec1, rec2, rec3, rec4}

	baseQ := newBaseQuery("items")

	// ctx.Err in partition loop (line 137)
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	sq := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
	}
	if _, err := applyGroupByContext(ctxCancel, sq, records, "items"); err == nil {
		t.Fatal("applyGroupByContext canceled: want error")
	}

	// groupKeyStr error (line 143)
	sqBadGroup := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{mockUnsupportedExpr{}},
	}
	if _, err := applyGroupByContext(context.Background(), sqBadGroup, records, "items"); err == nil {
		t.Fatal("applyGroupByContext bad group expr: want error")
	}

	// line 163: ctx.Err in projection loop
	stepCtxProj := &mockStepCancelContext{Context: context.Background(), cancelAt: 2}
	sqProjCancel := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customColumns:   []dal.Column{{Expression: dal.Field("grp")}},
	}
	if _, err := applyGroupByContext(stepCtxProj, sqProjCancel, records[:1], "items"); err == nil {
		t.Fatal("applyGroupByContext projection ctx cancel: want error")
	}

	// resolveGroupExpr error (line 169)
	sqBadCol := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customColumns:   []dal.Column{{Expression: mockUnsupportedExpr{}}},
	}
	if _, err := applyGroupByContext(context.Background(), sqBadCol, records, "items"); err == nil {
		t.Fatal("applyGroupByContext bad col expr: want error")
	}

	// line 181: ctx.Err in having loop
	stepCtxHaving := &mockStepCancelContext{Context: context.Background(), cancelAt: 3}
	sqHavingCancel := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customHaving:    dal.Comparison{Left: dal.Field("grp"), Operator: dal.Equal, Right: dal.Constant{Value: "A"}},
	}
	if _, err := applyGroupByContext(stepCtxHaving, sqHavingCancel, records[:1], "items"); err == nil {
		t.Fatal("applyGroupByContext having ctx cancel: want error")
	}

	// matchesHavingCondition error (line 185)
	sqBadHaving := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customHaving:    mockUnsupportedCond{},
	}
	if _, err := applyGroupByContext(context.Background(), sqBadHaving, records, "items"); err == nil {
		t.Fatal("applyGroupByContext bad having: want error")
	}

	// HAVING with matches and non-matches (lines 187-191)
	sqHavingFilter := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customColumns:   []dal.Column{{Expression: dal.Field("grp")}},
		customHaving:    dal.Comparison{Left: dal.Field("grp"), Operator: dal.Equal, Right: dal.Constant{Value: "A"}},
	}
	rHavingFilter, err := applyGroupByContext(context.Background(), sqHavingFilter, records, "items")
	if err != nil {
		t.Fatalf("having filter: %v", err)
	}
	recH, err := rHavingFilter.Next()
	if err != nil || recH.Data().(map[string]any)["grp"] != "A" {
		t.Fatalf("having filter next: want A, got %v", recH)
	}

	// OrderBy over groups (lines 197-213) with multiple groups kept, exercising:
	// - non-FieldRef order expr (!ok -> line 200 continue)
	// - descending order (c > 0 -> line 209)
	// - tie on first order expr (c == 0 -> line 206 continue)
	// - ascending order (c < 0 -> line 211)
	// - tie on all order exprs (return false -> line 213)
	// - limit < len(groups) (line 226)
	orderNonRef := dal.Ascending(mockUnsupportedExpr{})
	orderDescVal := dal.Descending(dal.Field("val"))
	orderAscSub := dal.Ascending(dal.Field("sub"))

	sqSort := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customColumns: []dal.Column{
			{Expression: dal.Field("grp")},
			{Expression: dal.Field("val")},
			{Expression: dal.Field("sub")},
		},
		customOrderBy: []dal.OrderExpression{orderNonRef, orderDescVal, orderAscSub},
		customLimit:   2,
	}
	reader, err := applyGroupByContext(context.Background(), sqSort, records, "items")
	if err != nil {
		t.Fatalf("applyGroupByContext sort: %v", err)
	}
	r1, err := reader.Next()
	if err != nil {
		t.Fatalf("reader.Next: %v", err)
	}
	d1 := r1.Data().(map[string]any)
	if d1["grp"] != "B" {
		t.Fatalf("expected B first (val 20, sub 2), got %v", d1["grp"])
	}

	// Offset and Limit branches:
	// offset >= len(groups)
	sqOffsetOver := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customOffset:    100,
	}
	rOffsetOver, err := applyGroupByContext(context.Background(), sqOffsetOver, records, "items")
	if err != nil {
		t.Fatalf("offset over: %v", err)
	}
	if _, err := rOffsetOver.Next(); err == nil {
		t.Fatal("expected no records")
	}

	// offset < len(groups) and limit < len(groups)
	sqOffsetLimit := mockStructuredQuery{
		StructuredQuery: baseQ,
		customGroupBy:   []dal.Expression{dal.Field("grp")},
		customOffset:    1,
		customLimit:     1,
	}
	rOffsetLimit, err := applyGroupByContext(context.Background(), sqOffsetLimit, records, "items")
	if err != nil {
		t.Fatalf("offset limit: %v", err)
	}
	if _, err := rOffsetLimit.Next(); err != nil {
		t.Fatalf("expected 1 record: %v", err)
	}
}

func TestQuery_ApplyProjectionContext(t *testing.T) {
	rec := dalrecord.NewRecordWithData(dalrecord.NewKeyWithID("items", "k1"), map[string]any{"a": 1})

	// ctx.Err() (line 436)
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := applyProjectionContext(ctxCancel, []dalrecord.Record{rec}, []dal.Column{{Alias: "a", Expression: dal.Field("a")}}, "items"); err == nil {
		t.Fatal("applyProjectionContext canceled: want error")
	}

	// resolveSimpleExpr error (line 444)
	if _, err := applyProjectionContext(context.Background(), []dalrecord.Record{rec}, []dal.Column{{Alias: "a", Expression: mockUnsupportedExpr{}}}, "items"); err == nil {
		t.Fatal("applyProjectionContext unsupported: want error")
	}
}

func TestQuery_ExecuteQuery_StoredOnlyAndContextBranches(t *testing.T) {
	dir := t.TempDir()
	colDef := &ingitdb.CollectionDef{
		ID:      "items",
		DirPath: filepath.Join(dir, "items"),
		RecordFile: &ingitdb.RecordFileDef{
			RecordType: ingitdb.SingleRecord,
			Format:     ingitdb.RecordFormatYAML,
			Name:       "{key}.yaml",
		},
	}
	def := &ingitdb.Definition{
		Collections: map[string]*ingitdb.CollectionDef{"items": colDef},
	}
	db := &Database{
		projectPath:     dir,
		storedOnlyReads: true,
	}
	tx := readonlyTx{
		db:  db,
		def: def,
	}

	recordPath := resolveRecordPath(colDef, "rec1")
	_ = os.MkdirAll(filepath.Dir(recordPath), 0o755)
	_ = os.WriteFile(recordPath, []byte("a: 1\n"), 0o644)

	baseQ := newBaseQuery("items")

	// Stored-only read error: unsupported RecordType (line 57)
	colDef.RecordFile.RecordType = "invalid_type"
	if _, err := executeQueryToRecordsReader(context.Background(), tx, baseQ); err == nil {
		t.Fatal("storedOnlyReads read error: want error")
	}

	// line 62: ctx.Err() in storedOnlyReads loop (stepCancelContext cancelAt 2)
	colDef.RecordFile.RecordType = ingitdb.SingleRecord
	stepCtxStored := &mockStepCancelContext{Context: context.Background(), cancelAt: 2}
	if _, err := executeQueryToRecordsReader(stepCtxStored, tx, baseQ); err == nil {
		t.Fatal("storedOnlyReads canceled ctx: want error")
	}

	// line 73: ctx.Err() after readAllRecordsFromDisk (when storedOnlyReads is false)
	db.storedOnlyReads = false
	stepCtxDisk := &mockStepCancelContext{Context: context.Background(), cancelAt: 2}
	if _, err := executeQueryToRecordsReader(stepCtxDisk, tx, baseQ); err == nil {
		t.Fatal("executeQueryToRecordsReader stepCtxDisk: want error")
	}

	// line 112: applyProjectionContext error
	qBadCol := mockStructuredQuery{
		StructuredQuery: baseQ,
		customFrom:      dal.From(dal.NewRootCollectionRef("items", "")),
		customColumns:   []dal.Column{{Expression: mockUnsupportedExpr{}}},
	}
	if _, err := executeQueryToRecordsReader(context.Background(), tx, qBadCol); err == nil {
		t.Fatal("executeQueryToRecordsReader bad projection: want error")
	}
}
