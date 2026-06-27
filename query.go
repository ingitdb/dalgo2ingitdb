package dalgo2ingitdb

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/dal-go/dalgo/dal"

	"github.com/ingitdb/ingitdb-go/ingitdb"
)

// executeQueryToRecordsReader reads every record in the collection
// referenced by query.From(), applies WHERE / GROUP BY / HAVING / ORDER BY /
// LIMIT / column projection in memory, and returns a slice-backed RecordsReader.
func executeQueryToRecordsReader(_ context.Context, r readonlyTx, query dal.Query) (dal.RecordsReader, error) {
	colDef, err := collectionFromQuery(r.def, query)
	if err != nil {
		return nil, err
	}
	// collectionFromQuery already validated that query is a StructuredQuery.
	sq, _ := query.(dal.StructuredQuery)

	records, err := readAllRecordsFromDisk(colDef)
	if err != nil {
		return nil, err
	}

	if cond := sq.Where(); cond != nil {
		records, err = applyWhere(records, cond)
		if err != nil {
			return nil, err
		}
	}

	// GROUP BY path: partition, aggregate, HAVING, ORDER BY, LIMIT, project.
	if groupBy := sq.GroupBy(); len(groupBy) > 0 {
		return applyGroupBy(sq, records, colDef.ID)
	}

	if orderBy := sq.OrderBy(); len(orderBy) > 0 {
		applyOrderBy(records, orderBy)
	}
	if limit := sq.Limit(); limit > 0 && len(records) > limit {
		records = records[:limit]
	}

	// Column projection (no GROUP BY).
	if columns := sq.Columns(); len(columns) > 0 {
		records, err = applyProjection(records, columns, colDef.ID)
		if err != nil {
			return nil, err
		}
	}

	return newSliceRecordsReader(records), nil
}

// applyGroupBy partitions records into groups keyed by the GROUP BY
// expressions, computes aggregate columns, applies HAVING, ORDER BY, LIMIT, and
// returns projected group records.
func applyGroupBy(sq dal.StructuredQuery, records []dal.Record, collection string) (dal.RecordsReader, error) {
	groupBy := sq.GroupBy()
	columns := sq.Columns()

	// Partition: preserve first-seen insertion order.
	type group struct {
		rows []map[string]any
		keys []map[string]any // parallel, for reading back group-key fields
		out  map[string]any
	}
	order := make([]string, 0)
	byKey := make(map[string]*group)

	for _, rec := range records {
		data := rec.Data().(map[string]any)
		recKey := fmt.Sprintf("%v", rec.Key().ID)
		gk, err := groupKeyStr(groupBy, data, recKey)
		if err != nil {
			return nil, err
		}
		g := byKey[gk]
		if g == nil {
			g = &group{}
			byKey[gk] = g
			order = append(order, gk)
		}
		g.rows = append(g.rows, data)
		g.keys = append(g.keys, map[string]any{"$id": recKey})
	}

	groups := make([]*group, len(order))
	for i, k := range order {
		groups[i] = byKey[k]
	}

	// Project each group's selected columns.
	for _, g := range groups {
		out := make(map[string]any, len(columns))
		for _, col := range columns {
			val, err := resolveGroupExpr(col.Expression, g.rows)
			if err != nil {
				return nil, err
			}
			out[columnOutKey(col)] = val
		}
		g.out = out
	}

	// HAVING: filter on projected output + re-evaluable aggregates.
	if having := sq.Having(); having != nil {
		kept := groups[:0]
		for _, g := range groups {
			ok, err := matchesHavingCondition(having, g.out, g.rows)
			if err != nil {
				return nil, err
			}
			if ok {
				kept = append(kept, g)
			}
		}
		groups = kept
	}

	// ORDER BY over projected output rows.
	if orderBy := sq.OrderBy(); len(orderBy) > 0 {
		sort.SliceStable(groups, func(i, j int) bool {
			for _, oe := range orderBy {
				f, ok := oe.Expression().(dal.FieldRef)
				if !ok {
					continue
				}
				vI := groups[i].out[f.Name()]
				vJ := groups[j].out[f.Name()]
				c := compareValues(vI, vJ)
				if c == 0 {
					continue
				}
				if oe.Descending() {
					return c > 0
				}
				return c < 0
			}
			return false
		})
	}

	// OFFSET / LIMIT.
	if offset := sq.Offset(); offset > 0 {
		if offset >= len(groups) {
			groups = nil
		} else {
			groups = groups[offset:]
		}
	}
	if limit := sq.Limit(); limit > 0 && limit < len(groups) {
		groups = groups[:limit]
	}

	result := make([]dal.Record, len(groups))
	for i, g := range groups {
		key := dal.NewKeyWithID(collection, fmt.Sprint(i))
		result[i] = dal.NewRecordWithData(key, g.out).SetError(nil)
	}
	return newSliceRecordsReader(result), nil
}

// groupKeyStr builds a stable partition key from the GROUP BY expressions.
func groupKeyStr(groupBy []dal.Expression, data map[string]any, recKey string) (string, error) {
	parts := make([]string, len(groupBy))
	for i, ge := range groupBy {
		v, err := resolveSimpleExpr(ge, data, recKey)
		if err != nil {
			return "", err
		}
		parts[i] = fmt.Sprintf("%T:%v", v, v)
	}
	return strings.Join(parts, "\x00"), nil
}

// resolveGroupExpr evaluates an expression for a group: aggregates over all
// member rows; FieldRef reads the first row's value (a GROUP BY key column);
// Constant returns the value.
func resolveGroupExpr(e dal.Expression, rows []map[string]any) (any, error) {
	switch ex := e.(type) {
	case dal.AggregateFunc:
		return evalAggregate(ex, rows)
	case dal.FieldRef:
		if len(rows) == 0 {
			return nil, nil
		}
		return rows[0][ex.Name()], nil
	case dal.Constant:
		return ex.Value, nil
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: unsupported grouped expression %T", e)
	}
}

// evalAggregate computes one aggregate function over a group's rows.
func evalAggregate(af dal.AggregateFunc, rows []map[string]any) (any, error) {
	args := af.FuncArgs()
	switch af.FuncName() {
	case dal.COUNT:
		// COUNT(*): non-field arg (star) → count all rows.
		if len(args) == 1 {
			if _, isField := args[0].(dal.FieldRef); !isField {
				return len(rows), nil
			}
		}
		// COUNT(field): count non-null values.
		n := 0
		for _, row := range rows {
			if len(args) > 0 {
				f, ok := args[0].(dal.FieldRef)
				if !ok {
					continue
				}
				v := row[f.Name()]
				if v != nil {
					n++
				}
			}
		}
		return n, nil
	case dal.SUM, dal.AVERAGE:
		if len(args) == 0 {
			return nil, nil
		}
		f, ok := args[0].(dal.FieldRef)
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: %s requires a field argument", af.FuncName())
		}
		sum, cnt := 0.0, 0
		for _, row := range rows {
			v := row[f.Name()]
			if v == nil {
				continue
			}
			n, ok := toFloat64(v)
			if !ok {
				continue
			}
			sum += n
			cnt++
		}
		if cnt == 0 {
			return nil, nil
		}
		if af.FuncName() == dal.AVERAGE {
			return sum / float64(cnt), nil
		}
		return sum, nil
	case dal.MIN, dal.MAX:
		if len(args) == 0 {
			return nil, nil
		}
		f, ok := args[0].(dal.FieldRef)
		if !ok {
			return nil, fmt.Errorf("dalgo2ingitdb: %s requires a field argument", af.FuncName())
		}
		var best any
		found := false
		for _, row := range rows {
			v := row[f.Name()]
			if v == nil {
				continue
			}
			if !found {
				best, found = v, true
				continue
			}
			c := compareValues(v, best)
			if (af.FuncName() == dal.MIN && c < 0) || (af.FuncName() == dal.MAX && c > 0) {
				best = v
			}
		}
		if !found {
			return nil, nil
		}
		return best, nil
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: unsupported aggregate %q", af.FuncName())
	}
}

// matchesHavingCondition evaluates a HAVING condition against a projected group
// output row plus the group's member rows (needed to re-evaluate aggregate
// expressions that appear in HAVING directly, not via a SELECT alias).
func matchesHavingCondition(cond dal.Condition, out map[string]any, rows []map[string]any) (bool, error) {
	switch c := cond.(type) {
	case dal.Comparison:
		l, err := resolveHavingExpr(c.Left, out, rows)
		if err != nil {
			return false, err
		}
		r, err := resolveHavingExpr(c.Right, out, rows)
		if err != nil {
			return false, err
		}
		cmp := compareValues(l, r)
		switch c.Operator {
		case dal.Equal:
			return cmp == 0, nil
		case dal.GreaterThen:
			return cmp > 0, nil
		case dal.GreaterOrEqual:
			return cmp >= 0, nil
		case dal.LessThen:
			return cmp < 0, nil
		case dal.LessOrEqual:
			return cmp <= 0, nil
		default:
			return false, fmt.Errorf("dalgo2ingitdb: unsupported HAVING operator %q", c.Operator)
		}
	case dal.GroupCondition:
		for _, sub := range c.Conditions() {
			ok, err := matchesHavingCondition(sub, out, rows)
			if err != nil {
				return false, err
			}
			if c.Operator() == dal.Or {
				if ok {
					return true, nil
				}
			} else {
				if !ok {
					return false, nil
				}
			}
		}
		return c.Operator() != dal.Or, nil
	default:
		return false, fmt.Errorf("dalgo2ingitdb: unsupported HAVING condition type %T", cond)
	}
}

// resolveHavingExpr resolves an expression in HAVING context: aggregates are
// re-evaluated over the group rows (handles COUNT(*) > 1 style); FieldRefs
// check the projected output row first (alias lookup), then fall back to the
// first member row; Constants yield their value directly.
func resolveHavingExpr(e dal.Expression, out map[string]any, rows []map[string]any) (any, error) {
	switch ex := e.(type) {
	case dal.AggregateFunc:
		return evalAggregate(ex, rows)
	case dal.FieldRef:
		// Check projected output (SELECT alias or GROUP BY column) first.
		if v, ok := out[ex.Name()]; ok {
			return v, nil
		}
		if len(rows) > 0 {
			return rows[0][ex.Name()], nil
		}
		return nil, nil
	case dal.Constant:
		return ex.Value, nil
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: unsupported HAVING expression type %T", e)
	}
}

// applyProjection reduces each record's data map to only the selected columns.
func applyProjection(records []dal.Record, columns []dal.Column, collection string) ([]dal.Record, error) {
	result := make([]dal.Record, len(records))
	for i, rec := range records {
		data := rec.Data().(map[string]any)
		recKey := fmt.Sprintf("%v", rec.Key().ID)
		out := make(map[string]any, len(columns))
		for _, col := range columns {
			v, err := resolveSimpleExpr(col.Expression, data, recKey)
			if err != nil {
				return nil, err
			}
			out[columnOutKey(col)] = v
		}
		key := dal.NewKeyWithID(collection, recKey)
		result[i] = dal.NewRecordWithData(key, out).SetError(nil)
	}
	return result, nil
}

// columnOutKey returns the output map key for a projected column: Alias, then
// FieldRef name, then the expression's string form.
func columnOutKey(col dal.Column) string {
	if col.Alias != "" {
		return col.Alias
	}
	if f, ok := col.Expression.(dal.FieldRef); ok {
		return f.Name()
	}
	return col.Expression.String()
}

// resolveSimpleExpr resolves a single expression against a flat data map.
func resolveSimpleExpr(e dal.Expression, data map[string]any, recKey string) (any, error) {
	switch ex := e.(type) {
	case dal.FieldRef:
		if ex.Name() == "$id" {
			return recKey, nil
		}
		return data[ex.Name()], nil
	case dal.Constant:
		return ex.Value, nil
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: unsupported expression type %T", e)
	}
}

// collectionFromQuery resolves the single collection a structured query reads
// from, validating the FROM clause and that the collection exists with a record
// file. Shared by the records-reader and recordset-reader query paths.
func collectionFromQuery(def *ingitdb.Definition, query dal.Query) (*ingitdb.CollectionDef, error) {
	if def == nil {
		return nil, fmt.Errorf("dalgo2ingitdb: transaction has no loaded definition")
	}
	sq, ok := query.(dal.StructuredQuery)
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: only StructuredQuery is supported, got %T", query)
	}
	fromSrc := sq.From()
	if fromSrc == nil {
		return nil, fmt.Errorf("dalgo2ingitdb: query has no FROM clause")
	}
	base := fromSrc.Base()
	colRef, ok := base.(dal.CollectionRef)
	if !ok {
		return nil, fmt.Errorf("dalgo2ingitdb: FROM source must be a CollectionRef, got %T", base)
	}
	collectionID := colRef.Name()
	colDef, exists := def.Collections[collectionID]
	if !exists {
		return nil, fmt.Errorf("dalgo2ingitdb: collection %q not found in definition", collectionID)
	}
	if colDef.RecordFile == nil {
		return nil, fmt.Errorf("dalgo2ingitdb: collection %q has no record_file definition", collectionID)
	}
	return colDef, nil
}

func readAllRecordsFromDisk(colDef *ingitdb.CollectionDef) ([]dal.Record, error) {
	switch colDef.RecordFile.RecordType {
	case ingitdb.SingleRecord:
		return readAllSingleRecords(colDef)
	case ingitdb.MapOfRecords:
		return readAllMapOfRecords(colDef)
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: query unsupported for record type %q", colDef.RecordFile.RecordType)
	}
}

// readAllSingleRecords reads every single-record file and bakes computed
// columns into each returned dal.Record.
func readAllSingleRecords(colDef *ingitdb.CollectionDef) ([]dal.Record, error) {
	stored, err := readAllSingleStored(colDef)
	if err != nil {
		return nil, err
	}
	return bakeStoredRecords(colDef, stored)
}

// readAllMapOfRecords reads a map-of-records file and bakes computed columns
// into each returned dal.Record.
func readAllMapOfRecords(colDef *ingitdb.CollectionDef) ([]dal.Record, error) {
	stored, err := readAllMapStored(colDef)
	if err != nil {
		return nil, err
	}
	return bakeStoredRecords(colDef, stored)
}

// bakeStoredRecords evaluates every computed column for each stored record and
// returns dal.Records carrying the merged stored+computed map.
//
// This is NOT on the read path of the select/delete/update/TUI consumers — those
// resolve computed values lazily via ExecuteQueryToRecordsetReader. It is
// retained for the write-time computed-foreign-key validation (Set/Delete),
// which is explicitly out of scope for the lazy migration and must keep
// evaluating computed FK columns to enforce referential integrity.
func bakeStoredRecords(colDef *ingitdb.CollectionDef, stored []KeyedStored) ([]dal.Record, error) {
	records := make([]dal.Record, 0, len(stored))
	for _, s := range stored {
		computed, computeErr := ApplyFormulasToRead(s.Stored, colDef.Columns, colDef.ID, s.Key)
		if computeErr != nil {
			return nil, computeErr
		}
		key := dal.NewKeyWithID(colDef.ID, s.Key)
		rec := dal.NewRecordWithData(key, computed)
		rec.SetError(nil)
		records = append(records, rec)
	}
	return records, nil
}

// readAllStoredRecords reads every record in the collection from disk and
// returns each one's locale-normalized stored fields, WITHOUT evaluating any
// computed column.
func readAllStoredRecords(colDef *ingitdb.CollectionDef) ([]KeyedStored, error) {
	switch colDef.RecordFile.RecordType {
	case ingitdb.SingleRecord:
		return readAllSingleStored(colDef)
	case ingitdb.MapOfRecords:
		return readAllMapStored(colDef)
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: query unsupported for record type %q", colDef.RecordFile.RecordType)
	}
}

func readAllSingleStored(colDef *ingitdb.CollectionDef) ([]KeyedStored, error) {
	nameTemplate := colDef.RecordFile.Name
	globPattern := strings.ReplaceAll(nameTemplate, "{key}", "*")
	basePath := filepath.Join(colDef.DirPath, colDef.RecordFile.RecordsBasePath())
	matches, err := filepath.Glob(filepath.Join(basePath, globPattern))
	if err != nil {
		return nil, fmt.Errorf("glob %s: %w", basePath, err)
	}
	keyExtractor, err := buildKeyExtractor(nameTemplate)
	if err != nil {
		return nil, err
	}
	stored := make([]KeyedStored, 0, len(matches))
	for _, match := range matches {
		if colDef.RecordFile.IsExcluded(filepath.Base(match)) {
			continue
		}
		relPath, relErr := filepathRel(basePath, match)
		if relErr != nil {
			return nil, fmt.Errorf("rel %s: %w", match, relErr)
		}
		recordKey := keyExtractor(relPath)
		if recordKey == "" {
			continue
		}
		data, found, readErr := readSingleRecord(match, colDef)
		if readErr != nil {
			return nil, readErr
		}
		if !found {
			continue
		}
		normalized := ingitdb.ApplyLocaleToRead(data, colDef.Columns)
		stored = append(stored, KeyedStored{Key: recordKey, Stored: normalized})
	}
	return stored, nil
}

func readAllMapStored(colDef *ingitdb.CollectionDef) ([]KeyedStored, error) {
	path := resolveRecordPath(colDef, "")
	allData, err := readMapOfRecordsFile(path, colDef.RecordFile.Format)
	if err != nil {
		return nil, err
	}
	stored := make([]KeyedStored, 0, len(allData))
	for id, fields := range allData {
		normalized := ingitdb.ApplyLocaleToRead(fields, colDef.Columns)
		stored = append(stored, KeyedStored{Key: id, Stored: normalized})
	}
	return stored, nil
}

// buildKeyExtractor returns a function that recovers the record key from
// a path relative to the records base directory.
func buildKeyExtractor(nameTemplate string) (func(relPath string) string, error) {
	idx := strings.Index(nameTemplate, "{key}")
	if idx < 0 {
		return func(relPath string) string {
			return strings.TrimSuffix(filepath.Base(relPath), filepath.Ext(relPath))
		}, nil
	}
	prefix := regexp.QuoteMeta(nameTemplate[:idx])
	remainder := nameTemplate[idx+len("{key}"):]
	suffix := regexp.QuoteMeta(strings.ReplaceAll(remainder, "{key}", "\x00"))
	suffix = strings.ReplaceAll(suffix, "\x00", ".*")
	re, err := regexpCompile("^" + prefix + "(.*?)" + suffix + "$")
	if err != nil {
		return nil, fmt.Errorf("build key extractor for %q: %w", nameTemplate, err)
	}
	return func(relPath string) string {
		normalised := filepath.ToSlash(relPath)
		m := re.FindStringSubmatch(normalised)
		if m == nil {
			return ""
		}
		return m[1]
	}, nil
}

func applyWhere(records []dal.Record, cond dal.Condition) ([]dal.Record, error) {
	filtered := records[:0]
	for _, rec := range records {
		data := rec.Data().(map[string]any)
		recKey := fmt.Sprintf("%v", rec.Key().ID)
		match, err := evaluateCondition(cond, data, recKey)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, rec)
		}
	}
	return filtered, nil
}

func applyOrderBy(records []dal.Record, orderBy []dal.OrderExpression) {
	sort.SliceStable(records, func(i, j int) bool {
		dataI := records[i].Data().(map[string]any)
		dataJ := records[j].Data().(map[string]any)
		keyI := fmt.Sprintf("%v", records[i].Key().ID)
		keyJ := fmt.Sprintf("%v", records[j].Key().ID)
		for _, expr := range orderBy {
			fieldRef, isRef := expr.Expression().(dal.FieldRef)
			if !isRef {
				continue
			}
			fieldName := fieldRef.Name()
			var vI, vJ any
			if fieldName == "$id" {
				vI, vJ = keyI, keyJ
			} else {
				vI = dataI[fieldName]
				vJ = dataJ[fieldName]
			}
			cmp := compareValues(vI, vJ)
			if cmp == 0 {
				continue
			}
			if expr.Descending() {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func evaluateCondition(cond dal.Condition, data map[string]any, recordKey string) (bool, error) {
	switch c := cond.(type) {
	case dal.Comparison:
		return evaluateComparison(c, data, recordKey)
	case dal.GroupCondition:
		return evaluateGroupCondition(c, data, recordKey)
	default:
		return false, fmt.Errorf("dalgo2ingitdb: unsupported condition type %T", cond)
	}
}

func evaluateGroupCondition(gc dal.GroupCondition, data map[string]any, recordKey string) (bool, error) {
	switch gc.Operator() {
	case dal.And:
		for _, cond := range gc.Conditions() {
			match, err := evaluateCondition(cond, data, recordKey)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		}
		return true, nil
	// The dal.Or case is commented out because no public dal API can produce an Or
	// group condition: dal.GroupCondition has unexported fields and the only
	// exported builder (dal.QueryBuilder) always emits the And operator; Or is
	// reachable only via the unexported structuredQuery.Or method. An Or condition
	// would therefore fall through to the default below. Restore this case if dalgo
	// ever exposes Or:
	//
	// case dal.Or:
	// 	for _, cond := range gc.Conditions() {
	// 		match, err := evaluateCondition(cond, data, recordKey)
	// 		if err != nil {
	// 			return false, err
	// 		}
	// 		if match {
	// 			return true, nil
	// 		}
	// 	}
	// 	return false, nil
	default:
		// untestable: the public dal API only ever builds And group conditions, so
		// no operator other than And can reach this guard.
		return false, fmt.Errorf("dalgo2ingitdb: unsupported group operator %q", gc.Operator())
	}
}

func evaluateComparison(c dal.Comparison, data map[string]any, recordKey string) (bool, error) {
	leftVal, err := resolveExpression(c.Left, data, recordKey)
	if err != nil {
		return false, err
	}
	rightVal, err := resolveExpression(c.Right, data, recordKey)
	if err != nil {
		return false, err
	}
	cmp := compareValues(leftVal, rightVal)
	switch c.Operator {
	case dal.Equal:
		return cmp == 0, nil
	case dal.GreaterThen:
		return cmp > 0, nil
	case dal.GreaterOrEqual:
		return cmp >= 0, nil
	case dal.LessThen:
		return cmp < 0, nil
	case dal.LessOrEqual:
		return cmp <= 0, nil
	default:
		return false, fmt.Errorf("dalgo2ingitdb: unsupported operator %q", c.Operator)
	}
}

func resolveExpression(expr dal.Expression, data map[string]any, recordKey string) (any, error) {
	switch e := expr.(type) {
	case dal.FieldRef:
		if e.Name() == "$id" {
			return recordKey, nil
		}
		return data[e.Name()], nil
	case dal.Constant:
		return e.Value, nil
	default:
		return nil, fmt.Errorf("dalgo2ingitdb: unsupported expression type %T", expr)
	}
}

func compareValues(a, b any) int {
	aFloat, aIsNum := toFloat64(a)
	bFloat, bIsNum := toFloat64(b)
	if aIsNum && bIsNum {
		switch {
		case aFloat < bFloat:
			return -1
		case aFloat > bFloat:
			return 1
		default:
			return 0
		}
	}
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
