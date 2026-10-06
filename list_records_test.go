package dalgo2ingitdb

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dal-go/record"
	"github.com/ingitdb/ingitdb-go/ingitdb"
)

func listTestDef(dir string, format ingitdb.RecordFormat, name string) *ingitdb.CollectionDef {
	return &ingitdb.CollectionDef{ID: "items", DirPath: dir,
		Columns:      map[string]*ingitdb.ColumnDef{"code": {Type: ingitdb.ColumnTypeString}, "blob": {Type: ingitdb.ColumnTypeString}},
		ColumnsOrder: []string{"code", "blob"},
		RecordFile:   &ingitdb.RecordFileDef{Name: name, Format: format, RecordType: ingitdb.ListOfRecords},
		SourceSchema: &ingitdb.SourceSchemaDef{KeyMode: "export-ordinal", Fields: []ingitdb.SourceFieldDef{{Name: "code", Type: "string"}, {Name: "blob", Type: "bytes"}}},
	}
}

func TestPointReadsMatchExportedQueryTypes(t *testing.T) {
	for _, format := range []ingitdb.RecordFormat{ingitdb.RecordFormatJSON, ingitdb.RecordFormatJSONL, ingitdb.RecordFormatCSV} {
		t.Run(string(format), func(t *testing.T) {
			dir := t.TempDir()
			def := listTestDef(dir, format, "records."+string(format))
			var content string
			switch format {
			case ingitdb.RecordFormatJSON:
				def.RecordFile.RecordType = ingitdb.MapOfRecords
				content = `{"row-1":{"code":"hello","blob":"AP8="}}`
			case ingitdb.RecordFormatJSONL:
				content = `{"$ID":"row-1","code":"hello","blob":"AP8="}` + "\n"
			case ingitdb.RecordFormatCSV:
				def.RecordFile.CSVCellEncoding = "json-v1"
				def.ColumnsOrder = append([]string{"$ID"}, def.ColumnsOrder...)
				content = "$ID,code,blob\n\"\"\"row-1\"\"\",\"\"\"hello\"\"\",\"\"\"AP8=\"\"\"\n"
			}
			if err := os.WriteFile(filepath.Join(dir, def.RecordFile.Name), []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			tx := readonlyTx{def: &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"items": def}}}
			key := record.NewKeyWithID("items", "row-1")
			got := record.NewRecordWithData(key, map[string]any{})
			if err := tx.Get(context.Background(), got); err != nil {
				t.Fatal(err)
			}
			data := got.Data().(map[string]any)
			if data["code"] != "hello" || string(data["blob"].([]byte)) != string([]byte{0, 255}) {
				t.Fatalf("point data: %#v", data)
			}
			if exists, err := tx.Exists(context.Background(), key); err != nil || !exists {
				t.Fatalf("existing key: %v, %v", exists, err)
			}
			missing := record.NewKeyWithID("items", "missing")
			if exists, err := tx.Exists(context.Background(), missing); err != nil || exists {
				t.Fatalf("missing key: %v, %v", exists, err)
			}
			if err := tx.Get(context.Background(), record.NewRecordWithData(missing, map[string]any{})); !record.IsNotFound(err) {
				t.Fatalf("missing Get: %v", err)
			}
		})
	}
}

func TestPointReadsPropagateMalformedExportErrors(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	def := listTestDef(dir, ingitdb.RecordFormatJSONL, "records.jsonl")
	path := filepath.Join(dir, def.RecordFile.Name)
	tx := readonlyTx{def: &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"items": def}}}
	key := record.NewKeyWithID("items", "row-1")
	newRecord := func() record.Record { return record.NewRecordWithData(key, map[string]any{}) }
	if err := os.WriteFile(path, []byte("bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tx.Get(ctx, newRecord()); err == nil {
		t.Fatal("Get accepted malformed list")
	}
	if _, err := tx.Exists(ctx, key); err == nil {
		t.Fatal("Exists accepted malformed list")
	}
	valid := []byte(`{"$ID":"row-1","code":"hello","blob":"AP8="}` + "\n")
	if err := os.WriteFile(path, valid, 0o644); err != nil {
		t.Fatal(err)
	}
	def.Columns["bad"] = &ingitdb.ColumnDef{Type: ingitdb.ColumnTypeInt, Formula: "1 / 0"}
	if err := tx.Get(ctx, newRecord()); err == nil {
		t.Fatal("Get accepted broken formula")
	}
	delete(def.Columns, "bad")
	badTarget := record.NewRecordWithData(key, &struct {
		Blob int `json:"blob"`
	}{})
	if err := tx.Get(ctx, badTarget); err == nil {
		t.Fatal("Get accepted incompatible target type")
	}
	mapDef := listTestDef(t.TempDir(), ingitdb.RecordFormatJSON, "records.json")
	mapDef.RecordFile.RecordType = ingitdb.MapOfRecords
	if err := os.WriteFile(filepath.Join(mapDef.DirPath, mapDef.RecordFile.Name), []byte(`{"row-1":{"code":"hello","blob":"!"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	mapTx := readonlyTx{def: &ingitdb.Definition{Collections: map[string]*ingitdb.CollectionDef{"items": mapDef}}}
	if err := mapTx.Get(ctx, newRecord()); err == nil || !strings.Contains(err.Error(), "decode source bytes") {
		t.Fatalf("invalid map BLOB: %v", err)
	}
}

func TestReadAllListStoredJSONLAndCSV(t *testing.T) {
	for _, tc := range []struct {
		name    string
		format  ingitdb.RecordFormat
		content string
	}{
		{"jsonl", ingitdb.RecordFormatJSONL, `{"$ID":"row-1","code":"hello","blob":"AP8="}` + "\n"},
		{"csv", ingitdb.RecordFormatCSV, "$ID,code,blob\n\"\"\"row-1\"\"\",\"\"\"hello\"\"\",\"\"\"AP8=\"\"\"\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			def := listTestDef(dir, tc.format, "records."+tc.name)
			if tc.format == ingitdb.RecordFormatCSV {
				def.RecordFile.CSVCellEncoding = "json-v1"
				def.ColumnsOrder = append([]string{"$ID"}, def.ColumnsOrder...)
			}
			if err := os.WriteFile(filepath.Join(dir, def.RecordFile.Name), []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			rows, err := readAllListStored(def)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0].Key != "row-1" || rows[0].Stored["code"] != "hello" || string(rows[0].Stored["blob"].([]byte)) != string([]byte{0, 255}) {
				t.Fatalf("rows: %#v", rows)
			}
			if _, exists := rows[0].Stored["$ID"]; exists {
				t.Fatalf("transport key leaked: %#v", rows[0].Stored)
			}
			all, err := readAllRecordsFromDisk(def)
			if err != nil || len(all) != 1 {
				t.Fatalf("query rows: %d, %v", len(all), err)
			}
			stored, err := readAllStoredRecords(def)
			if err != nil || len(stored) != 1 {
				t.Fatalf("stored query rows: %d, %v", len(stored), err)
			}
		})
	}
}

func TestReadAllListStoredFailures(t *testing.T) {
	for _, tc := range []struct{ name, content, want string }{
		{"malformed", `{"$ID":`, "parse"},
		{"missing-id", `{"code":"x","blob":"AP8="}` + "\n", "no transport ID"},
		{"duplicate", `{"$ID":"x","code":"a","blob":"AP8="}` + "\n" + `{"$ID":"x","code":"b","blob":"AP8="}` + "\n", "duplicate"},
		{"invalid-blob", `{"$ID":"x","code":"a","blob":"!"}` + "\n", "decode source bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			def := listTestDef(dir, ingitdb.RecordFormatJSONL, "records.jsonl")
			if err := os.WriteFile(filepath.Join(dir, def.RecordFile.Name), []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := readAllListStored(def); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error: %v", err)
			}
		})
	}
	def := listTestDef(t.TempDir(), ingitdb.RecordFormatJSONL, "missing.jsonl")
	if rows, err := readAllListStored(def); err != nil || len(rows) != 0 {
		t.Fatalf("missing records should be empty: %v, %v", rows, err)
	}
	def.RecordFile.Name = "directory"
	if err := os.Mkdir(filepath.Join(def.DirPath, "directory"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllListStored(def); err == nil {
		t.Fatal("directory accepted as records file")
	}
	loop := filepath.Join(def.DirPath, "loop")
	if err := os.Symlink("loop", loop); err != nil {
		t.Fatal(err)
	}
	def.RecordFile.Name = "loop"
	if _, err := readAllListStored(def); err == nil {
		t.Fatal("symlink loop accepted")
	}
	bad := listTestDef(t.TempDir(), ingitdb.RecordFormatJSONL, "records.jsonl")
	if err := os.WriteFile(filepath.Join(bad.DirPath, bad.RecordFile.Name), []byte("bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllRecordsFromDisk(bad); err == nil {
		t.Fatal("bad list query accepted")
	}
	csv := listTestDef(t.TempDir(), ingitdb.RecordFormatCSV, "records.csv")
	csv.RecordFile.CSVCellEncoding = "json-v1"
	csv.ColumnsOrder = append([]string{"$ID"}, csv.ColumnsOrder...)
	if err := os.WriteFile(filepath.Join(csv.DirPath, csv.RecordFile.Name), []byte("wrong,header\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllListStored(csv); err == nil {
		t.Fatal("malformed CSV accepted")
	}
	mapDef := listTestDef(t.TempDir(), ingitdb.RecordFormatJSON, "records.json")
	mapDef.RecordFile.RecordType = ingitdb.MapOfRecords
	if err := os.WriteFile(filepath.Join(mapDef.DirPath, mapDef.RecordFile.Name), []byte(`{"x":{"code":"x","blob":"!"}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readAllMapStored(mapDef); err == nil {
		t.Fatal("invalid BLOB carrier accepted")
	}
}

func TestDecodeSourceTransport(t *testing.T) {
	def := listTestDef(t.TempDir(), ingitdb.RecordFormatJSONL, "records.jsonl")
	for _, tc := range []struct {
		name    string
		value   any
		wantErr bool
	}{
		{"nil", nil, false}, {"bytes", []byte{0, 255}, false}, {"valid", base64.StdEncoding.EncodeToString([]byte{0, 255}), false}, {"invalid", "!", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]any{"blob": tc.value}
			err := decodeSourceTransport(def, fields)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error: %v", err)
			}
		})
	}
	def.SourceSchema = nil
	if err := decodeSourceTransport(def, map[string]any{"blob": "!"}); err != nil {
		t.Fatal(err)
	}
}
