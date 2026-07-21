package dalgo2ingitdb

// update_fieldpath_test.go contains white-box, table-driven tests for the
// full update-operation surface of applyUpdates/applyFieldUpdate.
//
// Covered:
//   - Multi-segment FieldPath set with auto-created intermediate maps
//   - Nested delete via DeleteField sentinel
//   - Delete of a missing path is a no-op (no error)
//   - dal.Increment on existing, missing (nil→0), and non-numeric fields
//   - update.ServerTimestamp sentinel → RFC3339Nano UTC string
//   - Mixed batch: multiple updates applied in order
//   - UpdateMulti: same updates applied to multiple keys via the TX method
//   - Non-map intermediate on a set path → clear error
//   - FieldName-based variants still work (regression)

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
	"github.com/dal-go/record/update"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// applyUpdates unit tests (pure in-memory, no I/O)
// ---------------------------------------------------------------------------

func TestApplyUpdates_NestedPathSetCreatesIntermediates(t *testing.T) {
	t.Parallel()
	data := map[string]any{}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"meta", "stats", "views"}, 42),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates: %v", err)
	}
	meta, ok := data["meta"].(map[string]any)
	if !ok {
		t.Fatalf("data[meta] type: got %T, want map[string]any", data["meta"])
	}
	stats, ok := meta["stats"].(map[string]any)
	if !ok {
		t.Fatalf("data[meta][stats] type: got %T, want map[string]any", meta["stats"])
	}
	if stats["views"] != 42 {
		t.Errorf("data[meta][stats][views]: got %v, want 42", stats["views"])
	}
}

func TestApplyUpdates_NestedPathSetUpdatesExistingLeaf(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"meta": map[string]any{"version": 1},
	}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"meta", "version"}, 2),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates: %v", err)
	}
	meta := data["meta"].(map[string]any)
	if meta["version"] != 2 {
		t.Errorf("meta[version]: got %v, want 2", meta["version"])
	}
}

func TestApplyUpdates_NestedDelete(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"members": map[string]any{
			"alice": true,
			"bob":   true,
		},
	}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"members", "alice"}, update.DeleteField),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates DeleteField nested: %v", err)
	}
	members := data["members"].(map[string]any)
	if _, present := members["alice"]; present {
		t.Error("alice should have been deleted")
	}
	if members["bob"] != true {
		t.Error("bob should survive the deletion")
	}
}

func TestApplyUpdates_DeleteMissingPathIsNoOp(t *testing.T) {
	t.Parallel()
	data := map[string]any{"x": 1}
	ups := []update.Update{
		// "a" doesn't exist, so "a.b" is definitely missing
		update.ByFieldPath(update.FieldPath{"a", "b"}, update.DeleteField),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates DeleteField missing path: %v", err)
	}
	// data must be unchanged
	if len(data) != 1 || data["x"] != 1 {
		t.Errorf("data changed unexpectedly: %v", data)
	}
}

func TestApplyUpdates_DeleteMissingTopLevelIsNoOp(t *testing.T) {
	t.Parallel()
	data := map[string]any{"x": 1}
	ups := []update.Update{
		update.DeleteByFieldName("nonexistent"),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates DeleteField top-level missing: %v", err)
	}
	if data["x"] != 1 {
		t.Errorf("data[x]: got %v, want 1", data["x"])
	}
}

func TestApplyUpdates_IncrementExistingField(t *testing.T) {
	t.Parallel()
	data := map[string]any{"count": int64(10)}
	ups := []update.Update{
		update.ByFieldName("count", dal.Increment(5)),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates Increment existing: %v", err)
	}
	got, ok := toNumericFloat64(data["count"])
	if !ok {
		t.Fatalf("data[count] not numeric: %T", data["count"])
	}
	if got != 15 {
		t.Errorf("data[count]: got %v, want 15", got)
	}
}

func TestApplyUpdates_IncrementMissingField(t *testing.T) {
	t.Parallel()
	data := map[string]any{}
	ups := []update.Update{
		update.ByFieldName("views", dal.Increment(3)),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates Increment missing field: %v", err)
	}
	got, ok := toNumericFloat64(data["views"])
	if !ok {
		t.Fatalf("data[views] not numeric: %T", data["views"])
	}
	if got != 3 {
		t.Errorf("data[views]: got %v, want 3", got)
	}
}

func TestApplyUpdates_IncrementNestedPath(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"stats": map[string]any{"clicks": int64(7)},
	}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"stats", "clicks"}, dal.Increment(3)),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates Increment nested: %v", err)
	}
	stats := data["stats"].(map[string]any)
	got, ok := toNumericFloat64(stats["clicks"])
	if !ok {
		t.Fatalf("stats[clicks] not numeric: %T", stats["clicks"])
	}
	if got != 10 {
		t.Errorf("stats[clicks]: got %v, want 10", got)
	}
}

func TestApplyUpdates_IncrementNonNumericError(t *testing.T) {
	t.Parallel()
	data := map[string]any{"score": "not-a-number"}
	ups := []update.Update{
		update.ByFieldName("score", dal.Increment(1)),
	}
	err := applyUpdates(data, ups)
	if err == nil {
		t.Fatal("want error when incrementing non-numeric field")
	}
	if !strings.Contains(err.Error(), "score") {
		t.Errorf("error should mention field name 'score', got: %v", err)
	}
}

func TestApplyUpdates_ServerTimestamp(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC().Truncate(time.Second)
	data := map[string]any{}
	ups := []update.Update{
		update.ByFieldName("updatedAt", update.ServerTimestamp),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates ServerTimestamp: %v", err)
	}
	raw, ok := data["updatedAt"].(string)
	if !ok {
		t.Fatalf("data[updatedAt] type: got %T, want string", data["updatedAt"])
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatalf("ServerTimestamp value %q is not RFC3339Nano: %v", raw, err)
	}
	after := time.Now().UTC().Add(time.Second)
	if parsed.Before(before) || parsed.After(after) {
		t.Errorf("ServerTimestamp %v is outside expected range [%v, %v]", parsed, before, after)
	}
}

func TestApplyUpdates_ServerTimestampNestedPath(t *testing.T) {
	t.Parallel()
	data := map[string]any{}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"audit", "updatedAt"}, update.ServerTimestamp),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates ServerTimestamp nested: %v", err)
	}
	audit, ok := data["audit"].(map[string]any)
	if !ok {
		t.Fatalf("data[audit] type: got %T, want map[string]any", data["audit"])
	}
	if _, ok := audit["updatedAt"].(string); !ok {
		t.Errorf("audit[updatedAt] type: got %T, want string", audit["updatedAt"])
	}
}

func TestApplyUpdates_NonMapIntermediateOnSetPathReturnsError(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"meta": "just a string",
	}
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"meta", "version"}, 2),
	}
	err := applyUpdates(data, ups)
	if err == nil {
		t.Fatal("want error when intermediate is not a map")
	}
	if !strings.Contains(err.Error(), "meta") {
		t.Errorf("error should mention 'meta', got: %v", err)
	}
}

func TestApplyUpdates_MixedBatchAppliedInOrder(t *testing.T) {
	t.Parallel()
	// Start with an existing record.
	data := map[string]any{
		"title": "original",
		"count": int64(10),
		"tags": map[string]any{
			"draft": true,
		},
	}
	ups := []update.Update{
		update.ByFieldName("title", "updated"),                                    // simple set
		update.ByFieldName("count", dal.Increment(5)),                             // increment
		update.ByFieldPath(update.FieldPath{"tags", "draft"}, update.DeleteField), // nested delete
		update.ByFieldPath(update.FieldPath{"tags", "published"}, true),           // nested set
		update.ByFieldName("ts", update.ServerTimestamp),                          // server timestamp
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates mixed batch: %v", err)
	}

	if data["title"] != "updated" {
		t.Errorf("title: got %v, want updated", data["title"])
	}
	countF, _ := toNumericFloat64(data["count"])
	if countF != 15 {
		t.Errorf("count: got %v, want 15", data["count"])
	}
	tags := data["tags"].(map[string]any)
	if _, present := tags["draft"]; present {
		t.Error("tags.draft should have been deleted")
	}
	if tags["published"] != true {
		t.Error("tags.published should be true")
	}
	if _, ok := data["ts"].(string); !ok {
		t.Errorf("ts type: got %T, want string", data["ts"])
	}
}

func TestApplyUpdates_FieldNameDeleteField(t *testing.T) {
	t.Parallel()
	data := map[string]any{
		"keep": "yes",
		"drop": "goodbye",
	}
	ups := []update.Update{
		update.DeleteByFieldName("drop"),
	}
	if err := applyUpdates(data, ups); err != nil {
		t.Fatalf("applyUpdates DeleteByFieldName: %v", err)
	}
	if data["keep"] != "yes" {
		t.Errorf("keep: got %v, want 'yes'", data["keep"])
	}
	if _, present := data["drop"]; present {
		t.Error("drop should have been deleted")
	}
}

// ---------------------------------------------------------------------------
// Integration tests: UpdateMulti via the TX — exercises the full read-modify-write
// pipeline including actual YAML file I/O.
// ---------------------------------------------------------------------------

func TestUpdateMulti_AppliesSameUpdatesToMultipleKeys(t *testing.T) {
	t.Parallel()
	tx, root := makeReadwriteTx(t)
	dir := filepath.Join(root, "items", "$records")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Seed two records.
	for _, key := range []string{"r1", "r2"} {
		p := filepath.Join(dir, key+".yaml")
		if err := os.WriteFile(p, []byte("name: old\n"), 0o644); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	keys := []*record.Key{record.NewKeyWithID("items", "r1"), record.NewKeyWithID("items", "r2")}
	ups := []update.Update{update.ByFieldName("name", "multi-updated")}
	if err := tx.UpdateMulti(context.Background(), keys, ups); err != nil {
		t.Fatalf("UpdateMulti: %v", err)
	}
	for _, key := range []string{"r1", "r2"} {
		p := filepath.Join(dir, key+".yaml")
		content, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		var got map[string]any
		if err := yaml.Unmarshal(content, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", key, err)
		}
		if got["name"] != "multi-updated" {
			t.Errorf("%s: name = %v, want multi-updated", key, got["name"])
		}
	}
}

func TestUpdate_NestedPathPersisted(t *testing.T) {
	t.Parallel()
	tx, root := makeReadwriteTx(t)
	dir := filepath.Join(root, "items", "$records")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Seed a record with a nested map.
	p := filepath.Join(dir, "nested1.yaml")
	if err := os.WriteFile(p, []byte("name: item\nmeta:\n  version: 1\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	key := record.NewKeyWithID("items", "nested1")
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"meta", "version"}, 99),
	}
	if err := tx.Update(context.Background(), key, ups); err != nil {
		t.Fatalf("Update nested path: %v", err)
	}
	content, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got map[string]any
	if err := yaml.Unmarshal(content, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	meta, ok := got["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta type: got %T, want map[string]any", got["meta"])
	}
	if meta["version"] != 99 {
		t.Errorf("meta[version]: got %v, want 99", meta["version"])
	}
}

func TestUpdate_DeleteNestedFieldPersisted(t *testing.T) {
	t.Parallel()
	tx, root := makeReadwriteTx(t)
	dir := filepath.Join(root, "items", "$records")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(dir, "del1.yaml")
	if err := os.WriteFile(p, []byte("name: item\ntags:\n  active: true\n  archived: true\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	key := record.NewKeyWithID("items", "del1")
	ups := []update.Update{
		update.ByFieldPath(update.FieldPath{"tags", "archived"}, update.DeleteField),
	}
	if err := tx.Update(context.Background(), key, ups); err != nil {
		t.Fatalf("Update DeleteField nested: %v", err)
	}
	content, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got map[string]any
	if err := yaml.Unmarshal(content, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tags, ok := got["tags"].(map[string]any)
	if !ok {
		t.Fatalf("tags type: got %T, want map[string]any", got["tags"])
	}
	if _, present := tags["archived"]; present {
		t.Error("tags.archived should have been deleted")
	}
	if tags["active"] != true {
		t.Error("tags.active should survive")
	}
}
