package dalgo2ingitdb

import (
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestEvaluateIsNullCondition_Values(t *testing.T) {
	var nilPtr *string
	text := "x"
	data := map[string]any{
		"present": "x",
		"empty":   nil,
		"nilptr":  nilPtr,
		"ptr":     &text,
		"zero":    0,
		"blank":   "",
		"list":    []string(nil),
		"a.b":     "dotted",
	}
	for _, tc := range []struct {
		name string
		cond dal.Condition
		want bool
	}{
		{"absent is null", dal.Field("missing").IsNull(), true},
		{"absent not-null", dal.Field("missing").IsNotNull(), false},
		{"nested absent is null", dal.Field("missing.child").IsNull(), true},
		{"dotted key present", dal.Field("a.b").IsNotNull(), true},
		{"explicit nil", dal.Field("empty").IsNull(), true},
		{"nil pointer is null", dal.Field("nilptr").IsNull(), true},
		{"nil pointer not-null", dal.Field("nilptr").IsNotNull(), false},
		{"non-nil pointer", dal.Field("ptr").IsNull(), false},
		{"value", dal.Field("present").IsNull(), false},
		{"value not-null", dal.Field("present").IsNotNull(), true},
		{"zero is a value", dal.Field("zero").IsNull(), false},
		{"empty string is a value", dal.Field("blank").IsNull(), false},
		{"nil slice is a value", dal.Field("list").IsNull(), false},
		{"$id never null", dal.Field("$id").IsNull(), false},
		{"and true", dal.NewGroupCondition(dal.And, dal.Field("missing").IsNull(), dal.Field("present").IsNotNull()), true},
		{"and false", dal.NewGroupCondition(dal.And, dal.Field("missing").IsNull(), dal.Field("present").IsNull()), false},
		{"or true", dal.NewGroupCondition(dal.Or, dal.Field("present").IsNull(), dal.Field("empty").IsNull()), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := evaluateCondition(tc.cond, data, "k")
			if err != nil || got != tc.want {
				t.Fatalf("%s = %v, %v; want %v", tc.cond, got, err, tc.want)
			}
		})
	}
}
