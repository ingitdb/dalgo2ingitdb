package dalgo2ingitdb

import (
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func TestEvaluateNullAndOrConditions(t *testing.T) {
	data := map[string]any{"present": "x", "empty": nil}
	for _, tc := range []struct {
		cond dal.Condition
		want bool
	}{
		{dal.Field("empty").IsNull(), true},
		{dal.Field("missing").IsNull(), true},
		{dal.Field("present").IsNull(), false},
		{dal.Field("present").IsNotNull(), true},
		{dal.Field("empty").IsNotNull(), false},
		{dal.NewGroupCondition(dal.Or, dal.Field("present").IsNull(), dal.Field("empty").IsNull()), true},
		{dal.NewGroupCondition(dal.Or, dal.Field("present").IsNull(), dal.Field("present").IsNull()), false},
	} {
		got, err := evaluateCondition(tc.cond, data, "k")
		if err != nil || got != tc.want {
			t.Fatalf("%s = %v, %v; want %v", tc.cond, got, err, tc.want)
		}
	}
}

func TestEvaluateInArray(t *testing.T) {
	for _, tc := range []struct {
		values any
		want   bool
	}{
		{[]int{1, 2}, true},
		{[]int{3}, false},
		{[]int{}, false},
	} {
		c := dal.Comparison{Left: dal.Field("v"), Operator: dal.In, Right: dal.NewArray(tc.values)}
		got, err := evaluateCondition(c, map[string]any{"v": int64(2)}, "k")
		if err != nil || got != tc.want {
			t.Fatalf("IN %v=%v,%v want %v", tc.values, got, err, tc.want)
		}
	}
}

func TestQueryNewConditionErrorBranches(t *testing.T) {
	if _, err := evaluateCondition(dal.NewIsNullCondition(mockUnsupportedExpr{}), nil, "k"); err == nil {
		t.Fatal("unsupported IS NULL expression accepted")
	}
	if _, err := evaluateGroupCondition(dal.NewGroupCondition(dal.Or, unsupportedCond{}), nil, "k"); err == nil {
		t.Fatal("OR child error lost")
	}
	if _, err := evaluateComparison(dal.Comparison{Left: dal.Field("v"), Operator: dal.In, Right: dal.Constant{Value: 2}}, map[string]any{"v": 2}, "k"); err == nil {
		t.Fatal("IN non-array accepted")
	}
	got, err := evaluateComparison(dal.Comparison{Left: dal.Field("v"), Operator: dal.NotIn, Right: dal.NewArray([]int{1})}, map[string]any{"v": 2}, "k")
	if err != nil || !got {
		t.Fatalf("NOT IN: %v, %v", got, err)
	}
}
