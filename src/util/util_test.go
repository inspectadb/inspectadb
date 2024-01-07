package util

import (
	"github.com/magiconair/properties/assert"
	"reflect"
	"strings"
	"testing"
)

func TestStringSliceToAnySlice(t *testing.T) {
	arg := []string{"my", "name"}
	asAnySlice := StringSliceToAnySlice(arg)

	assert.Equal(t, len(asAnySlice), len(arg))
	assert.Equal(t, reflect.TypeOf(asAnySlice), reflect.TypeOf([]any{}))
}

func TestUUIDWithoutHyphens(t *testing.T) {
	t.Run("No hyphens", func(t *testing.T) {
		assert.Equal(t, strings.Contains(UUIDWithoutHyphens(), "-"), false)
	})
}

func TestJoinNonEmpty(t *testing.T) {
	want := "my-name-jeff"
	got := joinNonEmpty([]string{"", "my", "name", "", "jeff"}, "-")

	assert.Equal(t, got, want)
}

func TestBuildChangeTableName(t *testing.T) {
	assert.Equal(t, BuildChangeTableName("prefix", "table", "suffix", 19), "prefix_table_suffix")

	t.Run("Test truncation", func(t *testing.T) {
		assert.Equal(t, BuildChangeTableName("prefix", "table", "suffix", 15), "prefix_table_su")
		assert.Equal(t, len(BuildChangeTableName("prefix", "table", "suffix", 15)), len("prefix_table_su"))
	})
}

func TestBuildTriggerName(t *testing.T) {
	triggerName := BuildTriggerName("table", "i", 19)
	assert.Equal(t, len(triggerName) <= 19, true)
}

func TestBuildFunctionName(t *testing.T) {
	funcName := BuildFunctionName("table", "i", 19)
	assert.Equal(t, len(funcName) <= 19, true)
}
