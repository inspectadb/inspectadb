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

func TestBuildIdentifierName(t *testing.T) {
	t.Run("Is ID generated", func(t *testing.T) {
		want := "my_id_1"
		got := BuildIdentifierName(25, "my", "id", "1")

		assert.Equal(t, got, want)
	})

	t.Run("Is string truncated", func(t *testing.T) {
		want := "my_id"
		got := BuildIdentifierName(5, "my", "id", "1")

		assert.Equal(t, got, want)
		assert.Equal(t, len(got), len(want))
	})
}
