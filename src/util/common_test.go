package util

import (
	"github.com/magiconair/properties/assert"
	"strings"
	"testing"
)

func TestUUIDWithoutHyphens(t *testing.T) {
	t.Run("No hyphens", func(t *testing.T) {
		assert.Equal(t, strings.Contains(UUIDWithoutHyphens(), "-"), false)
	})
}
