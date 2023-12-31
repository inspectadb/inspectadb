package mysql

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestMySQLDriver_WrapIdentifier(t *testing.T) {
	d := MySQLDriver{}

	assert.Equal(t, d.WrapIdentifier("id"), "`id`")
}

func TestMySQLDriver_GetIdentifierMaxLength(t *testing.T) {
	d := MySQLDriver{}

	assert.Equal(t, d.GetIdentifierMaxLength(), 63)
}
