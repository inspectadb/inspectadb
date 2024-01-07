package config

import (
	"github.com/inspectadb/inspectadb/src/errs"
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestParseDSN(t *testing.T) {
	t.Run("DSN without database", func(t *testing.T) {
		dbConfig, _ := parseDSN("mysql://root:password@localhost:3306/myschema")

		assert.Equal(t, dbConfig, DBConfig{
			Driver:   "mysql",
			User:     "root",
			Password: "password",
			Host:     "localhost",
			Port:     3306,
			Database: "myschema",
			Schema:   "myschema",
		})
	})

	t.Run("DSN with database", func(t *testing.T) {
		dbConfig, _ := parseDSN("pgsql://root:password@localhost:5432/db:schema")

		assert.Equal(t, dbConfig, DBConfig{
			Driver:   "pgsql",
			User:     "root",
			Password: "password",
			Host:     "localhost",
			Port:     5432,
			Database: "db",
			Schema:   "schema",
		})
	})

	t.Run("DSN without password", func(t *testing.T) {
		dbConfig, _ := parseDSN("pgsql://root:@localhost:5432/db:schema")

		assert.Equal(t, dbConfig, DBConfig{
			Driver:   "pgsql",
			User:     "root",
			Password: "",
			Host:     "localhost",
			Port:     5432,
			Database: "db",
			Schema:   "schema",
		})
	})

	t.Run("DSN with invalid port", func(t *testing.T) {
		dbConfig, err := parseDSN("pgsql://root:password@localhost:port/db:schema")

		assert.Equal(t, dbConfig, DBConfig{})
		assert.Equal(t, err, errs.InvalidPort)
	})
}
