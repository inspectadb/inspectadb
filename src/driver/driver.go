package driver

import (
	"database/sql"
	"github.com/inspectadb/inspectadb/src/config"
	"log"
)

type (
	Driver interface {
		WrapIdentifier(identifier string) string
		GetIdentifierMaxLength() int
		GetServerVersion(dbConfig config.DBConfig) (string, error)
		DebugQuery(SQL string, params []any)
		BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string
		// GetColumnsToSyncSQL
		// Get columns that need to be:
		// 	- added
		// 	- modified
		// 	- deleted
		// between the original and audit table
		// excluding base audit columns
		GetColumnsToSyncSQL() string
		Connect(dbConfig config.DBConfig) (*sql.DB, error)
		Audit(app config.App) error
		Reverse(app config.App, clean bool) error
	}
)

var (
	drivers = map[string]Driver{}
)

func Register(name string, driver Driver) {
	if _, dup := drivers[name]; dup {
		log.Fatalf("driver '%s' has already been registered.", name)
	}

	drivers[name] = driver
}

func Get(name string) Driver {
	d, ok := drivers[name]

	// If the key exists
	if !ok {
		log.Fatalf("unknown driver '%s'", name)
	}

	return d
}
