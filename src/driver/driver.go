package driver

import (
	"fmt"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/errs"
	"log"
)

type (
	Driver interface {
		GetIdentifierMaxLength() int
		WrapIdentifier(identifier string) string
		DebugQuery(SQL string, params []any)
		BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string
		BuildDSN(dbConfig config.DBConfig) string
		VerifyLicense(app config.App) bool
		GetServerVersionSQL() string
		GetCreateHistoryTableSQL(app config.App) string
		// GetColumnsToSyncSQL
		// Get columns that need to be:
		// 	- added
		// 	- modified - positioning or data types
		// 	- deleted
		// between the original and audit table
		// excluding base audit columns
		GetColumnsToSyncSQL() string
		Audit(app config.App) error
		Purge(app config.App) error
	}
)

var (
	drivers = map[string]Driver{}
)

func Register(name string, driver Driver) {
	if _, dup := drivers[name]; dup {
		log.Fatalln(fmt.Errorf("%w: %s", errs.DuplicateDriverRegistration, name))
	}

	drivers[name] = driver
}

func Get(name string) (Driver, error) {
	d, ok := drivers[name]

	if !ok {
		return nil, fmt.Errorf("%w: %s (get driver)", errs.UnknownDriverRequested, name)
	}

	return d, nil
}
