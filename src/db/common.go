package db

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/errs"
	"log"
)

type InformationSchemaColumn struct {
	Schema   sql.NullString // optional, used for syncing
	Table    sql.NullString // optional, used for syncing
	Name     string         // required, for all drivers
	Type     string         // required, for all drivers
	Position sql.NullInt16  // MySQL/MariaDB
	After    sql.NullString // MySQL/MariaDB
}

type txCallback func(tx *sql.Tx) error

func Connect(driver driver.Driver, app *config.App) error {
	dbConfig := app.DB.Config
	driverMap := map[string]string{
		"mysql":      "mysql",
		"mariadb":    "mysql",
		"postgresql": "postgres",
		"postgres":   "postgres",
		"pgsql":      "postgres",
	}
	driverName, hasDriver := driverMap[dbConfig.Driver]

	if !hasDriver {
		return fmt.Errorf("connect: %w", errs.UnknownDriverRequested)
	}

	conn, err := sql.Open(driverName, driver.BuildDSN(dbConfig))

	if err != nil {
		return errors.Join(errors.New("failed to initialize db driver 'mysql'"), err)
	}

	if err := conn.Ping(); err != nil {
		return errors.Join(errors.New("failed to connect to db"), err)
	}

	app.DB.Conn = conn

	return nil
}

func Transaction(conn *sql.DB, fn txCallback) {
	tx, err := conn.Begin()

	if err != nil {
		log.Fatalf("%v", fmt.Errorf("%w: %v", errs.FailedToBeginTransaction, err))
	}

	if err = fn(tx); err != nil {
		tx.Rollback()
		log.Fatalf("%v", fmt.Errorf("%w: %v", errs.FailedToExecuteTransaction, err))
	}

	if err = tx.Commit(); err != nil {
		log.Fatalf("%v", fmt.Errorf("%w: %v", errs.FailedToCommitTransaction, err))
	}
}

func GetTables(conn *sql.DB, query string, params []any) ([]string, error) {
	tables := []string{}
	rows, err := conn.Query(query, params...)

	if err != nil {
		return tables, err
	}

	defer rows.Close()

	for rows.Next() {
		var table string
		err = rows.Scan(&table)

		if err != nil {
			return tables, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func CreateHistoryTable(conn *sql.DB, SQL string) error {
	_, err := conn.Exec(SQL)

	if err != nil {
		return errors.Join(errs.FailedToCreateHistoryTable, err)
	}

	return nil
}

func GetServerVersion(conn *sql.DB, SQL string) (string, error) {
	var version string

	if err := conn.QueryRow(SQL).Scan(&version); err != nil {
		return "", err
	}

	return version, nil
}
