package db

import (
	"database/sql"
	"errors"
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

func WithTransaction(conn *sql.DB, fn txCallback) {
	tx, err := conn.Begin()

	if err != nil {
		log.Fatalf("failed to begin transaction. %s", err)
	}

	if err = fn(tx); err != nil {
		tx.Rollback()
		log.Fatalf("failed to execute transaction. %s", err)
	}

	if err = tx.Commit(); err != nil {
		tx.Rollback()
		log.Fatalf("failed to commit transaction. %s", err)
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
		var (
			table string
		)

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
		return errors.Join(errors.New("failed to create history table"), err)
	}

	return nil
}
