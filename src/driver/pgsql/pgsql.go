package pgsql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"inspectadb/src/config"
	"inspectadb/src/db"
	"inspectadb/src/util"
	"log"
	"regexp"
	"strings"
	"unicode/utf8"
)

// TODO: SSL mode

type PGSQLDriver struct{}

type historyRecord struct {
	Schema         string
	Action         string
	OriginalTable  string
	AuditTable     string
	InsertTrigger  string
	UpdateTrigger  string
	DeleteTrigger  string
	InsertFunction string
	UpdateFunction string
	DeleteFunction string
}

func (d PGSQLDriver) WrapIdentifier(identifier string) string {
	return `"` + identifier + `"`
}

func (d PGSQLDriver) GetIdentifierMaxLength() int {
	return 63
}

func (d PGSQLDriver) DebugQuery(SQL string, params []any) {
	count := 0
	regex := regexp.MustCompile(`\$\d+`)
	SQL = regex.ReplaceAllStringFunc(SQL, func(s string) string {
		count++
		return "'" + params[count-1].(string) + "'"
	})

	fmt.Println(SQL)
}

func (d PGSQLDriver) BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string {
	str := ""

	for i := 0; i < totalNoOfPlaceholders; i++ {
		if i+1 == totalNoOfPlaceholders {
			str += fmt.Sprintf("$%d", startFrom+i)
		} else {
			str += fmt.Sprintf("$%d,", startFrom+i)
		}
	}

	return str
}

func (d PGSQLDriver) GetColumnsToSyncSQL() string {
	//TODO implement me
	panic("implement me")
}

func (d PGSQLDriver) Connect(dbConfig config.DBConfig) (*sql.DB, error) {
	params := []any{dbConfig.Host, dbConfig.Port, dbConfig.Database, dbConfig.User}

	connStr := "host=%s port=%d dbname=%s user=%s"

	if utf8.RuneCountInString(dbConfig.Password) > 0 {
		connStr += " password=%s"
		params = append(params, dbConfig.Password)
	}

	connStr += " sslmode=disable"

	// ssl-mode =
	// disable - No SSL
	// require - Always SSL (skip verification)
	// verify-ca - Always SSL (verify that the certificate presented by the server was signed by a trusted CA)
	// verify-full - Always SSL (verify that the certification presented by the server was signed by a trusted CA and the server host name matches the one in the certificate)
	conn, err := sql.Open("postgres", fmt.Sprintf(connStr, params...))

	if err != nil {
		return conn, errors.Join(errors.New("failed to initialize db driver 'pgsql'"), err)
	}

	if err := conn.Ping(); err != nil {
		return conn, errors.Join(errors.New("failed to connect to db"), err)
	}

	return conn, nil
}

func (d PGSQLDriver) Audit(app config.App) error {
	SQLStatements := []map[string]any{}

	formatByStrategies := func(v string) string {
		return util.FormatByStrategies(v, app.Config.NamingStrategy, app.Config.CaseStrategy)
	}

	conn, err := d.Connect(app.Config.DB)

	if err != nil {
		return err
	}

	historyTableSQL := util.ReadStub("pgsql-create-history-table", map[string]string{
		"<SCHEMA>": app.Config.DB.Schema,
		"<TABLE>":  app.Config.HistoryTable,
	})

	if err := db.CreateHistoryTable(conn, historyTableSQL); err != nil {
		return err
	}

	getTablesSQL := `SELECT
					TABLE_NAME
				FROM INFORMATION_SCHEMA.TABLES
				WHERE
					TABLE_SCHEMA = $1 AND
					TABLE_TYPE = 'BASE TABLE' AND
					TABLE_NAME NOT IN ($2<EXCLUDE>) AND
					TABLE_NAME NOT IN (SELECT audit_table FROM <SCHEMA>.<TABLE>);`

	if len(app.Config.Exclude) >= 1 {
		excludePlaceholders := d.BuildPlaceholders(len(app.Config.Exclude), 3)
		getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", ","+excludePlaceholders)
	} else {
		getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", "")
	}

	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<SCHEMA>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.DB.Schema)))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<TABLE>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.HistoryTable)))

	tables, err := db.GetTables(conn, getTablesSQL, append([]any{app.Config.DB.Schema, app.Config.HistoryTable}, util.StringSliceToAnySlice(app.Config.Exclude)...))

	if err != nil {
		return errors.Join(errors.New("failed to get tables"), err)
	}

	for _, table := range tables {
		historyRecord := historyRecord{}
		err := conn.QueryRow(
			`SELECT original_table, audit_table, insert_trigger, update_trigger, delete_trigger, insert_function, update_function, delete_function FROM `+app.Config.HistoryTable+` WHERE original_table = $1`, table).Scan(
			&historyRecord.OriginalTable,
			&historyRecord.AuditTable,
			&historyRecord.InsertTrigger,
			&historyRecord.UpdateTrigger,
			&historyRecord.DeleteTrigger,
			&historyRecord.InsertFunction,
			&historyRecord.UpdateFunction,
			&historyRecord.DeleteFunction,
		)

		if !errors.Is(err, sql.ErrNoRows) && err != nil {
			return err
		}

		// hasn't been audited
		if errors.Is(err, sql.ErrNoRows) {
			auditTable := formatByStrategies(util.BuildIdentifierName(d.GetIdentifierMaxLength(), app.Config.AuditTablePrefix, table, app.Config.AuditTableSuffix))
			newColumns, oldColumns := "", ""

			rows, err := conn.Query(`SELECT
        		"attname" AS COLUMN_NAME,
        		format_type(pga.atttypid, pga.atttypmod) AS COLUMN_TYPE
    		FROM pg_attribute pga
        		INNER JOIN pg_class pgc ON pgc.oid = pga.attrelid AND pgc.relname = $1
        		INNER JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = $2
    		WHERE
        		pga.attnum > 0 AND
        		NOT pga.attisdropped
			ORDER BY pga.attnum ASC;`, table, app.Config.DB.Schema)

			if err != nil {
				return errors.Join(errors.New("failed to get columns for '"+table+"'"), err)
			}

			notLast := rows.Next()

			for notLast {
				var column db.InformationSchemaColumn

				err = rows.Scan(&column.Name, &column.Type)

				if err != nil {
					return errors.Join(errors.New("failed to scan column information"), err)
				}

				notLast = rows.Next()

				if notLast {
					newColumns += fmt.Sprintf("%s.%s, ", "new", d.WrapIdentifier(column.Name))
					oldColumns += fmt.Sprintf("%s.%s, ", "old", d.WrapIdentifier(column.Name))
				} else {
					newColumns += fmt.Sprintf("%s.%s", "new", d.WrapIdentifier(column.Name))
					oldColumns += fmt.Sprintf("%s.%s", "old", d.WrapIdentifier(column.Name))
				}
			}
		} else {

		}

		log.Println(table)
	}

	db.WithTransaction(conn, func(tx *sql.Tx) error {
		for _, SQLStatement := range SQLStatements {
			query := SQLStatement["query"].(string)
			params, hasParams := SQLStatement["params"].([]any)

			if hasParams {
				_, err = tx.Exec(query, params...)
			} else {
				_, err = tx.Exec(query)
			}

			// TODO: Test this...
			if err != nil {
				return err
			}
		}

		return err
	})

	return nil
}

func (d PGSQLDriver) Reverse(app config.App, clean bool) error {
	//TODO implement me
	panic("implement me")
}
