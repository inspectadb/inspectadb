package postgresql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/db"
	"github.com/inspectadb/inspectadb/src/errs"
	"github.com/inspectadb/inspectadb/src/stub"
	"github.com/inspectadb/inspectadb/src/util"
	"log"
	"regexp"
	"strings"
	"unicode/utf8"
)

type PostgreSQL struct{}

type historyRecord struct {
	Schema         string
	Action         string
	TriggerTable   string
	ChangeTable    string
	InsertTrigger  string
	UpdateTrigger  string
	DeleteTrigger  string
	InsertFunction string
	UpdateFunction string
	DeleteFunction string
}

func (d PostgreSQL) GetIdentifierMaxLength() int {
	return 63
}

func (d PostgreSQL) WrapIdentifier(identifier string) string {
	return `"` + identifier + `"`
}

func (d PostgreSQL) DebugQuery(SQL string, params []any) {
	count := 0
	regex := regexp.MustCompile(`\$\d+`)
	SQL = regex.ReplaceAllStringFunc(SQL, func(s string) string {
		count++
		return "'" + params[count-1].(string) + "'"
	})

	fmt.Println(SQL)
}

func (d PostgreSQL) BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string {
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

func (d PostgreSQL) BuildDSN(dbConfig config.DBConfig) string {
	params := []any{dbConfig.Host, dbConfig.Port, dbConfig.Database, dbConfig.User}
	dsn := "host=%s port=%d dbname=%s user=%s"

	if utf8.RuneCountInString(dbConfig.Password) > 0 {
		dsn += " password=%s"
		params = append(params, dbConfig.Password)
	}

	dsn += " sslmode=disable"

	return fmt.Sprintf(dsn, params...)
}

func (d PostgreSQL) VerifyLicense(app config.App) bool {
	return true
}

func (d PostgreSQL) GetServerVersionSQL() string {
	return "SHOW server_version;"
}

func (d PostgreSQL) GetCreateHistoryTableSQL(app config.App) string {
	return stub.Read("pgsql-create-history-table", map[string]string{
		"<TABLE>": fmt.Sprintf("%s.%s", app.DB.Config.Schema, app.Config.HistoryTable),
	})
}

func (d PostgreSQL) GetColumnsToSyncSQL() string {
	//TODO implement me
	panic("implement me")
}

func (d PostgreSQL) Audit(app config.App) error {
	SQLStatements := []map[string]any{}
	triggerTableSchema := app.DB.Config.Schema
	changeTableSchema := app.DB.Config.Schema

	if app.Config.AlternateSchema != "" {
		changeTableSchema = app.Config.AlternateSchema
	}

	// TODO: Once we have entry, amend this query
	getTablesSQL := `SELECT
					TABLE_NAME
				FROM INFORMATION_SCHEMA.TABLES
				WHERE
					TABLE_SCHEMA = $1 AND
					TABLE_TYPE = 'BASE TABLE' AND
					TABLE_NAME NOT IN ($2<EXCLUDE>) AND
					TABLE_NAME NOT IN (SELECT change_table FROM <SCHEMA>.<TABLE>);`

	if len(app.Config.Exclude) >= 1 {
		excludePlaceholders := d.BuildPlaceholders(len(app.Config.Exclude), 3)
		getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", ","+excludePlaceholders)
	} else {
		getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", "")
	}

	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<SCHEMA>", fmt.Sprintf("%s", d.WrapIdentifier(app.DB.Config.Schema)))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<TABLE>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.HistoryTable)))

	triggerTables, err := db.GetTables(app.DB.Conn, getTablesSQL, append([]any{app.DB.Config.Schema, app.Config.HistoryTable}, util.StringSliceToAnySlice(app.Config.Exclude)...))

	if err != nil {
		return fmt.Errorf("%w: %v", errs.FailedToGetTriggerTables, err)
	}

	for _, triggerTable := range triggerTables {
		createChangeTable := false
		changeTable := ""
		insertFunction := util.BuildFunctionName(triggerTable, "i", d.GetIdentifierMaxLength())
		updateFunction := util.BuildFunctionName(triggerTable, "u", d.GetIdentifierMaxLength())
		deleteFunction := util.BuildFunctionName(triggerTable, "d", d.GetIdentifierMaxLength())
		insertTrigger := util.BuildTriggerName(triggerTable, "i", d.GetIdentifierMaxLength())
		updateTrigger := util.BuildTriggerName(triggerTable, "u", d.GetIdentifierMaxLength())
		deleteTrigger := util.BuildTriggerName(triggerTable, "d", d.GetIdentifierMaxLength())
		triggerOptions := []map[string]any{}

		historyRecordRow := historyRecord{}
		err := app.DB.Conn.QueryRow(
			`SELECT trigger_table, change_table, insert_trigger, update_trigger, delete_trigger, insert_function, update_function, delete_function FROM "`+app.DB.Config.Schema+`"."`+app.Config.HistoryTable+`" WHERE trigger_table = $1`, triggerTable).Scan(
			&historyRecordRow.TriggerTable,
			&historyRecordRow.ChangeTable,
			&historyRecordRow.InsertTrigger,
			&historyRecordRow.UpdateTrigger,
			&historyRecordRow.DeleteTrigger,
			&historyRecordRow.InsertFunction,
			&historyRecordRow.UpdateFunction,
			&historyRecordRow.DeleteFunction,
		)

		if !errors.Is(err, sql.ErrNoRows) && err != nil {
			return fmt.Errorf("%w: %v", errs.FailedToGetHistoryRecord, err)
		}
		// hasn't been audited
		if errors.Is(err, sql.ErrNoRows) {
			createChangeTable = true
			changeTable = fmt.Sprintf("%s.%s", changeTableSchema, util.BuildIdentifierName(d.GetIdentifierMaxLength(), app.Config.ChangeTablePrefix, triggerTable, app.Config.ChangeTableSuffix))

			SQLStatements = append(SQLStatements, map[string]any{
				"query": stub.Read("pgsql-create-change-table", map[string]string{
					"<CHANGE_TABLE>":  changeTable,
					"<CHANGE_ID>":     app.Config.ChangeIdColumn,
					"<CHANGE_ACTION>": app.Config.ChangeActionColumn,
					"<CHANGED_BY>":    app.Config.ChangedByColumn,
					"<CHANGED_AT>":    app.Config.ChangedAtColumn,
					"<TRIGGER_TABLE>": fmt.Sprintf("%s.%s", triggerTableSchema, triggerTable),
				}),
			})

			triggerOptions = []map[string]any{
				{
					"trigger": insertTrigger,
					"action":  "INSERT",
				},
				{
					"trigger": updateTrigger,
					"action":  "UPDATE",
				},
				{
					"trigger": deleteTrigger,
					"action":  "DELETE",
				},
			}
		} else {

		}

		// schema.table
		SQLStatements = append(SQLStatements, map[string]any{
			"query": fmt.Sprintf("UPDATE `%s`.`%s` SET `performed_at` = NOW() WHERE `trigger_table` = ? AND `change_table` = ?", app.DB.Config.Schema, app.Config.HistoryTable),
			"params": []any{
				historyRecordRow.TriggerTable,
				historyRecordRow.ChangeTable,
			},
		})

		triggerOptions = []map[string]any{
			{
				"trigger": historyRecordRow.InsertTrigger,
				"action":  "INSERT",
			},
			{
				"trigger": historyRecordRow.UpdateTrigger,
				"action":  "UPDATE",
			},
			{
				"trigger": historyRecordRow.DeleteTrigger,
				"action":  "DELETE",
			},
		}

		columns := ""
		rows, err := app.DB.Conn.Query(`SELECT
				COLUMN_NAME,
				COLUMN_TYPE
			FROM information_schema.COLUMNS
			WHERE
				TABLE_SCHEMA = ? AND
				TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION ASC;`, triggerTableSchema, triggerTable)

		if err != nil {
			return fmt.Errorf("%w: %s. %v", errs.FailedToGetTriggerTableColumns, triggerTable, err)
		}

		notLast := rows.Next()

		for notLast {
			var column db.InformationSchemaColumn

			err = rows.Scan(&column.Name, &column.Type)

			if err != nil {
				return fmt.Errorf("%w: %v", errs.FailedToScanTriggerTableColumns, err)
			}

			notLast = rows.Next()

			if notLast {
				columns += fmt.Sprintf("%s.%s, ", "<KEYWORD>", d.WrapIdentifier(column.Name))
			} else {
				columns += fmt.Sprintf("%s.%s", "<KEYWORD>", d.WrapIdentifier(column.Name))
			}
		}

		insertStatement := fmt.Sprintf("INSERT INTO %s VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", changeTable, "INSERT", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		updateStatement := fmt.Sprintf("INSERT INTO %s VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", changeTable, "UPDATE", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		deleteStatement := fmt.Sprintf("INSERT INTO %s VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", changeTable, "DELETE", strings.ReplaceAll(columns, "<KEYWORD>", "old"))

		for _, triggerOption := range triggerOptions {
			if !createChangeTable {
				SQLStatements = append(SQLStatements, map[string]any{
					"query": stub.Read("mysql-drop-trigger", map[string]string{
						"<SCHEMA>":  changeTableSchema,
						"<TRIGGER>": triggerOption["trigger"].(string),
					}),
				})
			}

			trigger, action, statement := triggerOption["trigger"].(string), triggerOption["action"].(string), ""

			if action == "INSERT" {
				statement = insertStatement
			} else if action == "UPDATE" {
				statement = updateStatement
			} else if action == "DELETE" {
				statement = deleteStatement
			}

			SQLStatements = append(SQLStatements, map[string]any{
				"query": stub.Read("mysql-create-trigger", map[string]string{
					"<TRIGGER>":   fmt.Sprintf("%s", trigger),
					"<ACTION>":    action,
					"<TABLE>":     fmt.Sprintf("%s.%s", triggerTableSchema, triggerTable),
					"<STATEMENT>": statement,
				}),
			})
		}

		//newColumns, oldColumns := "", ""
		//
		//rows, err := conn.Query(`SELECT
		//	"attname" AS COLUMN_NAME,
		//	format_type(pga.atttypid, pga.atttypmod) AS COLUMN_TYPE
		//FROM pg_attribute pga
		//	INNER JOIN pg_class pgc ON pgc.oid = pga.attrelid AND pgc.relname = $1
		//	INNER JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = $2
		//WHERE
		//	pga.attnum > 0 AND
		//	NOT pga.attisdropped
		//ORDER BY pga.attnum ASC;`, table, app.Config.DB.Schema)
		//
		//if err != nil {
		//	return errors.Join(errors.New("failed to get columns for '"+table+"'"), err)
		//}
		//
		//notLast := rows.Next()
		//
		//for notLast {
		//	var column db.InformationSchemaColumn
		//
		//	err = rows.Scan(&column.Name, &column.Type)
		//
		//	if err != nil {
		//		return errors.Join(errors.New("failed to scan column information"), err)
		//	}
		//
		//	notLast = rows.Next()
		//
		//	if notLast {
		//		newColumns += fmt.Sprintf("%s.%s, ", "new", d.WrapIdentifier(column.Name))
		//		oldColumns += fmt.Sprintf("%s.%s, ", "old", d.WrapIdentifier(column.Name))
		//	} else {
		//		newColumns += fmt.Sprintf("%s.%s", "new", d.WrapIdentifier(column.Name))
		//		oldColumns += fmt.Sprintf("%s.%s", "old", d.WrapIdentifier(column.Name))
		//	}
		//}
		//
		insertStatement := fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (DEFAULT, '%s', current_user(), now(), %s);`, app.Config.DB.Schema, auditTable, "INSERT", newColumns)
		updateStatement := fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (DEFAULT, '%s', current_user(), now(), %s);`, app.Config.DB.Schema, auditTable, "UPDATE", newColumns)
		deleteStatement := fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (DEFAULT, '%s', current_user(), now(), %s);`, app.Config.DB.Schema, auditTable, "DELETE", oldColumns)

		log.Println(triggerTable)
	}

	if len(SQLStatements) > 0 {
		db.Transaction(app.DB.Conn, func(tx *sql.Tx) error {
			for _, SQLStatement := range SQLStatements {
				query := SQLStatement["query"].(string)
				params, hasParams := SQLStatement["params"].([]any)

				if hasParams {
					_, err = tx.Exec(query, params...)
				} else {
					_, err = tx.Exec(query)
				}

				if err != nil {
					return fmt.Errorf("%w: sql: %s. params: %v", err, query, params)
				}
			}

			return nil
		})
	}

	return nil
}

func (d PostgreSQL) Purge(app config.App) error {
	//TODO implement me
	panic("implement me")
}
