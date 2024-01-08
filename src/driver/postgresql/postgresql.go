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

// GetColumnsToAddSQL
// Params: changeTableSchema, changeTable, tableCatalog (db), triggerTableSchema, triggerTable
func GetColumnsToAddSQL() string {
	return `SELECT
				'ADD' AS ACTION,
				tt.column_name,
				tt.data_type,
				tt.ordinal_position,
				(
					SELECT
						COLUMN_NAME
					FROM information_schema.columns
					WHERE
						table_schema = tt.table_schema AND
						table_name = tt.table_name AND
						ordinal_position < tt.ordinal_position
					ORDER BY ordinal_position DESC
					LIMIT 1
				) AS AFTER_COLUMN_NAME
			FROM information_schema.columns AS tt
			LEFT JOIN information_schema.columns AS cc ON
				(cc.table_catalog, cc.table_schema, cc.table_name, tt.column_name) = (tt.table_catalog, $1, $2, cc.column_name)
			WHERE
				(tt.table_catalog, tt.table_schema, tt.table_name) = ($3, $4, $5) AND
				cc.table_name IS NULL`
}

// GetColumnsToModifySQL
// Params: changeTableSchema, changeTable, tableCatalog (db), triggerTableSchema, triggerTable
func GetColumnsToModifySQL() string {
	return `SELECT
				'MODIFY' AS ACTION,
				tt.column_name,
				tt.data_type,
				tt.ordinal_position,
				NULL AS AFTER_COLUMN_NAME
			FROM information_schema.columns AS tt
			INNER JOIN information_schema.columns ct ON
				(ct.table_catalog, ct.table_schema, ct.table_name) = ($1, $2, $3) AND
				tt.column_name = ct.column_name
			WHERE
				(tt.table_catalog, tt.table_schema, tt.table_name) = ($4, $5, $6) AND
				tt.column_name NOT IN ($7, $8, $9, $10) AND
				ct.column_name IS NULL;`
}

// GetColumnsToDropSQL
// Params: tt_db, tt_schema, tt, ct_db, ct_schema, ct, change_id, change_action, changed_by, changed_at
func GetColumnsToDropSQL() string {
	return `SELECT
				'DROP' AS ACTION,
				ct.column_name,
				ct.data_type,
				ct.ordinal_position,
				NULL AS AFTER_COLUMN_NAME
			FROM information_schema.columns AS ct
			LEFT JOIN information_schema.columns tt ON
				(tt.table_catalog, tt.table_schema, tt.table_name) = ($1, $2, $3) AND
				ct.column_name = tt.column_name
			WHERE
				(ct.table_catalog, ct.table_schema, ct.table_name) = ($4, $5, $6) AND
				ct.column_name NOT IN ($7, $8, $9, $10) AND
				tt.column_name IS NULL;`
}

// GetColumnsToSyncSQL
// Params for A: changeTableSchema, changeTable, tableCatalog (db), triggerTableSchema, triggerTable
// Params for D: change_id, change_action, changed_by, changed_at
// Params for M: trigger_table, schema, trigger_table, schema, change_table, schema
func (d PostgreSQL) GetColumnsToSyncSQL() string {
	return `
			UNION
			`
}

func (d PostgreSQL) Audit(app config.App) error {
	SQLStatements := []map[string]any{}
	triggerTableSchema := app.DB.Config.Schema
	changeTableSchema := app.DB.Config.Schema

	if app.Config.AlternateSchema != "" {
		changeTableSchema = app.Config.AlternateSchema
	}

	getTablesSQL := `SELECT
					TABLE_NAME
				FROM INFORMATION_SCHEMA.TABLES
				WHERE
					TABLE_SCHEMA = $1 AND
					TABLE_TYPE = 'BASE TABLE' AND
					TABLE_NAME NOT IN ($2<EXCLUDE>) AND
					TABLE_NAME NOT IN (SELECT SPLIT_PART(change_table, '.', 2) FROM <SCHEMA>.<TABLE>);`

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
			`SELECT trigger_table, change_table, insert_trigger, update_trigger, delete_trigger, insert_function, update_function, delete_function FROM "`+app.DB.Config.Schema+`"."`+app.Config.HistoryTable+`" WHERE trigger_table = $1`, fmt.Sprintf("%s.%s", app.DB.Config.Schema, triggerTable)).Scan(
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
			changeTable = fmt.Sprintf("%s.%s", changeTableSchema, util.BuildChangeTableName(app.Config.ChangeTablePrefix, triggerTable, app.Config.ChangeTableSuffix, d.GetIdentifierMaxLength()))

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

			SQLStatements = append(SQLStatements, map[string]any{
				"query": fmt.Sprintf(`INSERT INTO "%s"."%s" ("trigger_table", "change_table", "insert_trigger", "update_trigger", "delete_trigger", "insert_function", "update_function", "delete_function", "user") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, current_user)`, app.DB.Config.Schema, app.Config.HistoryTable),
				"params": []any{
					fmt.Sprintf("%s.%s", triggerTableSchema, triggerTable),
					changeTable,
					insertTrigger,
					updateTrigger,
					deleteTrigger,
					insertFunction,
					updateFunction,
					deleteFunction,
				},
			})

			triggerOptions = []map[string]any{
				{
					"trigger":  insertTrigger,
					"function": insertFunction,
					"action":   "INSERT",
				},
				{
					"trigger":  updateTrigger,
					"function": updateFunction,
					"action":   "UPDATE",
				},
				{
					"trigger":  deleteTrigger,
					"function": deleteFunction,
					"action":   "DELETE",
				},
			}
		} else {
			// schema.table
			createChangeTable = false
			changeTable = historyRecordRow.ChangeTable
			SQLStatements = append(SQLStatements, map[string]any{
				"query": fmt.Sprintf(`UPDATE "%s"."%s" SET "performed_at" = NOW() WHERE "trigger_table" = $1 AND "change_table" = $2`, app.DB.Config.Schema, app.Config.HistoryTable),
				"params": []any{
					historyRecordRow.TriggerTable,
					historyRecordRow.ChangeTable,
				},
			})

			triggerOptions = []map[string]any{
				{
					"trigger":  historyRecordRow.InsertTrigger,
					"function": historyRecordRow.InsertFunction,
					"action":   "INSERT",
				},
				{
					"trigger":  historyRecordRow.UpdateTrigger,
					"function": historyRecordRow.UpdateFunction,
					"action":   "UPDATE",
				},
				{
					"trigger":  historyRecordRow.DeleteTrigger,
					"function": historyRecordRow.DeleteFunction,
					"action":   "DELETE",
				},
			}
		}

		columns := ""
		rows, err := app.DB.Conn.Query(`SELECT
			"attname" AS COLUMN_NAME,
			format_type(pga.atttypid, pga.atttypmod) AS COLUMN_TYPE
		FROM pg_attribute pga
			INNER JOIN pg_class pgc ON pgc.oid = pga.attrelid AND pgc.relname = $1
			INNER JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = $2
		WHERE
			pga.attnum > 0 AND
			NOT pga.attisdropped
		ORDER BY pga.attnum ASC;`, triggerTable, triggerTableSchema)

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

		insertStatement := fmt.Sprintf("INSERT INTO %s VALUES (DEFAULT, '%s', current_user, now(), %s);", changeTable, "INSERT", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		updateStatement := fmt.Sprintf("INSERT INTO %s VALUES (DEFAULT, '%s', current_user, now(), %s);", changeTable, "UPDATE", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		deleteStatement := fmt.Sprintf("INSERT INTO %s VALUES (DEFAULT, '%s', current_user, now(), %s);", changeTable, "DELETE", strings.ReplaceAll(columns, "<KEYWORD>", "old"))

		for _, triggerOption := range triggerOptions {
			if !createChangeTable {
				SQLStatements = append(SQLStatements, map[string]any{
					"query": stub.Read("pgsql-drop-trigger", map[string]string{
						"<TRIGGER>": triggerOption["trigger"].(string),
						"<TABLE>":   fmt.Sprintf("%s.%s", app.DB.Config.Schema, triggerTable),
					}),
				})

				SQLStatements = append(SQLStatements, map[string]any{
					"query": stub.Read("pgsql-drop-function", map[string]string{
						"<FUNCTION>": triggerOption["function"].(string),
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
				"query": stub.Read("pgsql-create-function", map[string]string{
					"<FUNCTION>":  triggerOption["function"].(string),
					"<STATEMENT>": statement,
				}),
			})

			SQLStatements = append(SQLStatements, map[string]any{
				"query": stub.Read("pgsql-create-trigger", map[string]string{
					"<TRIGGER>":  fmt.Sprintf("%s", trigger),
					"<ACTION>":   action,
					"<TABLE>":    fmt.Sprintf("%s.%s", triggerTableSchema, triggerTable),
					"<FUNCTION>": triggerOption["function"].(string),
				}),
			})
		}
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
	SQLStatements := []map[string]any{}

	rows, err := app.DB.Conn.Query(fmt.Sprintf(`SELECT "trigger_table", "change_table", "insert_trigger", "update_trigger", "delete_trigger", "insert_function", "update_function", "delete_function" FROM "%s"."%s"`, app.DB.Config.Schema, app.Config.HistoryTable))
	defer rows.Close()

	if err != nil {
		return fmt.Errorf("%w: %v", errs.FailedToGetHistoryRecord, err)
	}

	for rows.Next() {
		historyRecordRow := historyRecord{}

		_ = rows.Scan(&historyRecordRow.TriggerTable, &historyRecordRow.ChangeTable, &historyRecordRow.InsertTrigger, &historyRecordRow.UpdateTrigger, &historyRecordRow.DeleteTrigger, &historyRecordRow.InsertFunction, &historyRecordRow.UpdateFunction, &historyRecordRow.DeleteFunction)

		// drop change table
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-table", map[string]string{
				"<TABLE>": historyRecordRow.ChangeTable,
			}),
		})

		// insert trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecordRow.InsertTrigger,
				"<TABLE>":   historyRecordRow.TriggerTable,
			}),
		})

		// insert function
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-function", map[string]string{
				"<FUNCTION>": historyRecordRow.InsertFunction,
			}),
		})

		// update trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecordRow.UpdateTrigger,
				"<TABLE>":   historyRecordRow.TriggerTable,
			}),
		})

		// update function
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-function", map[string]string{
				"<FUNCTION>": historyRecordRow.UpdateFunction,
			}),
		})

		// delete trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecordRow.DeleteTrigger,
				"<TABLE>":   historyRecordRow.TriggerTable,
			}),
		})

		// delete function
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("pgsql-drop-function", map[string]string{
				"<FUNCTION>": historyRecordRow.DeleteFunction,
			}),
		})
	}

	// drop history table
	SQLStatements = append(SQLStatements, map[string]any{
		"query": stub.Read("pgsql-drop-table", map[string]string{
			"<TABLE>": fmt.Sprintf("%s.%s", app.DB.Config.Schema, app.Config.HistoryTable),
		}),
	})

	db.Transaction(app.DB.Conn, func(tx *sql.Tx) error {
		var (
			query string
			err   error
		)

		for _, SQLStatement := range SQLStatements {
			query = SQLStatement["query"].(string)
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

	return nil
}
