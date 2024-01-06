package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/db"
	"github.com/inspectadb/inspectadb/src/errs"
	"github.com/inspectadb/inspectadb/src/lang"
	"github.com/inspectadb/inspectadb/src/stub"
	"github.com/inspectadb/inspectadb/src/util"
	"log"
	"strings"
)

type MySQLDriver struct{}

type historyRecord struct {
	Action        string
	TriggerTable  string
	ChangeTable   string
	InsertTrigger string
	UpdateTrigger string
	DeleteTrigger string
}

func (d MySQLDriver) GetIdentifierMaxLength() int {
	// 64 is common for newer but to avoid
	// issues with NDB storage engine < 8.0.18
	// we set it to
	return 63
}

func (d MySQLDriver) WrapIdentifier(identifier string) string {
	return "`" + identifier + "`"
}

func (d MySQLDriver) DebugQuery(SQL string, params []any) {
	log.Println(fmt.Sprintf(strings.ReplaceAll(SQL, "?", `"%v"`), params...))
}

func (d MySQLDriver) BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string {
	return strings.Repeat(", ?", totalNoOfPlaceholders)
}

func (d MySQLDriver) BuildDSN(dbConfig config.DBConfig) string {
	cfg := mysqlDriver.Config{
		User:                 dbConfig.User,
		Passwd:               dbConfig.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", dbConfig.Host, dbConfig.Port),
		DBName:               dbConfig.Schema,
		AllowNativePasswords: true,
	}

	return cfg.FormatDSN()
}

func (d MySQLDriver) VerifyLicense(app config.App) bool {
	return true
}

func (d MySQLDriver) GetServerVersionSQL() string {
	return "SELECT @@version;"
}

func (d MySQLDriver) GetCreateHistoryTableSQL(app config.App) string {
	return stub.Read("mysql-create-history-table", map[string]string{
		"<TABLE>": fmt.Sprintf("%s.%s", app.DB.Config.Schema, app.Config.HistoryTable),
	})
}

// GetColumnsToSyncSQL
// Params for A: trigger_table, schema, trigger_table, schema, audit_table, schema
// Params for D: audit_table, schema, trigger_table, schema, change_id, change_action, changed_by, changed_at
// Params for M: trigger_table, schema, trigger_table, schema, change_table, schema
func (d MySQLDriver) GetColumnsToSyncSQL() string {
	return `SELECT 
				'ADD' AS ACTION,
				A.COLUMN_NAME,
				A.COLUMN_TYPE,
				A.ORDINAL_POSITION,
				(
					SELECT COLUMN_NAME
						FROM information_schema.COLUMNS C
					WHERE 
						C.TABLE_NAME = ? AND 
						C.TABLE_SCHEMA = ? AND 
						C.ORDINAL_POSITION = A.ORDINAL_POSITION - 1
					ORDER BY C.ORDINAL_POSITION LIMIT 1
				) AS COLUMN_TO_GO_AFTER
			FROM
				information_schema.COLUMNS A
			WHERE
				A.TABLE_NAME = ?
				AND A.TABLE_SCHEMA = ?
				AND COLUMN_NAME NOT IN (
					SELECT COLUMN_NAME
					FROM information_schema.COLUMNS B
					WHERE B.TABLE_NAME = ? AND B.TABLE_SCHEMA = ?
				)
			
			UNION
			
			SELECT 
				'DROP' AS ACTION,
				COLUMN_NAME,
				COLUMN_TYPE,
				ORDINAL_POSITION,
				NULL AS COLUMN_TO_GO_AFTER
			FROM
				information_schema.COLUMNS B
			WHERE
				B.TABLE_NAME = ?
				AND B.TABLE_SCHEMA = ?
				AND COLUMN_NAME NOT IN (
					SELECT COLUMN_NAME
					FROM information_schema.COLUMNS A
					WHERE A.TABLE_NAME = ? AND A.TABLE_SCHEMA = ?
				)
				AND COLUMN_NAME NOT IN (?, ?, ?, ?)
			
			UNION
			
			SELECT 
				'MODIFY' AS ACTION,
				A.COLUMN_NAME,
				A.COLUMN_TYPE,
				A.ORDINAL_POSITION,
				(
					SELECT COLUMN_NAME
						FROM information_schema.COLUMNS C
					WHERE 
						C.TABLE_NAME = ? AND 
						C.TABLE_SCHEMA = ? AND 
						C.ORDINAL_POSITION = A.ORDINAL_POSITION - 1
					ORDER BY C.ORDINAL_POSITION LIMIT 1
				) AS COLUMN_TO_GO_AFTER
			FROM
				information_schema.COLUMNS A
			JOIN
				information_schema.COLUMNS B
			ON
				A.TABLE_NAME = ? AND 
			    A.TABLE_SCHEMA = ? AND
			    B.TABLE_NAME = ? AND 
			    B.TABLE_SCHEMA = ? AND 
			    A.COLUMN_NAME = B.COLUMN_NAME AND 
				(
					A.COLUMN_TYPE != B.COLUMN_TYPE OR
					A.ORDINAL_POSITION != B.ORDINAL_POSITION - 4
				)
			ORDER BY ORDINAL_POSITION ASC;`
}

func (d MySQLDriver) Audit(app config.App) error {
	SQLStatements := []map[string]any{
		{
			"query": "SET SESSION sql_mode='';",
		},
		{
			"query": "SET SESSION foreign_key_checks = 0;",
		},
	}
	triggerTableSchema := app.DB.Config.Schema
	changeTableSchema := app.DB.Config.Schema

	if app.Config.AlternateSchema != "" {
		changeTableSchema = app.Config.AlternateSchema
	}

	getTablesSQL := `SELECT
						TABLE_NAME
					FROM INFORMATION_SCHEMA.TABLES
					WHERE
						TABLE_SCHEMA = ? AND
						TABLE_TYPE = "BASE TABLE" AND
						TABLE_NAME NOT IN (?<EXCLUDE>) AND
						TABLE_NAME NOT IN (SELECT SUBSTRING_INDEX(change_table, '.', -1) FROM <SCHEMA>.<TABLE>)`
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<SCHEMA>", fmt.Sprintf("%s", d.WrapIdentifier(app.DB.Config.Schema)))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", d.BuildPlaceholders(len(app.Config.Exclude), 0))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<TABLE>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.HistoryTable)))

	triggerTables, err := db.GetTables(app.DB.Conn, getTablesSQL, append([]any{app.DB.Config.Schema, app.Config.HistoryTable}, util.StringSliceToAnySlice(app.Config.Exclude)...))

	if err != nil {
		return fmt.Errorf("%w: %v", errs.FailedToGetTriggerTables, err)
	}

	for _, triggerTable := range triggerTables {
		createAuditTable := false
		changeTable := ""
		insertTrigger := fmt.Sprintf("%s.%s", app.DB.Config.Schema, util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", triggerTable, "ins", "trgr", util.UUIDWithoutHyphens()))
		updateTrigger := fmt.Sprintf("%s.%s", app.DB.Config.Schema, util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", triggerTable, "upd", "trgr", util.UUIDWithoutHyphens()))
		deleteTrigger := fmt.Sprintf("%s.%s", app.DB.Config.Schema, util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", triggerTable, "del", "trgr", util.UUIDWithoutHyphens()))
		triggerOptions := []map[string]any{}

		historyRecordRow := historyRecord{}
		err := app.DB.Conn.QueryRow(
			"SELECT `trigger_table`, `change_table`, `insert_trigger`, `update_trigger`, `delete_trigger` FROM `"+app.Config.HistoryTable+"` WHERE trigger_table = ?", fmt.Sprintf("%s.%s", app.DB.Config.Schema, triggerTable)).Scan(
			&historyRecordRow.TriggerTable,
			&historyRecordRow.ChangeTable,
			&historyRecordRow.InsertTrigger,
			&historyRecordRow.UpdateTrigger,
			&historyRecordRow.DeleteTrigger,
		)

		if !errors.Is(err, sql.ErrNoRows) && err != nil {
			return fmt.Errorf("%w: %v", errs.FailedToGetHistoryRecord, err)
		}

		// hasn't been audited
		if errors.Is(err, sql.ErrNoRows) {
			createAuditTable = true
			changeTable = fmt.Sprintf("%s.%s", changeTableSchema, util.BuildIdentifierName(d.GetIdentifierMaxLength(), app.Config.ChangeTablePrefix, triggerTable, app.Config.ChangeTableSuffix))

			SQLStatements = append(SQLStatements, map[string]any{
				"query": stub.Read("mysql-create-change-table", map[string]string{
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

			SQLStatements = append(SQLStatements, map[string]any{
				"query": fmt.Sprintf("INSERT INTO `%s`.`%s` (`trigger_table`, `change_table`, `insert_trigger`, `update_trigger`, `delete_trigger`, `user`) VALUES (?, ?, ?, ?, ?, CURRENT_USER())", app.DB.Config.Schema, app.Config.HistoryTable),
				"params": []any{
					fmt.Sprintf("%s.%s", triggerTableSchema, triggerTable),
					changeTable,
					insertTrigger,
					updateTrigger,
					deleteTrigger,
				},
			})
		} else {
			// table has been audited
			changeTable = historyRecordRow.ChangeTable
			createAuditTable = false

			// format: schema.table
			triggerTableSplit := strings.Split(historyRecordRow.TriggerTable, ".")
			triggerTableSchema, triggerTable := triggerTableSplit[0], triggerTableSplit[1]
			changeTableSplit := strings.Split(historyRecordRow.ChangeTable, ".")
			changeTableSchema, changeTable := changeTableSplit[0], changeTableSplit[1]

			// Params for A: original_table, schema, original_table, schema, audit_table, schema
			// Params for D: audit_table, schema, original_table, schema, EXCLUDE COLUMNS (x4)
			// Params for M: original_table, audit_table, schema, schema
			changedColumnsRows, err := app.DB.Conn.Query(d.GetColumnsToSyncSQL(),
				// A
				triggerTable,
				triggerTableSchema,
				triggerTable,
				triggerTableSchema,
				changeTable,
				changeTableSchema,
				// D
				changeTable,
				changeTableSchema,
				triggerTable,
				triggerTableSchema,
				app.Config.ChangeIdColumn,
				app.Config.ChangeActionColumn,
				app.Config.ChangedByColumn,
				app.Config.ChangedAtColumn,
				// M
				triggerTable,
				triggerTableSchema,
				triggerTable,
				triggerTableSchema,
				changeTable,
				changeTableSchema,
			)

			if !errors.Is(err, sql.ErrNoRows) && err != nil {
				return err
			}

			hasNext := changedColumnsRows.Next()

			if errors.Is(err, sql.ErrNoRows) || !hasNext {
				log.Println(fmt.Sprintf(lang.NoDriftDetected, historyRecordRow.TriggerTable, historyRecordRow.ChangeTable))
				continue
			} else {
				log.Println(fmt.Sprintf(lang.DriftDetected, historyRecordRow.TriggerTable, historyRecordRow.ChangeTable))
			}

			for hasNext {
				var (
					action string
					column = db.InformationSchemaColumn{
						Schema: sql.NullString{String: changeTableSchema, Valid: true},
						Table:  sql.NullString{String: changeTable, Valid: true},
					}
					SQL string
				)

				_ = changedColumnsRows.Scan(&action, &column.Name, &column.Type, &column.Position, &column.After)

				if column.Position.Valid {
					if !column.After.Valid && column.Position.Int16 == int16(1) {
						column.After = sql.NullString{String: app.Config.ChangedAtColumn, Valid: true}
					}
				}

				if action == "ADD" {
					SQL = stub.Read("mysql-add-column", map[string]string{
						"<TABLE>":  fmt.Sprintf("%s.%s", changeTableSchema, column.Table.String),
						"<COLUMN>": column.Name,
						"<TYPE>":   column.Type,
						"<AFTER>":  column.After.String,
					})

					switch strings.ToLower(column.Type) {
					case "timestamp":
						SQL = strings.ReplaceAll(SQL, "<DEFAULT>", "NULL DEFAULT NULL")
					default:
						SQL = strings.ReplaceAll(SQL, "<DEFAULT> ", "")
					}

					SQLStatements = append(SQLStatements, map[string]any{
						"query": SQL,
					})
				} else if action == "MODIFY" {
					SQL = stub.Read("mysql-modify-column", map[string]string{
						"<TABLE>":  fmt.Sprintf("%s.%s", changeTableSchema, column.Table.String),
						"<COLUMN>": column.Name,
						"<TYPE>":   column.Type,
						"<AFTER>":  column.After.String,
					})

					switch strings.ToLower(column.Type) {
					case "timestamp":
						SQL = strings.ReplaceAll(SQL, "<DEFAULT>", "NULL DEFAULT NULL")
					default:
						SQL = strings.ReplaceAll(SQL, "<DEFAULT> ", "")
					}

					SQLStatements = append(SQLStatements, map[string]any{
						"query": SQL,
					})
				} else if action == "DROP" {
					SQLStatements = append(SQLStatements, map[string]any{
						"query": stub.Read("mysql-drop-column", map[string]string{
							"<TABLE>":  fmt.Sprintf("%s.%s", changeTableSchema, column.Table.String),
							"<COLUMN>": column.Name,
						}),
					})
				}

				hasNext = changedColumnsRows.Next()
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
			if !createAuditTable {
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
	}

	// we have two preparatory queries by default
	if len(SQLStatements) > 2 {
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

func (d MySQLDriver) Purge(app config.App) error {
	SQLStatements := []map[string]any{
		{
			"query": "SET SESSION sql_mode='';",
		},
		{
			"query": "SET SESSION foreign_key_checks = 0;",
		},
	}

	historyTableSQL := stub.Read("mysql-create-history-table", map[string]string{
		"<TABLE>": fmt.Sprintf("%s.%s", app.DB.Config.Schema, app.Config.HistoryTable),
	})

	if err := db.CreateHistoryTable(app.DB.Conn, historyTableSQL); err != nil {
		return err
	}

	rows, err := app.DB.Conn.Query(fmt.Sprintf("SELECT `trigger_table`, `change_table`, `insert_trigger`, `update_trigger`, `delete_trigger` FROM `%s`", app.Config.HistoryTable))
	defer rows.Close()

	if err != nil {
		return fmt.Errorf("%w: %v", errs.FailedToGetHistoryRecord, err)
	}

	for rows.Next() {
		historyRecord := historyRecord{}

		_ = rows.Scan(&historyRecord.TriggerTable, &historyRecord.ChangeTable, &historyRecord.InsertTrigger, &historyRecord.UpdateTrigger, &historyRecord.DeleteTrigger)

		// drop change table
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("mysql-drop-table", map[string]string{
				"<TABLE>": historyRecord.ChangeTable,
			}),
		})

		// drop insert trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("mysql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecord.InsertTrigger,
			}),
		})

		// drop update trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("mysql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecord.UpdateTrigger,
			}),
		})

		// drop delete trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": stub.Read("mysql-drop-trigger", map[string]string{
				"<TRIGGER>": historyRecord.DeleteTrigger,
			}),
		})
	}

	// drop history table
	SQLStatements = append(SQLStatements, map[string]any{
		"query": stub.Read("mysql-drop-table", map[string]string{
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
