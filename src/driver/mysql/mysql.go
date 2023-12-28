package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/db"
	"github.com/inspectadb/inspectadb/util"
	"log"
	"strings"
)

// Notes:
// https://dev.mysql.com/doc/refman/5.7/en/cannot-roll-back.html - DDL statements cannot be rolled back

// TODO: Columns hardcoded in queries and other places need to be dynamic
// TODO: Alternative db
// TODO: Setup manual rollback i.e. cleanup of all created refs on failure
// TODO: Fix exclude placeholder
// TODO: Remove redundancy for already audited and unaudited table (building triggers and functions etc.)

type MySQLDriver struct{}

type historyRecord struct {
	Schema        string
	Action        string
	OriginalTable string
	AuditTable    string
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

// GetColumnsToSyncSQL
// Params for A: original_table, schema, original_table, schema, audit_table, schema
// Params for D: audit_table, schema, original_table, schema, EXCLUDE COLUMNS (x4)
// Params for M: original_table, schema, original_table, audit_table, schema, schema
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
			    B.TABLE_NAME = ? AND 
			    A.TABLE_SCHEMA = ? AND 
			    B.TABLE_SCHEMA = ? AND 
			    A.COLUMN_NAME = B.COLUMN_NAME AND 
				(
					A.COLUMN_TYPE != B.COLUMN_TYPE OR
					A.ORDINAL_POSITION != B.ORDINAL_POSITION - 4
				)
			ORDER BY ORDINAL_POSITION ASC;`
}

func (d MySQLDriver) BuildPlaceholders(totalNoOfPlaceholders int, startFrom int) string {
	return strings.Repeat(", ?", totalNoOfPlaceholders)
}

func (d MySQLDriver) Connect(dbConfig config.DBConfig) (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 dbConfig.User,
		Passwd:               dbConfig.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%d", dbConfig.Host, dbConfig.Port),
		DBName:               dbConfig.Schema,
		AllowNativePasswords: true,
	}

	conn, err := sql.Open("mysql", cfg.FormatDSN())

	if err != nil {
		return conn, errors.Join(errors.New("failed to initialize db driver 'mysql'"), err)
	}

	if err := conn.Ping(); err != nil {
		return conn, errors.Join(errors.New("failed to connect to db"), err)
	}

	return conn, nil
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

	formatByStrategies := func(v string) string {
		return util.FormatByStrategies(v, app.Config.NamingStrategy, app.Config.CaseStrategy)
	}

	conn, err := d.Connect(app.Config.DB)

	if err != nil {
		return err
	}

	historyTableSQL := util.ReadStub("mysql-create-history-table", map[string]string{
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
					TABLE_SCHEMA = ? AND
					TABLE_TYPE = "BASE TABLE" AND
					TABLE_NAME NOT IN (?<EXCLUDE>) AND
					TABLE_NAME NOT IN (SELECT audit_table FROM <SCHEMA>.<TABLE>)`
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<SCHEMA>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.DB.Schema)))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<EXCLUDE>", d.BuildPlaceholders(len(app.Config.Exclude), 0))
	getTablesSQL = strings.ReplaceAll(getTablesSQL, "<TABLE>", fmt.Sprintf("%s", d.WrapIdentifier(app.Config.HistoryTable)))

	tables, err := db.GetTables(conn, getTablesSQL, append([]any{app.Config.DB.Schema, app.Config.HistoryTable}, util.StringSliceToAnySlice(app.Config.Exclude)...))

	if err != nil {
		return errors.Join(errors.New("failed to get tables"), err)
	}

	for _, table := range tables {
		createAuditTable := false

		auditTable := ""
		insertTrigger := formatByStrategies(util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", table, "ins", "trgr", util.UUIDWithoutHyphens()))
		updateTrigger := formatByStrategies(util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", table, "upd", "trgr", util.UUIDWithoutHyphens()))
		deleteTrigger := formatByStrategies(util.BuildIdentifierName(d.GetIdentifierMaxLength(), "inspecta", table, "del", "trgr", util.UUIDWithoutHyphens()))
		triggerOptions := []map[string]any{}

		historyRecord := historyRecord{}
		err := conn.QueryRow(
			"SELECT `original_table`, `audit_table`, `insert_trigger`, `update_trigger`, `delete_trigger` FROM `"+app.Config.HistoryTable+"` WHERE original_table = ?", table).Scan(
			&historyRecord.OriginalTable,
			&historyRecord.AuditTable,
			&historyRecord.InsertTrigger,
			&historyRecord.UpdateTrigger,
			&historyRecord.DeleteTrigger,
		)

		if !errors.Is(err, sql.ErrNoRows) && err != nil {
			return err
		}

		// hasn't been audited
		if errors.Is(err, sql.ErrNoRows) {
			createAuditTable = true
			auditTable = formatByStrategies(util.BuildIdentifierName(d.GetIdentifierMaxLength(), app.Config.AuditTablePrefix, table, app.Config.AuditTableSuffix))

			SQLStatements = append(SQLStatements, map[string]any{
				"query": util.ReadStub("mysql-create-audit-table", map[string]string{
					"<SCHEMA>":         app.Config.DB.Schema,
					"<AUDIT_TABLE>":    auditTable,
					"<AUDIT_ID>":       "audit_id",
					"<AUDIT_ACTION>":   "audit_action",
					"<AUDIT_USER>":     "audit_user",
					"<AUDITED_AT>":     "audited_at",
					"<ORIGINAL_TABLE>": table,
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
				"query": fmt.Sprintf("INSERT INTO `%s`.`%s` (`original_table`, `audit_table`, `insert_trigger`, `update_trigger`, `delete_trigger`, `user`) VALUES (?, ?, ?, ?, ?, CURRENT_USER())", app.Config.DB.Schema, app.Config.HistoryTable),
				"params": []any{
					table,
					auditTable,
					insertTrigger,
					updateTrigger,
					deleteTrigger,
				},
			})
		} else {
			// table has been audited
			auditTable = historyRecord.AuditTable
			createAuditTable = false

			// Params for A: original_table, schema, original_table, schema, audit_table, schema
			// Params for D: audit_table, schema, original_table, schema, EXCLUDE COLUMNS (x4)
			// Params for M: original_table, audit_table, schema, schema
			changedColumnsRows, err := conn.Query(d.GetColumnsToSyncSQL(),
				// A
				historyRecord.OriginalTable,
				app.Config.DB.Schema,
				historyRecord.OriginalTable,
				app.Config.DB.Schema,
				historyRecord.AuditTable,
				app.Config.DB.Schema,
				// D
				historyRecord.AuditTable,
				app.Config.DB.Schema,
				historyRecord.OriginalTable,
				app.Config.DB.Schema,
				"audit_id",
				"audit_action",
				"audit_user",
				"audited_at",
				// M
				historyRecord.OriginalTable,
				app.Config.DB.Schema,
				historyRecord.OriginalTable,
				historyRecord.AuditTable,
				app.Config.DB.Schema,
				app.Config.DB.Schema,
			)

			// need to sync columns
			hasNext := changedColumnsRows.Next()

			if errors.Is(err, sql.ErrNoRows) || !hasNext {
				log.Println(fmt.Sprintf("no drift detected between %s (original) and %s (audit), skipping...", historyRecord.OriginalTable, historyRecord.AuditTable))
				continue
			} else if err != nil {
				return err
			} else {
				log.Println(fmt.Sprintf("%s (original) and %s (audit) have drifted, reconciling...", historyRecord.OriginalTable, historyRecord.AuditTable))
			}

			for hasNext {
				var (
					action string
					column = db.InformationSchemaColumn{
						Schema: sql.NullString{String: app.Config.DB.Schema, Valid: true},
						Table:  sql.NullString{String: historyRecord.AuditTable, Valid: true},
					}
					SQL string
				)

				_ = changedColumnsRows.Scan(&action, &column.Name, &column.Type, &column.Position, &column.After)

				if column.Position.Valid {
					if !column.After.Valid && column.Position.Int16 == int16(1) {
						column.After = sql.NullString{String: "audited_at", Valid: true}
					}
				}

				if action == "ADD" {
					SQL = util.ReadStub("mysql-add-column", map[string]string{
						"<SCHEMA>": column.Schema.String,
						"<TABLE>":  column.Table.String,
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
				} else if action == "MODIFY" {
					SQL = util.ReadStub("mysql-modify-column", map[string]string{
						"<SCHEMA>": column.Schema.String,
						"<TABLE>":  column.Table.String,
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
				} else if action == "DROP" {
					SQL = util.ReadStub("mysql-drop-column", map[string]string{
						"<SCHEMA>": column.Schema.String,
						"<TABLE>":  column.Table.String,
						"<COLUMN>": column.Name,
					})
				}

				if _, err := conn.Exec(SQL); err != nil {
					return errors.Join(errors.New("failed to synchronize ("+action+") column: "+SQL), err)
				}

				hasNext = changedColumnsRows.Next()
			}

			SQLStatements = append(SQLStatements, map[string]any{
				"query": fmt.Sprintf("UPDATE `%s`.`%s` SET `performed_at` = NOW() WHERE `original_table` = ? AND `audit_table` = ?", app.Config.DB.Schema, app.Config.HistoryTable),
				"params": []any{
					historyRecord.OriginalTable,
					historyRecord.AuditTable,
				},
			})

			triggerOptions = []map[string]any{
				{
					"trigger": historyRecord.InsertTrigger,
					"action":  "INSERT",
				},
				{
					"trigger": historyRecord.UpdateTrigger,
					"action":  "UPDATE",
				},
				{
					"trigger": historyRecord.DeleteTrigger,
					"action":  "DELETE",
				},
			}
		}

		columns := ""

		rows, err := conn.Query(`SELECT
				COLUMN_NAME,
				COLUMN_TYPE
			FROM information_schema.COLUMNS
			WHERE
				TABLE_SCHEMA = ? AND
				TABLE_NAME = ?
			ORDER BY ORDINAL_POSITION ASC;`, app.Config.DB.Schema, table)

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
				columns += fmt.Sprintf("%s.%s, ", "<KEYWORD>", d.WrapIdentifier(column.Name))
			} else {
				columns += fmt.Sprintf("%s.%s", "<KEYWORD>", d.WrapIdentifier(column.Name))
			}
		}

		insertStatement := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", app.Config.DB.Schema, auditTable, "INSERT", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		updateStatement := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", app.Config.DB.Schema, auditTable, "UPDATE", strings.ReplaceAll(columns, "<KEYWORD>", "new"))
		deleteStatement := fmt.Sprintf("INSERT INTO `%s`.`%s` VALUES (NULL, '%s', CURRENT_USER(), NOW(), %s);", app.Config.DB.Schema, auditTable, "DELETE", strings.ReplaceAll(columns, "<KEYWORD>", "old"))

		for _, triggerOption := range triggerOptions {
			if !createAuditTable {
				SQLStatements = append(SQLStatements, map[string]any{
					"query": util.ReadStub("mysql-drop-trigger", map[string]string{
						"<SCHEMA>":  app.Config.DB.Schema,
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
				"query": util.ReadStub("mysql-create-trigger", map[string]string{
					"<TRIGGER>":   trigger,
					"<ACTION>":    action,
					"<SCHEMA>":    app.Config.DB.Schema,
					"<TABLE>":     table,
					"<STATEMENT>": statement,
				}),
			})
		}
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

func (d MySQLDriver) Reverse(app config.App, clean bool) error {
	SQLStatements := []map[string]any{
		{
			"query": "SET SESSION sql_mode='';",
		},
		{
			"query": "SET SESSION foreign_key_checks = 0;",
		},
	}

	conn, err := d.Connect(app.Config.DB)

	if err != nil {
		return err
	}

	historyTableSQL := util.ReadStub("mysql-create-history-table", map[string]string{
		"<SCHEMA>": app.Config.DB.Schema,
		"<TABLE>":  app.Config.HistoryTable,
	})

	if err := db.CreateHistoryTable(conn, historyTableSQL); err != nil {
		return err
	}

	rows, err := conn.Query("SELECT `original_table`, `audit_table`, `insert_trigger`, `update_trigger`, `delete_trigger` FROM `" + app.Config.HistoryTable + "`")
	defer rows.Close()

	if err != nil {
		return errors.Join(errors.New("failed to get history records"), err)
	}

	for rows.Next() {
		historyRecord := historyRecord{}
		historyRecord.Schema = app.Config.DB.Schema

		rows.Scan(&historyRecord.OriginalTable, &historyRecord.AuditTable, &historyRecord.InsertTrigger, &historyRecord.UpdateTrigger, &historyRecord.DeleteTrigger)

		// drop audit table
		SQLStatements = append(SQLStatements, map[string]any{
			"query": util.ReadStub("mysql-drop-table", map[string]string{
				"<SCHEMA>": app.Config.DB.Schema,
				"<TABLE>":  historyRecord.AuditTable,
			}),
		})

		// drop insert trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": util.ReadStub("mysql-drop-trigger", map[string]string{
				"<SCHEMA>":  app.Config.DB.Schema,
				"<TRIGGER>": historyRecord.InsertTrigger,
			}),
		})

		// drop update trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": util.ReadStub("mysql-drop-trigger", map[string]string{
				"<SCHEMA>":  app.Config.DB.Schema,
				"<TRIGGER>": historyRecord.UpdateTrigger,
			}),
		})

		// drop delete trigger
		SQLStatements = append(SQLStatements, map[string]any{
			"query": util.ReadStub("mysql-drop-trigger", map[string]string{
				"<SCHEMA>":  app.Config.DB.Schema,
				"<TRIGGER>": historyRecord.DeleteTrigger,
			}),
		})

		// remove respective record from history table
		if !clean {
			SQLStatements = append(SQLStatements, map[string]any{
				"query":  fmt.Sprintf("DELETE FROM %s.%s WHERE original_table = ? AND audit_table = ?", app.Config.DB.Schema, app.Config.HistoryTable),
				"params": []any{historyRecord.OriginalTable, historyRecord.AuditTable},
			})
		}
	}

	// clean = remove entire history table
	if clean {
		SQLStatements = append(SQLStatements, map[string]any{
			"query": util.ReadStub("mysql-drop-table", map[string]string{
				"<SCHEMA>": app.Config.DB.Schema,
				"<TABLE>":  app.Config.HistoryTable,
			}),
		})
	}

	db.WithTransaction(conn, func(tx *sql.Tx) error {
		for _, SQLStatementSync := range SQLStatements {
			query := SQLStatementSync["query"].(string)
			params, hasParams := SQLStatementSync["params"].([]any)

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
