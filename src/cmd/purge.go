package cmd

import (
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/db"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/errs"
	"github.com/inspectadb/inspectadb/src/lang"
	"github.com/inspectadb/inspectadb/src/profiler"
	"github.com/inspectadb/inspectadb/src/telemetry"
	"github.com/spf13/cobra"
	"log"
	"math"
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purge all changes made by Inspecta (removes history table, audit tables, triggers etc.).",
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			app config.App
			err error
			d   driver.Driver
		)

		if app, err = config.Load(configPath); err != nil {
			return err
		}

		if d, err = driver.Get(app.DB.Config.Driver); err != nil {
			return err
		}

		if !d.VerifyLicense(app) {
			return errs.FailedToVerifyLicense
		}

		if err = db.Connect(d, &app); err != nil {
			return err
		}

		profile := profiler.New()

		if err := db.CreateHistoryTable(app.DB.Conn, d.GetCreateHistoryTableSQL(app)); err != nil {
			return err
		}

		if err = d.Purge(app); err != nil {
			return err
		}

		profile.End()

		log.Printf(lang.PurgeCompleted, math.Round(profile.Delta.Seconds()*100)/100)

		if app.Config.Telemetry {
			version, _ := db.GetServerVersion(app.DB.Conn, d.GetServerVersionSQL())

			telemetry.NewSignal(
				"purge",
				version,
				"",
				map[string]any{
					"start":   profile.StartedAt.Unix(),
					"elapsed": profile.Delta.Nanoseconds(),
				},
			).Send()
		}

		return nil
	},
}

func init() {}
