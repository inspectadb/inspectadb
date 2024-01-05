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

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Setup change data capture.",
	RunE: func(cmd *cobra.Command, args []string) error {
		app, err := config.Load(configPath)

		if err != nil {
			return err
		}

		d, err := driver.Get(app.DB.Config.Driver)

		if err != nil {
			return err
		}

		if !d.VerifyLicense(app) {
			return errs.FailedToVerifyLicense
		}

		conn, err := db.Connect(d, app.DB.Config)

		if err != nil {
			return err
		}

		app.DB.Conn = conn
		profile := profiler.New()
		err = d.Audit(app)

		if err != nil {
			return err
		}

		profile.End()

		log.Printf(lang.AuditCompleted, math.Round(profile.Delta.Seconds()*100)/100)

		if app.Config.Telemetry {
			version, _ := db.GetServerVersion(app.DB.Conn, d.GetServerVersionSQL())

			telemetry.NewSignal(
				"audit",
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
