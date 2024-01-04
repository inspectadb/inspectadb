package cmd

import (
	"github.com/inspectadb/inspectadb/src/config"
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

		d, err := driver.Get(app.Config.DB.Driver)

		if err != nil {
			return err
		}

		if !d.VerifyLicense(app) {
			return errs.FailedToVerifyLicense
		}

		profile := profiler.New()
		err = d.Audit(app)

		if err != nil {
			return err
		}

		profile.End()

		log.Printf(lang.AuditCompleted, math.Round(profile.Delta.Seconds()*100)/100)

		if app.Config.Telemetry {
			version, _ := d.GetServerVersion(app.Config.DB)

			telemetry.NewSignal(
				"audit",
				app.Config.DB.Driver,
				version,
				map[string]any{
					"start":   profile.StartedAt.Unix(),
					"elapsed": profile.Delta.Nanoseconds(),
				},
			).Send()
		}

		return nil
	},
}
