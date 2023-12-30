package cmd

import (
	"errors"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/profiler"
	"github.com/inspectadb/inspectadb/src/telemetry"
	"github.com/spf13/cobra"
)

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Setup change data capture.",
	RunE: func(cmd *cobra.Command, args []string) error {
		app, err := config.Load(configPath)

		if err != nil {
			return errors.Join(errors.New("failed to load config"), err)
		}

		d := driver.Get(app.Config.DB.Driver)

		if !d.VerifyLicense(app) {
			return errors.New("failed to verify license, cannot proceed")
		}

		profile := profiler.New()
		err = d.Audit(app)

		if err != nil {
			return err
		}

		profile.End()

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
