package cmd

import (
	"errors"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/profiler"
	"github.com/inspectadb/inspectadb/src/telemetry"
	"github.com/spf13/cobra"
)

var purgeCmd = &cobra.Command{
	Use:   "purge",
	Short: "Purge all changes made by Inspecta (removes history table, audit tables, triggers etc.).",
	RunE: func(cmd *cobra.Command, args []string) error {
		app, err := config.Load(configPath)

		if err != nil {
			return errors.Join(errors.New("failed to load config"), err)
		}

		d, err := driver.Get(app.Config.DB.Driver)

		if err != nil {
			return err
		}

		profile := profiler.New()
		err = d.Purge(app)

		if err != nil {
			return err
		}

		profile.End()

		if app.Config.Telemetry {
			version, _ := d.GetServerVersion(app.Config.DB)

			telemetry.NewSignal(
				"purge",
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

func init() {}
