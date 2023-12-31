package cmd

import (
	"errors"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/profiler"
	"github.com/inspectadb/inspectadb/src/telemetry"
	"github.com/spf13/cobra"
)

var clean bool

var reverseCmd = &cobra.Command{
	Use:   "reverse",
	Short: "Reverse changes made by Inspecta.",
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
		err = d.Reverse(app, clean)

		if err != nil {
			return err
		}

		profile.End()

		if app.Config.Telemetry {
			version, _ := d.GetServerVersion(app.Config.DB)

			telemetry.NewSignal(
				"reverse",
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

func init() {
	reverseCmd.Flags().BoolVar(&clean, "clean", false, "Removes the history (metadata) table")
}
