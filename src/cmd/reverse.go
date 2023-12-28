package cmd

import (
	"errors"
	"github.com/inspectadb/inspectadb/src/config"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/spf13/cobra"
)

var clean bool

var reverseCmd = &cobra.Command{
	Use:   "reverse",
	Short: "Reverse changes made by Inspecta. This will remove audit tables (and other objects listed in the history table)",
	RunE: func(cmd *cobra.Command, args []string) error {
		app, err := config.Load(configPath)

		if err != nil {
			return errors.Join(errors.New("failed to load config"), err)
		}

		d := driver.Get(app.Config.DB.Driver)

		err = d.Reverse(app, clean)

		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	reverseCmd.Flags().BoolVar(&clean, "clean", false, "Removes the history (metadata) table")
}
