package cmd

import (
	"errors"
	"github.com/spf13/cobra"
	"inspectadb/src/config"
	"inspectadb/src/driver"
)

var auditCmd = &cobra.Command{
	Use:   "audit",
	Short: "Setup the record auditing.",
	Long:  "Setup the record auditing. This will create new tables (and other objects such as triggers, functions etc. as needed)",
	RunE: func(cmd *cobra.Command, args []string) error {
		app, err := config.Load(configPath)

		if err != nil {
			return errors.Join(errors.New("failed to load config"), err)
		}

		d := driver.Get(app.Config.DB.Driver)

		err = d.Audit(app)

		if err != nil {
			return err
		}

		return nil
	},
}
