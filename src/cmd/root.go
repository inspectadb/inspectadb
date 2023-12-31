package cmd

import (
	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use:   "inspecta",
	Short: "Inspecta automates the setup of change data capture through audit tables seamlessly.",
}

func init() {
	rootCmd.AddCommand(auditCmd)
	rootCmd.AddCommand(purgeCmd)
	rootCmd.AddCommand(versionCmd)

	auditCmd.Flags().StringVar(&configPath, "config", "", "Path to config file")
	purgeCmd.Flags().StringVar(&configPath, "config", "", "Path to config file")
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}
