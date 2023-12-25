package cmd

import (
	"github.com/spf13/cobra"
)

var configPath string

var rootCmd = &cobra.Command{
	Use:   "inspecta",
	Short: "Simple, automated record auditing",
}

func init() {
	rootCmd.AddCommand(auditCmd)
	rootCmd.AddCommand(reverseCmd)
	rootCmd.AddCommand(versionCmd)

	auditCmd.Flags().StringVar(&configPath, "config", "", "Path to config file")
	reverseCmd.Flags().StringVar(&configPath, "config", "", "Path to config file")
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		return err
	}

	return nil
}
