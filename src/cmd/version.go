package cmd

import (
	"github.com/spf13/cobra"
	"log"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of Inspecta",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Insepcta v0.1")
	},
}
