package cmd

import (
	"fmt"
	"github.com/inspectadb/inspectadb/src/consts"
	"github.com/spf13/cobra"
	"log"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of Inspecta",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println(fmt.Sprintf("v%s", consts.AppVersion))
	},
}
