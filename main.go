package main

import (
	"embed"
	"inspectadb/src/cmd"
	"inspectadb/src/driver"
	"inspectadb/src/driver/mysql"
	"inspectadb/src/driver/pgsql"
	"inspectadb/src/util"
	"log"
	"os"
)

//go:embed stubs/*.stub
var stubsFolder embed.FS

func init() {
	util.StubsFolder = stubsFolder

	driver.Register("mysql", &mysql.MySQLDriver{})
	driver.Register("pgsql", &pgsql.PGSQLDriver{})
}

func main() {
	err := cmd.Execute()

	if err != nil {
		log.Fatalln(err)
	}

	os.Exit(0)
}
