package main

import (
	"embed"
	"github.com/inspectadb/inspectadb/src/cmd"
	"github.com/inspectadb/inspectadb/src/driver"
	"github.com/inspectadb/inspectadb/src/driver/mysql"
	"github.com/inspectadb/inspectadb/src/stub"
	"log"
	"os"
)

//go:embed stubs/*.stub
var stubsFolder embed.FS

func init() {
	stub.StubsFolder = stubsFolder

	driver.Register("mysql", &mysql.MySQLDriver{})
	driver.Register("mariadb", &mysql.MySQLDriver{})
}

func main() {
	err := cmd.Execute()

	if err != nil {
		log.Fatalln(err)
	}

	os.Exit(0)
}
