package main

import (
	"embed"
	"github.com/inspectadb/inspectadb/cmd"
	"github.com/inspectadb/inspectadb/driver"
	"github.com/inspectadb/inspectadb/driver/mysql"
	"github.com/inspectadb/inspectadb/util"
	"log"
	"os"
)

//go:embed stubs/*.stub
var stubsFolder embed.FS

func init() {
	util.StubsFolder = stubsFolder

	driver.Register("mysql", &mysql.MySQLDriver{})
	driver.Register("maria", &mysql.MySQLDriver{})
}

func main() {
	err := cmd.Execute()

	if err != nil {
		log.Fatalln(err)
	}

	os.Exit(0)
}
