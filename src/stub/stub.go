package stub

import (
	"embed"
	"fmt"
	"github.com/inspectadb/inspectadb/src/errs"
	"log"
	"strings"
)

var (
	StubsFolder embed.FS
)

func Read(path string, args map[string]string) string {
	contents, err := StubsFolder.ReadFile(fmt.Sprintf("stubs/%s.stub", path))

	if err != nil {
		log.Fatalf(fmt.Errorf("%w: %s", errs.FailedToReadStubFile, path).Error())
	}

	contentsStr := string(contents)

	for search, replace := range args {
		contentsStr = strings.ReplaceAll(contentsStr, search, replace)
	}

	return contentsStr
}
