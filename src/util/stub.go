package util

import (
	"embed"
	"fmt"
	"log"
	"strings"
)

var (
	StubsFolder embed.FS
)

func ReadStub(path string, args map[string]string) string {
	contents, err := StubsFolder.ReadFile(fmt.Sprintf("stubs/%s.stub", path))

	if err != nil {
		log.Fatalf("failed to read stub file: %s. %s.", path, err)
	}

	contentsStr := string(contents)

	for search, replace := range args {
		contentsStr = strings.ReplaceAll(contentsStr, search, replace)
	}

	return contentsStr
}
