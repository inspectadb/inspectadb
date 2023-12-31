package util

import (
	"github.com/iancoleman/strcase"
	"strings"
)

func FormatByStrategies(v string, namingStrategy string, caseStrategy string) string {
	if namingStrategy == "camel" {
		v = strcase.ToLowerCamel(v)
	} else if namingStrategy == "pascal" {
		v = strcase.ToCamel(v)
	} else {
		v = strings.ReplaceAll(v, " ", "_")
	}

	if caseStrategy == "upper" {
		return strings.ToUpper(v)
	} else {
		return strings.ToLower(v)
	}
}

func BuildIdentifierName(maxLength int, ids ...string) string {
	id := joinNonEmpty(ids, " ")

	if len(id) > maxLength {
		return id[:maxLength]
	}

	return id
}
