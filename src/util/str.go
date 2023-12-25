package util

import (
	"fmt"
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

func BuildAuditTableName(prefix string, table string, suffix string, maxLength int) string {
	name := fmt.Sprintf("%s %s %s", prefix, table, suffix)

	if len(name) > maxLength {
		return name[:maxLength]
	}

	return name
}

func BuildIdentifierName(maxLength int, ids ...string) string {
	id := joinNonEmpty(ids, " ")

	if len(id) > maxLength {
		return id[:maxLength]
	}

	return id
}
