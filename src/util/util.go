package util

import (
	"fmt"
	"github.com/google/uuid"
	"strings"
)

func StringSliceToAnySlice(s []string) []any {
	as := []any{}

	for _, v := range s {
		as = append(as, v)
	}

	return as
}

func UUIDWithoutHyphens() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

func joinNonEmpty(items []string, sep string) string {
	var nonEmptyItems []string

	for _, item := range items {
		if item != "" {
			nonEmptyItems = append(nonEmptyItems, item)
		}
	}

	return strings.Join(nonEmptyItems, sep)
}

func BuildChangeTableName(prefix string, triggerTable string, suffix string, maxLength int) string {
	name := joinNonEmpty([]string{prefix, triggerTable, suffix}, "_")
	return name[:min(len(name), maxLength)]
}

func BuildTriggerName(triggerTable string, action string, maxLength int) string {
	name := fmt.Sprintf("inspectadb_%s_%s_trgr", triggerTable, action)
	return name[:min(len(name), maxLength)]
}

func BuildFunctionName(triggerTable, action string, maxLength int) string {
	name := fmt.Sprintf("inspectadb_%s_%s_fn", triggerTable, action)
	return name[:min(len(name), maxLength)]
}
