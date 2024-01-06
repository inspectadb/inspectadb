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

// BuildIdentifierName
// TODO: Make sep an arg (?)
func BuildIdentifierName(maxLength int, ids ...string) string {
	id := joinNonEmpty(ids, "_")

	if len(id) > maxLength {
		return id[:maxLength]
	}

	return id
}

func BuildChangeTableName(prefix string) {

}

func BuildTriggerName(triggerTable string, action string, maxLength int) string {
	name := fmt.Sprintf("inspecta_%s_%s_trgr_%s", triggerTable, action, UUIDWithoutHyphens())
	return name[:min(len(name), maxLength)]
}

func BuildFunctionName(triggerTable, action string, maxLength int) string {
	name := fmt.Sprintf("inspecta_%s_%s_fn_%s", triggerTable, action, UUIDWithoutHyphens())
	return name[:min(len(name), maxLength)]
}
