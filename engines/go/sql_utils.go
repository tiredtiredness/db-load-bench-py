package main

import (
	"fmt"
	"strings"
)

func cleanIdentifier(name string) string {
	return cleanStr(name)
}

func quoteIdentifier(dbType, name string) string {
	clean := cleanIdentifier(name)
	switch dbType {
	case "mysql":
		return "`" + strings.ReplaceAll(clean, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(clean, `"`, `""`) + `"`
	}
}

func placeholder(dbType string, i int) string {
	if dbType == "postgresql" {
		return fmt.Sprintf("$%d", i+1)
	}
	return "?"
}

func buildInsertSQL(dbType, tableName string, headers []string) string {
	cols := make([]string, len(headers))
	phs  := make([]string, len(headers))
	for i, h := range headers {
		cols[i] = quoteIdentifier(dbType, h)
		phs[i]  = placeholder(dbType, i)
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(dbType, tableName),
		strings.Join(cols, ", "),
		strings.Join(phs, ", "),
	)
}

func rowToArgs(row []string) []interface{} {
	args := make([]interface{}, len(row))
	for i, v := range row {
		args[i] = v
	}
	return args
}