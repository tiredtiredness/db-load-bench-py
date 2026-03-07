package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

type CSVData struct {
	Headers []string
	Rows    [][]string
}

func readCSV(path string) (*CSVData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("csv is empty")
	}

	headers, err := parseWrappedLine(lines[0])
	if err != nil {
		return nil, fmt.Errorf("parse headers: %w", err)
	}
	if len(headers) > 0 {
		headers[0] = strings.TrimPrefix(headers[0], "\xef\xbb\xbf")
	}

	var rows [][]string
	for i, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}
		row, err := parseWrappedLine(line)
		if err != nil {
			return nil, fmt.Errorf("parse row %d: %w", i+1, err)
		}
		rows = append(rows, row)
	}

	return &CSVData{Headers: headers, Rows: rows}, nil
}

func parseWrappedLine(line string) ([]string, error) {
	unwrapped  := unwrapOuter(line)
	normalized := strings.ReplaceAll(unwrapped, `""`, `"`)

	reader                  := csv.NewReader(strings.NewReader(normalized))
	reader.LazyQuotes        = true
	reader.TrimLeadingSpace  = true

	fields, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("csv parse %q: %w", normalized, err)
	}

	for i, f := range fields {
		fields[i] = cleanStr(f)
	}

	return fields, nil
}

func unwrapOuter(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func cleanStr(s string) string {
	for {
		stripped := strings.TrimSpace(s)
		stripped  = strings.Trim(stripped, `"`)
		stripped  = strings.Trim(stripped, "`")
		stripped  = strings.Trim(stripped, `'`)
		stripped  = strings.TrimSpace(stripped)
		if stripped == s {
			break
		}
		s = stripped
	}
	return s
}