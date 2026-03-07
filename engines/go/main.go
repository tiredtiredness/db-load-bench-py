package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type Result struct {
	Engine           string                 `json:"engine"`
	DbType           string                 `json:"db_type"`
	Method           string                 `json:"method"`
	ExperimentConfig map[string]int         `json:"experiment_config"`
	MethodConfig     map[string]interface{} `json:"method_config"`
	Metrics          map[string]float64     `json:"metrics"`
}

func main() {
	method    := flag.String("method",     "default_insert", "Insert method")
	csvFile   := flag.String("csv",        "",               "Path to CSV file")
	tableName := flag.String("table",      "Test",           "Table name")
	dbType    := flag.String("db-type",    "mysql",          "Database type: mysql | postgresql")
	host      := flag.String("host",       "localhost",      "Database host")
	port      := flag.Int("port",          3306,             "Database port")
	user      := flag.String("user",       "",               "Database user")
	password  := flag.String("password",   "",               "Database password")
	database  := flag.String("database",   "",               "Database name")
	batchSize := flag.Int("batch-size",    1000,             "Batch size for bulk_insert")
	flag.Parse()

	if *csvFile == "" {
		fmt.Fprintln(os.Stderr, "error: --csv is required")
		os.Exit(1)
	}

	inserter, err := NewInserter(*dbType, *host, *port, *user, *password, *database)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection error:", err)
		os.Exit(1)
	}
	defer inserter.Close()

	rows, elapsed, err := inserter.Run(*method, *csvFile, *tableName, *batchSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, "insert error:", err)
		os.Exit(1)
	}

	var batchSizeVal interface{}
	if *method == "bulk_insert" {
		batchSizeVal = *batchSize
	}

	result := Result{
		Engine: "Go",
		DbType: *dbType,
		Method: *method,
		ExperimentConfig: map[string]int{
			"rows": rows,
		},
		MethodConfig: map[string]interface{}{
			"batch_size": batchSizeVal,
		},
		Metrics: map[string]float64{
			"elapsed": elapsed,
			"rps":     float64(rows) / elapsed,
		},
	}

	out, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintln(os.Stderr, "json marshal error:", err)
		os.Exit(1)
	}

	fmt.Println(string(out))
}