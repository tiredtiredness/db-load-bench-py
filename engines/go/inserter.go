package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
)

type Inserter struct {
	db     *sql.DB
	dbType string
	// Параметры подключения нужны для file_insert (отдельное соединение)
	host, user, password, database string
	port                           int
}

func NewInserter(dbType, host string, port int, user, password, database string) (*Inserter, error) {
	db, err := newSQLDB(dbType, host, port, user, password, database)
	if err != nil {
		return nil, err
	}
	return &Inserter{
		db:       db,
		dbType:   dbType,
		host:     host,
		port:     port,
		user:     user,
		password: password,
		database: database,
	}, nil
}

func (ins *Inserter) Close() {
	ins.db.Close()
}

func (ins *Inserter) Run(method, csvFile, tableName string, batchSize int) (int, float64, error) {
	start := time.Now()

	var rows int
	var err  error

	switch method {
	case "default_insert":
		rows, err = ins.defaultInsert(csvFile, tableName)
	case "bulk_insert":
		rows, err = ins.bulkInsert(csvFile, tableName, batchSize)
	case "file_insert":
		rows, err = ins.fileInsert(csvFile, tableName)
	default:
		return 0, 0, fmt.Errorf("unknown method: %s", method)
	}

	elapsed := time.Since(start).Seconds()
	return rows, elapsed, err
}

// ─── default_insert ───────────────────────────────────────────────────────────

func (ins *Inserter) defaultInsert(csvFile, tableName string) (int, error) {
	data, err := readCSV(csvFile)
	if err != nil {
		return 0, err
	}

	query := buildInsertSQL(ins.dbType, tableName, data.Headers)

	tx, err := ins.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("prepare stmt: %w", err)
	}
	defer stmt.Close()

	for _, row := range data.Rows {
		if _, err := stmt.Exec(rowToArgs(row)...); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("exec row: %w", err)
		}
	}

	return len(data.Rows), tx.Commit()
}

// ─── bulk_insert ──────────────────────────────────────────────────────────────

func (ins *Inserter) bulkInsert(csvFile, tableName string, batchSize int) (int, error) {
	data, err := readCSV(csvFile)
	if err != nil {
		return 0, err
	}

	cols := make([]string, len(data.Headers))
	for i, h := range data.Headers {
		cols[i] = quoteIdentifier(ins.dbType, h)
	}
	colStr := strings.Join(cols, ", ")
	table  := quoteIdentifier(ins.dbType, tableName)
	total  := 0

	for i := 0; i < len(data.Rows); i += batchSize {
		end := i + batchSize
		if end > len(data.Rows) {
			end = len(data.Rows)
		}
		batch := data.Rows[i:end]

		valueStrings := make([]string, len(batch))
		valueArgs    := make([]interface{}, 0, len(batch)*len(data.Headers))

		for j, row := range batch {
			phs := make([]string, len(data.Headers))
			for k := range data.Headers {
				phs[k] = placeholder(ins.dbType, j*len(data.Headers)+k)
			}
			valueStrings[j] = "(" + strings.Join(phs, ", ") + ")"
			valueArgs = append(valueArgs, rowToArgs(row)...)
		}

		query := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s",
			table, colStr, strings.Join(valueStrings, ", "),
		)

		tx, err := ins.db.Begin()
		if err != nil {
			return total, fmt.Errorf("begin tx: %w", err)
		}
		if _, err := tx.Exec(query, valueArgs...); err != nil {
			tx.Rollback()
			return total, fmt.Errorf("exec batch: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return total, fmt.Errorf("commit: %w", err)
		}
		total += len(batch)
	}

	return total, nil
}

// ─── file_insert ──────────────────────────────────────────────────────────────

func (ins *Inserter) fileInsert(csvFile, tableName string) (int, error) {
	switch ins.dbType {
	case "mysql":
		return ins.fileInsertMySQL(csvFile, tableName)
	case "postgresql":
		return ins.fileInsertPg(csvFile, tableName)
	default:
		return 0, fmt.Errorf("file_insert not supported for %s", ins.dbType)
	}
}

// fileInsertMySQL использует LOAD DATA LOCAL INFILE
func (ins *Inserter) fileInsertMySQL(csvFile, tableName string) (int, error) {
	// Считаем строки заранее — LOAD DATA не возвращает точный rowcount
	data, err := readCSV(csvFile)
	if err != nil {
		return 0, err
	}
	rowCount := len(data.Rows)

	// Регистрируем файл для LOCAL INFILE
	absPath, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	if !strings.HasPrefix(csvFile, "/") {
		csvFile = absPath + "/" + csvFile
	}
	mysql.RegisterLocalFile(csvFile)

	table := quoteIdentifier("mysql", tableName)
	query := fmt.Sprintf(`
		LOAD DATA LOCAL INFILE '%s'
		INTO TABLE %s
		FIELDS TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		LINES TERMINATED BY '\n'
		IGNORE 1 ROWS
	`, csvFile, table)

	if _, err := ins.db.Exec(query); err != nil {
		return 0, fmt.Errorf("load data infile: %w", err)
	}

	return rowCount, nil
}

// fileInsertPg использует COPY FROM STDIN через pgx — самый быстрый метод для PostgreSQL
func (ins *Inserter) fileInsertPg(csvFile, tableName string) (int, error) {
	data, err := readCSV(csvFile)
	if err != nil {
		return 0, err
	}

	// Открываем прямое pgx соединение (не через database/sql) для COPY
	ctx  := context.Background()
	dsn  := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		ins.host, ins.port, ins.user, ins.password, ins.database,
	)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("pgx connect: %w", err)
	}
	defer conn.Close(ctx)

	// Формируем CSV в памяти с очищенными заголовками
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	cleanHeaders := make([]string, len(data.Headers))
	for i, h := range data.Headers {
		cleanHeaders[i] = cleanIdentifier(h)
	}
	writer.Write(cleanHeaders)
	writer.WriteAll(data.Rows)
	writer.Flush()

	table := fmt.Sprintf(`"%s"`, strings.ReplaceAll(tableName, `"`, `""`))
	copySQL := fmt.Sprintf(
		"COPY %s FROM STDIN WITH (FORMAT csv, HEADER true)",
		table,
	)

	// PgConn().CopyFrom — стримит данные напрямую без SQL-парсинга
	tag, err := conn.PgConn().CopyFrom(ctx, &buf, copySQL)
	if err != nil {
		return 0, fmt.Errorf("copy from stdin: %w", err)
	}

	return int(tag.RowsAffected()), nil
}