package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

type DbError struct {
	statusCode int
	err        error
}

func (e DbError) Error() string {
	return e.err.Error()
}

func (e DbError) Status() int {
	return e.statusCode
}

func Contains[T comparable](slice []T, value T) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func retrieveParam(paramStr string, defaultValue int) int {
	var res int
	if paramStr == "" {
		res = defaultValue
	} else {
		var err error
		res, err = strconv.Atoi(paramStr)
		if err != nil {
			res = defaultValue
		}
	}
	return res
}

func getTables(db *sql.DB) (map[string][]string, error) {

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	res := make(map[string][]string)
	defer rows.Close()
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tables := res["tables"]
		tables = append(tables, tableName)
		res["tables"] = tables
	}
	return res, nil
}

// лучше хранить в каком нибудь кэше или неумираемой переменной.
func getDatabases(db *sql.DB) (map[string]map[string]struct{}, error) {
	// получить список таблиц из *всех баз данных*
	rows, err := db.Query(
		"SELECT TABLE_SCHEMA, TABLE_NAME " +
			"FROM information_schema.tables " +
			"WHERE TABLE_TYPE = 'BASE TABLE';")

	if err != nil {
		return nil, err
	}
	res := make(map[string]map[string]struct{}, 0)
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName string
		rows.Scan(&tableSchema, &tableName)
		tables := res[tableSchema]
		if tables == nil {
			tables = make(map[string]struct{})
		}
		tables[tableName] = struct{}{}
		res[tableSchema] = tables
	}
	return res, nil
}

func findDatabase(tableName string, db *sql.DB) (string, error) {
	var databaseName string

	databases, err := getDatabases(db)
	if err != nil {
		return "", err
	}
	for database, tables := range databases {
		if _, ok := tables[tableName]; ok {
			databaseName = database
			break
		}
	}
	if len(databaseName) == 0 {
		err := DbError{err: errors.New("unknown table"), statusCode: http.StatusNotFound}
		return "", err
	}
	return databaseName, nil
}

func getPrimaryKey(db *sql.DB, databaseName, tableName string) (string, error) {
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 1", databaseName, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	return columns[0], nil
}

func isIdAutoIncrement(db *sql.DB, id string, databaseName, tableName string) bool {
	query := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA LIKE ?"
	row := db.QueryRow(query, databaseName, tableName, "%auto_increment%")
	err := row.Err()
	if err != nil || row == nil {
		return false
	}
	var v string
	row.Scan(&v)
	return v == id
}

func HandleError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-type", "application/json")
	if e, ok := err.(DbError); ok {
		w.WriteHeader(e.statusCode)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}

	json.NewEncoder(w).Encode(map[string]string{
		"error": err.Error(),
	})
}

func IsRecordExist(db *sql.DB, databaseName, tableName, primaryKey string, id int) bool {
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)
	row := db.QueryRow(query, id)
	if row != nil {
		return true
	}
	return false
}

func prepareResponceData(rows *sql.Rows) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(columns))
	for i := range values { // написать объяснение что это
		var tmp interface{}
		values[i] = &tmp
	}
	for rows.Next() {
		err = rows.Scan(values...) // ожидает ровно столько аргументов, сколько колонок в таблице.
		if err != nil {
			return nil, err
		}
		data := make(map[string]interface{}, 0)
		for i := 0; i < len(columns); i++ {
			v := *(values[i].(*interface{}))
			if b, ok := v.([]byte); ok {
				data[columns[i]] = string(b)
			} else {
				data[columns[i]] = v
			}
		}
		res = append(res, data)
	}
	if len(res) == 0 {
		err := DbError{err: errors.New("record not found"), statusCode: http.StatusNotFound}
		return nil, err
	}
	return res, nil
}
