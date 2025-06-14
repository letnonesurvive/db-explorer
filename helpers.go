package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
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
	defer rows.Close()
	res := make(map[string][]string)
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
			"WHERE TABLE_TYPE = 'BASE TABLE'" +
			"AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')")

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	databases := make(map[string]map[string]struct{}, 0)
	for rows.Next() {
		var tableSchema, tableName string
		err := rows.Scan(&tableSchema, &tableName)
		if err != nil {
			rows.Close()
			return nil, err
		}
		tables := databases[tableSchema]
		if tables == nil {
			tables = make(map[string]struct{})
		}
		tables[tableName] = struct{}{}
		databases[tableSchema] = tables
	}
	if err = rows.Err(); err != nil {
		return databases, err
	}
	return databases, nil
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
	//return "golang", nil
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

func isPrimaryKey(db *sql.DB, databaseName, tableName string, key string) bool {
	primaryKey, err := getPrimaryKey(db, databaseName, tableName)
	if err != nil || key != primaryKey {
		return false
	}
	return true
}

type TypeInfo struct {
	Type       reflect.Type
	IsNullable bool
}

func getReference(db *sql.DB, databaseName, tableName string) (map[string]TypeInfo, error) {
	query := "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;"
	rows, err := db.Query(query, databaseName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	types := make(map[string]TypeInfo) // информация о типах взятая из БД
	for rows.Next() {
		var Field, Type string
		var IsNullable string
		if err := rows.Scan(&Field, &Type, &IsNullable); err != nil {
			return nil, err
		}
		types[Field] = TypeInfo{toGoNativeType(Type), IsNullable == "YES"}
	}
	return types, nil
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

func printAllRecords(db *sql.DB, databaseName, tableName string) {
	query := fmt.Sprintf("SELECT * FROM %s.%s;", databaseName, tableName)
	rows, _ := db.Query(query)
	defer rows.Close()
	records, _ := Pack(rows)
	fmt.Println(records)
}

func IsRecordExist(db *sql.DB, databaseName, tableName, primaryKey string, id int) bool {
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)

	row := db.QueryRow(query, id)
	if row != nil {
		return true
	}
	return false
}

func Pack(rows *sql.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
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

func toGoNativeType(Type string) reflect.Type {
	if Type == "varchar" || Type == "text" {
		return reflect.TypeOf("")
	} else if Type == "int" {
		return reflect.TypeOf(int64(0))
	}
	return nil
}
