package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type DbExplorer struct {
	db     *sql.DB
	router *http.ServeMux
}

func (exp *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	exp.router.ServeHTTP(w, r)
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	exp := &DbExplorer{db: db, router: http.NewServeMux()}

	exp.router.HandleFunc("/", exp.listFunc)

	return exp, nil
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

func IsIdAutoIncrement(db *sql.DB, id string, databaseName, tableName string) bool {
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

func SendResponse(w http.ResponseWriter, data any) {
	response := make(map[string]interface{})
	response["response"] = data
	json.NewEncoder(w).Encode(response)
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

func (exp *DbExplorer) listFunc(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")

	switch r.Method {
	case "GET":
		if r.URL.Path == "/" {
			w.Header().Set("Content-type", "application/json")
			data, err := getTables(exp.db)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			SendResponse(w, data)
		} else {
			switch len(segments) {
			case 1:
				tableName := segments[0]
				databaseName, err := findDatabase(tableName, exp.db)
				if err != nil {
					HandleError(w, err)
					return
				}
				limit := retrieveParam(r.FormValue("limit"), 5)
				offset := retrieveParam(r.FormValue("offset"), 0)

				query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT ? OFFSET ?", databaseName, tableName)
				rows, err := exp.db.Query(query, limit, offset)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				w.Header().Set("Content-type", "application/json")
				records, err := prepareResponceData(rows) // невалидный id нужно обработать
				if err != nil {
					HandleError(w, err)
					return
				}
				data := make(map[string]interface{})
				data["records"] = records
				SendResponse(w, data)
			case 2:
				tableName := segments[0]
				id, err := strconv.Atoi(segments[1])
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				databaseName, err := findDatabase(tableName, exp.db)
				if err != nil || len(databaseName) == 0 {
					http.Error(w, "Not found such table", http.StatusNotFound)
					return
				}
				primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
				if err != nil {
					http.Error(w, "Not found primary key", http.StatusNotFound)
					return
				}
				query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)
				rows, err := exp.db.Query(query, id)
				if err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				defer rows.Close()
				w.Header().Set("Content-type", "application/json")
				records, err := prepareResponceData(rows) // невалидный id нужно обработать
				if err != nil {
					HandleError(w, err)
					return
				}
				data := make(map[string]interface{})
				data["record"] = records[0]
				SendResponse(w, data)
			}
		}
	case "PUT":
		switch len(segments) {
		case 1:
			tableName := segments[0]
			databaseName, err := findDatabase(tableName, exp.db)
			if err != nil || len(databaseName) == 0 {
				http.Error(w, "Not found such table", http.StatusNotFound)
				return
			}
			body := make(map[string]interface{}, 0)
			err = json.NewDecoder(r.Body).Decode(&body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
			if err != nil {
				http.Error(w, "Not found primary key", http.StatusNotFound)
				return
			}
			isKeyAutoIncrement := IsIdAutoIncrement(exp.db, primaryKey, databaseName, tableName)

			keys := make([]string, 0)
			values := make([]interface{}, 0)
			for key, val := range body {
				if isKeyAutoIncrement && key == primaryKey {
					continue
				}
				keys = append(keys, key)
				values = append(values, val)
			}

			questionMark := strings.Repeat("?, ", len(values)-1)
			questionMark += "?"
			query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", databaseName, tableName, strings.Join(keys, ","), questionMark)
			result, err := exp.db.Exec(query, values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			id, err := result.LastInsertId()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if id == 0 { // not auto increment
				var ok bool
				var rawId float64
				if rawId, ok = body[primaryKey].(float64); !ok {
					http.Error(w, "Not found primary key", http.StatusNotFound)
					return
				}
				id = int64(rawId)
			}
			data := make(map[string]int64, 1)
			data[primaryKey] = id
			SendResponse(w, data)
		}
	case "POST":
	case "DELETE":
		switch len(segments) {
		case 2:
			tableName := segments[0]
			id, err := strconv.Atoi(segments[1])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			databaseName, err := findDatabase(tableName, exp.db)
			if err != nil || len(databaseName) == 0 {
				http.Error(w, "Not found such table", http.StatusNotFound)
				return
			}
			primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
			if err != nil {
				http.Error(w, "Not found primary key", http.StatusNotFound)
				return
			}
			query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)
			result, err := exp.db.Exec(query, id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			affected, err := result.RowsAffected()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			data := make(map[string]int64, 1)
			data["deleted"] = affected
			json.NewEncoder(w).Encode(data)
		}
	}

}
