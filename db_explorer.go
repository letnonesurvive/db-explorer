package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

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
		return databaseName, err
	}
	for database, tables := range databases {
		if _, ok := tables[tableName]; ok {
			databaseName = database
			break
		}
	}
	return databaseName, nil
}

func getPrimaryKeyName(db *sql.DB, databaseName, tableName string) (string, error) {
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

func (exp *DbExplorer) listFunc(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")

	switch r.Method {
	case "GET":
		if r.URL.Path == "/" {
			database, err := getDatabases(exp.db)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			names := ""
			for databaseName, tables := range database {
				for table := range tables {
					names += databaseName + " " + table + "\n"
				}
			}
			w.Write([]byte(names))
		} else {
			switch len(segments) {
			case 1:
				tableName := segments[0]
				databaseName, err := findDatabase(tableName, exp.db)
				if err != nil || len(databaseName) == 0 {
					http.Error(w, "Not found such table", http.StatusNotFound)
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
				columns, err := rows.Columns()
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				values := make([]interface{}, len(columns))
				for i := range values { // написать объяснение что это
					var tmp interface{}
					values[i] = &tmp
				}
				//encoder.SetEscapeHTML(false) // Отключает \u0026
				for rows.Next() {
					rows.Scan(values...) // ожидает ровно столько аргументов, сколько колонок в таблице.
					data := make(map[string]interface{}, 0)
					for i := 0; i < len(columns); i++ {
						v := *(values[i].(*interface{}))
						if b, ok := v.([]byte); ok {
							data[columns[i]] = string(b)
						} else {
							data[columns[i]] = v
						}
					}
					fmt.Println(data)
					json.NewEncoder(w).Encode(data)
				}
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
				primaryKey, err := getPrimaryKeyName(exp.db, databaseName, tableName)
				if err != nil {
					http.Error(w, "Not found primary key", http.StatusNotFound)
					return
				}
				query := fmt.Sprintf("SELECT * FROM %s.%s WHERE ? = ? LIMIT 1;", databaseName, tableName)
				rows, err := exp.db.Query(query, primaryKey, id)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				w.Header().Set("Content-type", "application/json")
				columns, err := rows.Columns()
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				values := make([]interface{}, len(columns))
				for i := range values { // написать объяснение что это
					var tmp interface{}
					values[i] = &tmp
				}
				//encoder.SetEscapeHTML(false) // Отключает \u0026
				//for rows.Next() {
				rows.Scan(values...) // ожидает ровно столько аргументов, сколько колонок в таблице.
				data := make(map[string]interface{}, 0)
				for i := 0; i < len(columns); i++ {
					v := *(values[i].(*interface{}))
					if b, ok := v.([]byte); ok {
						data[columns[i]] = string(b)
					} else {
						data[columns[i]] = v
					}
				}
				fmt.Println(data)
				json.NewEncoder(w).Encode(data)
				//}
			}
		}
	case "PUT":
	case "POST":
	case "DELETE":
	}

}
