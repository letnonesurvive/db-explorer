package main

import (
	"database/sql"
	"fmt"
	"net/http"
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
				databases, err := getDatabases(exp.db)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				var isFoundTable bool
				var databaseName string
				for database, tables := range databases {
					if _, ok := tables[tableName]; ok {
						isFoundTable = true
						databaseName = database
						break
					}
				}
				if !isFoundTable {
					http.Error(w, "Not found such table", http.StatusNotFound)
					return
				}
				//limit := r.FormValue("limit")
				//offset := r.FormValue("offset")
				query := fmt.Sprintf("SELECT * FROM %s.%s;", databaseName, tableName)
				rows, err := exp.db.Query(query)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer rows.Close()

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
				for rows.Next() {
					rows.Scan(values...) // ожидает ровно столько аргументов, сколько колонок в таблице.
					fmt.Println(values...)
					for _, val := range values {
						v := *(val.(*interface{})) // тоже какая то странная конструкция
						fmt.Sprintf("%v\t", v)
					}
				}
				fmt.Println(values...)

				// w.Write([]byte(entries))
			case 2:
				fmt.Println("попали в case 2")
			}
		}
	case "PUT":
	case "POST":
	case "DELETE":
	}

}
