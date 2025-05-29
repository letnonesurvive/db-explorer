package main

import (
	"database/sql"
	"fmt"
	"net/http"
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

func (exp *DbExplorer) listFunc(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// limit := r.FormValue("limit")
		// fmt.Println(limit)
		if r.URL.Path == "/" {
			rows, err := exp.db.Query("SHOW DATABASES")
			defer rows.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			for rows.Next() {
				var name string
				rows.Scan(&name)
				fmt.Println(name)
			}
		}
	case "PUT":
	case "POST":
	case "DELETE":

	}

}
