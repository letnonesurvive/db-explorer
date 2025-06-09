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

func SendResponse(w http.ResponseWriter, data any) {
	//w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-type", "application/json")
	response := make(map[string]interface{})
	response["response"] = data
	json.NewEncoder(w).Encode(response)
}

func (exp *DbExplorer) AllTables(w http.ResponseWriter, r *http.Request) {
	data, err := getTables(exp.db)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}
	SendResponse(w, data)
}

func (exp *DbExplorer) printAllRecords(databaseName, tableName string) {
	query := fmt.Sprintf("SELECT * FROM %s.%s;", databaseName, tableName)
	rows, _ := exp.db.Query(query)
	records, _ := prepareResponceData(rows)
	fmt.Println(records)
}

func (exp *DbExplorer) List(w http.ResponseWriter, r *http.Request, tableName string) {
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
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}
	defer rows.Close()
	records, err := prepareResponceData(rows) // невалидный id нужно обработать
	if err != nil {
		HandleError(w, err)
		return
	}
	data := make(map[string]interface{})
	data["records"] = records
	SendResponse(w, data)
}

func (exp *DbExplorer) RecordById(w http.ResponseWriter, r *http.Request, tableName string, id int) {
	databaseName, err := findDatabase(tableName, exp.db)
	if err != nil || len(databaseName) == 0 {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found such table")})
		return
	}
	primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found primary key")})
		return
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)
	rows, err := exp.db.Query(query, id)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: err})
		return
	}
	defer rows.Close()
	records, err := prepareResponceData(rows) // невалидный id нужно обработать
	if err != nil {
		HandleError(w, err)
		return
	}
	data := make(map[string]interface{})
	data["record"] = records[0]
	SendResponse(w, data)
}

func (exp *DbExplorer) CreateRecord(w http.ResponseWriter, r *http.Request, tableName string) {
	databaseName, err := findDatabase(tableName, exp.db)
	if err != nil || len(databaseName) == 0 {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found such table")})
		return
	}
	body := make(map[string]interface{}, 0)
	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}

	primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found primary key")})
		return
	}
	isKeyAutoIncrement := isIdAutoIncrement(exp.db, primaryKey, databaseName, tableName)

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
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}
	if id == 0 { // not auto increment
		var ok bool
		var rawId float64
		if rawId, ok = body[primaryKey].(float64); !ok {
			HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found primary key")})
			return
		}
		id = int64(rawId)
	}
	data := make(map[string]int64, 1)
	data[primaryKey] = id
	SendResponse(w, data)
}

func (exp *DbExplorer) UpdateRecord(w http.ResponseWriter, r *http.Request, tableName string, id int) {
	databaseName, err := findDatabase(tableName, exp.db)
	if err != nil || len(databaseName) == 0 {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found such table")})
		return
	}
	body := make(map[string]interface{}, 0)
	err = json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}

	primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found such table")})
		return
	}

	keys := make([]string, 0)
	values := make([]interface{}, 0)
	for key, val := range body {
		keys = append(keys, key)
		values = append(values, val)
	}
	values = append(values, id)
	setValue := ""
	for i := 0; i < len(keys); i++ {
		setValue += keys[i] + " = ?"
		if i != len(keys)-1 {
			setValue += ","
		}
	}

	if Contains(keys, primaryKey) && IsRecordExist(exp.db, databaseName, tableName, primaryKey, id) {
		HandleError(w, DbError{statusCode: http.StatusBadRequest, err: errors.New("field id have invalid type")})
		return
	}

	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s = ?", databaseName, tableName, setValue, primaryKey)
	result, err := exp.db.Exec(query, values...)
	if err != nil {
		HandleError(w, err)
		return
	}

	fmt.Println(result.LastInsertId())
	data := make(map[string]int64, 1)
	data["updated"] = 1
	SendResponse(w, data)
}

func (exp *DbExplorer) Delete(w http.ResponseWriter, r *http.Request, tableName string, id int) {
	databaseName, err := findDatabase(tableName, exp.db)
	if err != nil || len(databaseName) == 0 {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found such table")})
		return
	}
	primaryKey, err := getPrimaryKey(exp.db, databaseName, tableName)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("not found primary key")})
		return
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s = ?;", databaseName, tableName, primaryKey)
	result, err := exp.db.Exec(query, id)
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}
	affected, err := result.RowsAffected()
	if err != nil {
		HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
		return
	}
	data := make(map[string]int64, 1)
	data["deleted"] = affected
	SendResponse(w, data)
}

func (exp *DbExplorer) handleGET(w http.ResponseWriter, r *http.Request, segments []string) {
	if r.URL.Path == "/" {
		exp.AllTables(w, r)
	} else {
		tableName := segments[0]
		switch len(segments) {
		case 1:
			exp.List(w, r, tableName)
		case 2:
			id, err := strconv.Atoi(segments[1])
			if err != nil {
				HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
				return
			}
			exp.RecordById(w, r, tableName, id)
		default:
			HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("unknown method")})
		}
	}
}

func (exp *DbExplorer) handlePUT(w http.ResponseWriter, r *http.Request, segments []string) {
	tableName := segments[0]
	switch len(segments) {
	case 1:
		exp.CreateRecord(w, r, tableName)
	default:
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("unknown method")})
	}
}

func (exp *DbExplorer) handlePOST(w http.ResponseWriter, r *http.Request, segments []string) {
	tableName := segments[0]
	switch len(segments) {
	case 2:
		id, err := strconv.Atoi(segments[1])
		if err != nil {
			HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
			return
		}
		exp.UpdateRecord(w, r, tableName, id)
	default:
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("unknown method")})
	}
}

func (exp *DbExplorer) handleDELETE(w http.ResponseWriter, r *http.Request, segments []string) {
	tableName := segments[0]
	switch len(segments) {
	case 2:
		id, err := strconv.Atoi(segments[1])
		if err != nil {
			HandleError(w, DbError{statusCode: http.StatusInternalServerError, err: err})
			return
		}
		exp.Delete(w, r, tableName, id)
	default:
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("unknown method")})
	}
}

func (exp *DbExplorer) listFunc(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")

	// вариант использолвать  router map[string]func(http.ResponseWriter, *http.Request)
	// и инициализировать маршруты по следующему виду exp.router["/items/{id}"] = exp.GetItemById
	// затем каждый входящий url приводить к виду, который лежит в map
	switch r.Method {
	case "GET":
		exp.handleGET(w, r, segments)
	case "PUT":
		exp.handlePUT(w, r, segments)
	case "POST":
		exp.handlePOST(w, r, segments)
	case "DELETE":
		exp.handleDELETE(w, r, segments)
	default:
		HandleError(w, DbError{statusCode: http.StatusNotFound, err: errors.New("unknown method")})
	}

}
