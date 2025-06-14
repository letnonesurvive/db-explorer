package main

import (
	"encoding/json"
	"io"
	"net/http"
)

func convertNumber(num json.Number) interface{} {
	if i, err := num.Int64(); err == nil {
		return i // сохранить как int64
	} else if f, err := num.Float64(); err == nil {
		return f
	}
	return nil
}

func convertNumbers(src map[string]interface{}) map[string]interface{} {

	res := make(map[string]interface{}) // можно соптимизировать, приводить к типу in-place без создания новой мапы
	for k, v := range src {
		if num, ok := v.(json.Number); ok {
			res[k] = convertNumber(num)
		} else {
			res[k] = v
		}
	}

	return res
}

func jsonBodyParser(Body io.ReadCloser) (map[string]interface{}, error) {

	decoder := json.NewDecoder(Body)
	decoder.UseNumber()

	data := make(map[string]interface{})
	err := decoder.Decode(&data)

	if err != nil {
		return nil, DbError{statusCode: http.StatusInternalServerError, err: err}
	}

	_, err = io.Copy(io.Discard, decoder.Buffered())
	if err != nil {
		return nil, DbError{statusCode: http.StatusInternalServerError, err: err}
	}

	res := convertNumbers(data)

	return res, nil
}
