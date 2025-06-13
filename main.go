// тут лежит тестовый код
// менять вам может потребоваться только коннект к базе
package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

var (
	// DSN это соединение с базой
	// вы можете изменить этот на тот который вам нужен
	// sudo docker run -p 3307:3306 --name db -v ${PWD}:/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
	// docker start db
	// DSN = "root@tcp(localhost:3306)/golang2017?charset=utf8"

	// sudo docker exec -it db bash
	// mysql -u root -p
	DSN = "root:1234@tcp(localhost:3307)/golang?charset=utf8"
)

func main() {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err)
	}
	err = db.Ping() // вот тут будет первое подключение к базе
	if err != nil {
		panic(err)
	}
	defer db.Close()

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}
