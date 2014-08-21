package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/sethgrid/dapi/db/mysql"
	"github.com/sethgrid/dapi/rest"
)

var dbName, host, port, password, user, raw string

func init() {
	flag.StringVar(&dbName, "db_name", "test_db", "Mysql Database Name")
	flag.StringVar(&host, "db_host", "localhost", "Mysql Database Host")
	flag.StringVar(&port, "db_port", "3306", "Mysql Database Port, usually 3306")
	flag.StringVar(&password, "db_pw", "", "Mysql Database Password")
	flag.StringVar(&user, "db_user", "", "Mysql Database Username")
	flag.StringVar(&raw, "db_datasource", "", "Mysql Database Resource, overrides other settings: username:password@protocol(address)/dbname")
}

func main() {
	flag.Parse()
	log.Println("Attempting to connect to DB...")

	mysql := &mysql.DataSource{DBName: dbName, Host: host, Port: port, Password: password, User: user, Raw: raw}
	database, err := mysql.DB()
	if err != nil {
		log.Fatalf("unable to connect to database", err)
	}

	// container object to expose the db and the tables at endpoints
	myApid := &rest.Apid{DataSource: mysql, Context: database}

	// routing. how would we add custom endpoints from here?
	router := myApid.NewRouter()

	myApid.Router = router

	// could wrap this and use a recover() to prevent
	// panics from taking down the server?
	http.ListenAndServe(":9000", myApid)
}
