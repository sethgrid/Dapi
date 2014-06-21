package main

import (
	"flag"
	"log"
	"net/http"

	"vendored/apid"
)

var dbName, host, port, password, user, raw string

func init() {
	flag.StringVar(&dbName, "db_name", "test_db", "Mysql Database Name")
	flag.StringVar(&host, "db_host", "", "Mysql Database Host")
	flag.StringVar(&port, "db_port", "", "Mysql Database Port, usually 3306")
	flag.StringVar(&password, "db_pw", "", "Mysql Database Password")
	flag.StringVar(&user, "db_user", "", "Mysql Database Username")
	flag.StringVar(&raw, "db_datasource", "", "Mysql Database Resource, overrides other settings: username:password@protocol(address)/dbname")
}

func main() {
	flag.Parse()
	log.Println("Attempting to connect to DB...")

	conn := &apid.DataSourceName{DBName: dbName, Host: host, Port: port, Password: password, User: user, Raw: raw}
	DB := apid.OpenDB(conn)

	log.Print("Connected to " + conn.DBName)

	// grab all the table data. We can now easily remove any tables if we want
	// we will prolly want to use this differently. This should prolly be done
	// behind the scenes by having a NewApid() method
	tables := apid.GetTables(DB)

	// container object to expose the db and the tables at endpoints
	myApid := &apid.Apid{DB: DB, Tables: tables}

	// routing. how would we add custom endpoints from here?
	router := myApid.NewRouter()

	// could wrap this and use a recover() to prevent
	// panics from taking down the server?
	http.ListenAndServe(":9000", router)
}
