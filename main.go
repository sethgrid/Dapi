package main

import (
	"log"
	"net/http"

	"vendored/apid"
)

func main() {
	log.Println("started")

	// currently, we only work on mysql dbs with no user, no password
	DB := apid.OpenDB()

	// grab all the table data. We can now easily remove any tables if we want
	tables := apid.GetTables(DB)

	// container object to expose the db and the tables at endpoints
	myApid := &apid.Apid{DB: DB, Tables: tables}

	// routing
	router := myApid.NewRouter()

	// could wrap this and use a recover() to prevent
	// panics from taking down the server?
	http.ListenAndServe(":9000", router)
}
