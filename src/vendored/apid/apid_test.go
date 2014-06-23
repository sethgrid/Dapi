package apid

// TODO: add test tag for integration (no need to spin up a db for each test)

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sets up a test db and tests endpoints
func TestServer(t *testing.T) {
	dbName := fmt.Sprintf("apid_test_db_%d", time.Now().Unix())

	// create the test db
	// we require a "hook" database that gives us access to mysql
	// after that, we set up a new database for each run and us it.
	// I tried to os.exec mysql to create the db, and it failed.
	// TODO: make this easily configurable
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/apid_integration_test")
	if err != nil {
		t.Errorf("Unable to open database '%s'. Please create it. %s", "apid_integration_test", err)
	}

	// create the test db and drop it when we are done
	setUp(dbName, db)
	defer func() {
		tearDown(dbName, db)
	}()

	// set up apid and routes
	tables := GetTables(db)
	apid := &Apid{DB: db, Tables: tables}
	router := apid.NewRouter()

	// test cases
	var tests = []struct {
		method, url, reqBody, resBodyContains string
		code                                  int
	}{
		{"GET", "/api/v1/crud/user/_meta", "", "MySQL Table user", 200},
		{"GET", "/api/v1/crud/settings/_meta", "", "MySQL Table settings", 200},
		{"GET", "/api/v1/crud/unknown_table/_meta", "", "No table", 404},
	}

	// test runner
	for _, test := range tests {
		req, _ := http.NewRequest(test.method, test.url, nil)
		req.RequestURI = test.url // http.NewRequest does not set the RequestURI
		rw := httptest.NewRecorder()
		rw.Body = new(bytes.Buffer)

		router.ServeHTTP(rw, req)

		if g, w := rw.Code, test.code; g != w {
			t.Errorf("Actual status (%d) not equal expected status (%d)", g, w)
		}
		if g, w := rw.Body.String(), test.resBodyContains; !strings.Contains(g, w) {
			t.Errorf("Response Body Error, actual does not contain expected (\"%s\")\n\n%s\n\n", w, g)
		}
	}
}

// set up a new database with user and settings tables
func setUp(dbName string, db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	if err != nil {
		return err
	}

	_, err = db.Exec("use " + dbName)
	_, err = db.Exec("CREATE TABLE `user` (" +
		"`name` varchar(20) DEFAULT NULL," +
		"`id` int(11) NOT NULL AUTO_INCREMENT," +
		"`email` varchar(255) DEFAULT NULL," +
		"PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8;")
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE TABLE `settings` (" +
		"`id` int(11) NOT NULL AUTO_INCREMENT," +
		"`user_id` int(11) DEFAULT NULL," +
		"`setting` varchar(255) DEFAULT NULL," +
		"`enabled` tinyint(1) DEFAULT NULL," +
		"PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		return err
	}

	return nil
}

// remove the database
func tearDown(dbName string, db *sql.DB) error {
	_, err := db.Exec("drop database " + dbName)
	return err
}
