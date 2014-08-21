package db

// TODO: add test tag for integration (no need to spin up a db for each test)

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// open question: why do I need to define a closer on the tests stuct to access the body? Seems like httpTest should provide this.
type Test struct {
	method          string
	url             string
	reqBody         ReqBody
	resBodyContains string
	code            int
}

type ReqBody struct {
	contents string
}

func (r ReqBody) Close() error {
	return nil
}

func (r ReqBody) Read(p []byte) (n int, err error) {
	// how is this supposed to work? p needs to be a pointer...
	b := make([]byte, 0)
	b = []byte(r.contents)

	copy(p, b)
	p = bytes.Trim(p, "\x00")
	return len(p), io.EOF
}

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
	var tests = []Test{
		{"GET", "/api/v1/crud/user/_meta", ReqBody{}, "MySQL Table user", 200},
		{"GET", "/api/v1/crud/settings/_meta", ReqBody{}, "MySQL Table settings", 200},
		{"GET", "/api/v1/crud/unknown_table/_meta", ReqBody{}, "No table", 404},
		{"POST", "/api/v1/crud/user/", ReqBody{}, "", 307}, // trailing slash redirects
		{"POST", "/api/v1/crud/user", ReqBody{`{"name":"jack","email":"jack@example.com"}`}, "inserted_id", 200},
		{"GET", "/api/v1/crud/user", ReqBody{}, "jack", 200},
		{"GET", "/api/v1/crud/user?name=jack", ReqBody{}, "jack@example.com", 200},
		{"POST", "/api/v1/crud/user", ReqBody{`{"id":26,"name":"jack","email":"jack-n-jill@example.com"}`}, "Duplicate", 404},
		{"PUT", "/api/v1/crud/user", ReqBody{`{"id":26,"name":"jack","email":"jack-n-jill@example.com"}`}, "success", 200},
		{"DELETE", "/api/v1/crud/user", ReqBody{`{"id":26,"limit":1}`}, "rows_affected\":1", 200}, // not sure why id 26 is first yet
	}

	// test runner
	for _, test := range tests {
		req, _ := http.NewRequest(test.method, test.url, test.reqBody)
		req.RequestURI = test.url // http.NewRequest does not set the RequestURI

		rw := httptest.NewRecorder()
		rw.Body = new(bytes.Buffer)

		router.ServeHTTP(rw, req)

		// got, want
		if g, w := rw.Code, test.code; g != w {
			t.Errorf("%s %s - Actual status (%d) not equal expected status (%d)", test.method, test.url, g, w)
		}
		if g, w := rw.Body.String(), test.resBodyContains; !strings.Contains(g, w) {
			t.Errorf("%s %s - Response Body Error, actual does not contain expected (\"%s\")\n\n%s\n\n", test.method, test.url, w, g)
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
