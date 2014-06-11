package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
)

const (
	URI_APID_CRUD = "/api/v1/crud/"
)

func OpenDB() *sql.DB {
	db, err := sql.Open("mysql", "/test_db")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func main() {
	log.Println("started")
	DB := OpenDB()
	tables := GetTables(DB)
	apid := &Apid{Tables: tables}

	router := httprouter.New()
	router.GET("/", rootHandler)
	router.GET("/favicon.ico", nullHandler)
	//router.GET("/api/v1/crud/_meta", apid.MetaHandler)
	router.GET("/api/v1/crud/:table/_meta", apid.TableMetaHandler)
	router.GET("/api/v1/crud/:table", apid.TableHandler)

	router.RedirectTrailingSlash = true

	http.ListenAndServe(":9000", router)

}

func nullHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {}

func rootHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Write([]byte("Root"))
	log.Print("index handler")
}

type Apid struct {
	Tables map[string]*Table
}

func (a *Apid) TableHandler(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	if t.ByName("table") == "_meta" {
		a.MetaHandler(w, r, nil)
		return
	}
	w.Write([]byte("table"))

	log.Printf("params: %v ", t)
	table := t.ByName("table")
	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ", table)
	} else {
		log.Println("No table found ", table)
	}
	log.Print("tableHandler")
}

func (a *Apid) TableMetaHandler(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("table meta"))
	table := t.ByName("table")
	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ", table)
	} else {
		log.Println("No table found ", table)
	}
	log.Print("TableMetaHandler")
}

func (a *Apid) MetaHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Write([]byte("meta"))

	log.Print("metaHandler")
}

type Table struct {
	Name string
	Cols []*TableSchema
}

type TableSchema struct {
	TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT sql.NullString
}

func GetTables(db *sql.DB) map[string]*Table {
	allTables := make(map[string]*Table)

	// could also get TABLE_SCHEMA if it is important for future use
	r, err := db.Query("select TABLE_NAME from information_schema.tables where table_type=\"BASE TABLE\"")
	if err != nil {
		log.Fatal("unable to reach information schema ", err)
	}

	tables := make([]string, 0)
	for r.Next() {
		var name string
		err = r.Scan(&name)
		if err != nil {
			log.Print("error scanning schema ", err)
		}
		tables = append(tables, name)
	}

	for _, t := range tables {
		nextTable := &Table{Name: t}
		allTables[t] = nextTable
		r, err := db.Query(
			fmt.Sprintf(
				"select * from information_schema.columns where "+
					"table_name=\"%s\"", t))

		if err != nil {
			log.Fatal("unable to query table ", t, err)
		}

		for r.Next() {
			var TABLE_CATALOG sql.NullString
			var TABLE_SCHEMA sql.NullString
			var TABLE_NAME sql.NullString
			var COLUMN_NAME sql.NullString
			var ORDINAL_POSITION sql.NullString
			var COLUMN_DEFAULT sql.NullString
			var IS_NULLABLE sql.NullString
			var DATA_TYPE sql.NullString
			var CHARACTER_MAXIMUM_LENGTH sql.NullString
			var CHARACTER_OCTET_LENGTH sql.NullString
			var NUMERIC_PRECISION sql.NullString
			var NUMERIC_SCALE sql.NullString
			var DATETIME_PRECISION sql.NullString
			var CHARACTER_SET_NAME sql.NullString
			var COLLATION_NAME sql.NullString
			var COLUMN_TYPE sql.NullString
			var COLUMN_KEY sql.NullString
			var EXTRA sql.NullString
			var PRIVILEGES sql.NullString
			var COLUMN_COMMENT sql.NullString

			err = r.Scan(
				&TABLE_CATALOG,
				&TABLE_SCHEMA,
				&TABLE_NAME,
				&COLUMN_NAME,
				&ORDINAL_POSITION,
				&COLUMN_DEFAULT,
				&IS_NULLABLE,
				&DATA_TYPE,
				&CHARACTER_MAXIMUM_LENGTH,
				&CHARACTER_OCTET_LENGTH,
				&NUMERIC_PRECISION,
				&NUMERIC_SCALE,
				&DATETIME_PRECISION,
				&CHARACTER_SET_NAME,
				&COLLATION_NAME,
				&COLUMN_TYPE,
				&COLUMN_KEY,
				&EXTRA,
				&PRIVILEGES,
				&COLUMN_COMMENT)
			if err != nil {
				log.Print("error scanning column schema ", err)
			}
			info := &TableSchema{TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT}
			allTables[t].Cols = append(allTables[t].Cols, info)
		}
	}
	return allTables
}
