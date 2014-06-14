package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
)

func main() {
	log.Println("started")
	DB := OpenDB()
	tables := GetTables(DB)
	apid := &Apid{DB: DB, Tables: tables}

	router := httprouter.New()
	router.GET("/", rootHandler)
	router.GET("/favicon.ico", nullHandler)
	router.GET("/api/v1/crud/:table/_meta", apid.TableMetaHandler)

	router.GET("/api/v1/crud/:table", apid.GetTable)
	router.POST("/api/v1/crud/:table", apid.PostTable)
	router.PUT("/api/v1/crud/:table", apid.PutTable)
	router.DELETE("/api/v1/crud/:table", apid.DeleteTable)

	router.NotFound = NotFound
	router.RedirectTrailingSlash = true

	http.ListenAndServe(":9000", router)

}

/********************************
 *   APID Struct and Handlers   *
 ********************************/

type Apid struct {
	DB     *sql.DB
	Tables map[string]*Table
}

func nullHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {}

func rootHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("index handler")
	w.Write([]byte("Root. Available paths: /api/v1/crud/_meta, /api/v1/crud/:table, /api/v1/crud/:table/_meta"))
}

func NotFound(w http.ResponseWriter, r *http.Request) {
	log.Printf("404 - %s %s", r.Method, r.RequestURI)
	http.Error(w, "resource does not exist", http.StatusNotFound)
}

func NotFoundWithParams(w http.ResponseWriter, r *http.Request, e string) {
	log.Printf("404 - %s %s", r.Method, r.RequestURI)
	http.Error(w, e, http.StatusNotFound)
}

// GetTable forwards _meta requests onward. Otherwise, it checks
// for the existance of the table requested and returns all
// records
func (a *Apid) GetTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	// forward to MetaHandler
	if t.ByName("table") == "_meta" {
		log.Print("loading meta handler")
		a.MetaHandler(w, r, nil)
		return
	}

	// verify table name validity
	tableName := t.ByName("table")
	if _, ok := a.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}

	// query the table
	table := a.Tables[tableName]
	rows, err := a.DB.Query("select * from " + table.Name)

	if err != nil {
		log.Printf("Error querying GET on %s", table.Name)
		NotFoundWithParams(w, r, fmt.Sprintf("Table (%s) GET Failed", table))
	}

	// grab all the column names returned and prepare them
	// to receive data
	columnNames, err := rows.Columns()
	if err != nil {
		log.Fatalln(err) // or 500, whatever error handling is appropriate
	}
	columns := make([]interface{}, len(columnNames))
	columnPointers := make([]interface{}, len(columnNames))
	for i := 0; i < len(columnNames); i++ {
		columnPointers[i] = &columns[i]
	}

	// to become the json response object
	resp := make(map[string]interface{})
	responses := make([]map[string]interface{}, 0)

	// populate json object from rows
	for rows.Next() {
		if err := rows.Scan(columnPointers...); err != nil {
			log.Fatalln(err)
		}

		for i, data := range columns {
			// Here we could do some type checking to get
			// int, bool, etc. Defaulting always to string.
			resp[columnNames[i]] = string(data.([]byte))
		}
		responses = append(responses, resp)
	}

	j, err := json.Marshal(responses)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(j))

}

func (a *Apid) PostTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	log.Print("PostTable")

	table := t.ByName("table")
	w.Write([]byte(fmt.Sprintf("POST request for table %s ", table)))

	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ")
	} else {
		log.Println("No table found ")
		NotFoundWithParams(w, r, "")
		return
	}
}

func (a *Apid) PutTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	log.Print("PutTable")

	table := t.ByName("table")
	w.Write([]byte(fmt.Sprintf("PUT request for table %s ", table)))

	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ")
	} else {
		log.Println("No table found ")
		NotFoundWithParams(w, r, "")
		return
	}
}

func (a *Apid) DeleteTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	log.Print("DeleteTable")

	table := t.ByName("table")
	w.Write([]byte(fmt.Sprintf("DELETE request for table %s ", table)))

	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ")
	} else {
		log.Println("No table found ")
		NotFoundWithParams(w, r, "")
		return
	}
}

func (a *Apid) TableMetaHandler(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	log.Print("TableMetaHandler")

	table := t.ByName("table")
	w.Write([]byte(fmt.Sprintf("request for table %s meta data ", table)))
	if _, ok := a.Tables[table]; ok {
		log.Println("Table found! ")
	} else {
		log.Println("No table found ")
		NotFoundWithParams(w, r, "")
		return
	}
}

func (a *Apid) MetaHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("metaHandler")
	w.Write([]byte("meta for whole schema"))
}

/********************************************
 *   DB stuff; connecting, getting tables   *
 ********************************************/

func OpenDB() *sql.DB {
	db, err := sql.Open("mysql", "/test_db")
	if err != nil {
		log.Fatal(err)
	}
	return db
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
