package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
)

func main() {
	log.Println("started")

	// currently, we only work on mysql dbs with no user, no password
	DB := OpenDB()

	// allows us to continue to introspect on our tables
	// without having to continually ping the database
	tables := GetTables(DB)

	// container object to expose the db and the tables at endpoints
	apid := &Apid{DB: DB, Tables: tables}

	// routing
	router := httprouter.New()
	router.GET("/", rootHandler)
	router.GET("/favicon.ico", nullHandler) // chrome browser handler
	router.GET("/api/v1/crud/:table/_meta", apid.TableMetaHandler)

	router.GET("/api/v1/crud/:table", apid.GetTable)
	router.POST("/api/v1/crud/:table", apid.PostTable)
	router.PUT("/api/v1/crud/:table", apid.PutTable)
	router.DELETE("/api/v1/crud/:table", apid.DeleteTable)

	router.GET("/api/v1/transaction", GetTransaction)
	router.POST("/api/v1/transaction", PostTransaction)
	router.PUT("/api/v1/transaction", PutTransaction)
	router.DELETE("/api/v1/transaction", DeleteTransaction)

	// use our own NotFound Handler
	router.NotFound = NotFound
	router.RedirectTrailingSlash = true

	// could wrap this and use a recover() to prevent
	// panics from taking down the server
	http.ListenAndServe(":9000", router)
}

// everything below here needs to be pulled into its own
// package

/********************************
 *   APID Struct and Handlers   *
 ********************************/

// the apid struct allows us to use the db at different endpoints
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
// for the existance of the table requested and returns requested
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
	query, args := a.SelectQueryComposer(table.Name, r)

	rows, err := a.DB.Query(query, args...)

	if err != nil {
		log.Printf("Error querying GET on %s", table.Name)
		NotFoundWithParams(w, r, fmt.Sprintf("GET request failed on %s", table.Name))
		return
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
	responses := make([]map[string]interface{}, 0)

	// populate json object from rows
	for rows.Next() {
		resp := make(map[string]interface{})
		if err := rows.Scan(columnPointers...); err != nil {
			log.Fatalln(err)
		}

		for i, data := range columns {
			// Here we could do some type checking to get
			// int, bool, etc. Defaulting always to string.
			if v, ok := data.(int64); ok {
				resp[columnNames[i]] = v
			} else if v, ok := data.([]byte); ok {
				resp[columnNames[i]] = string(v)
			}
		}
		responses = append(responses, resp)
	}

	j, err := json.Marshal(responses)
	if err != nil {
		log.Fatal(err)
	}

	// this should be pulled out into a function
	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(j))

}

// stub
func (a *Apid) PostTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	tableName := t.ByName("table")

	if _, ok := a.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}
	table := a.Tables[tableName]

	// should we look for the primary key and weed it out?
	q, args, err := InsertQueryComposer(table.Name, r)
	if err != nil {
		NotFoundWithParams(w, r, err.Error())
		return
	}

	res, err := a.DB.Exec(q, args...)
	if err != nil {
		NotFoundWithParams(w, r, err.Error()+" :: "+q)
		return
	}
	insertId, err := res.LastInsertId()
	if err != nil {
		log.Printf("error ", err)
	}
	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf("{\"message\":\"success\", \"inserted_id\":%d}", insertId)))
}

func (a *Apid) PutTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	tableName := t.ByName("table")

	if _, ok := a.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}
	table := a.Tables[tableName]

	pKey := ""
	for _, c := range table.Cols {
		if c.COLUMN_KEY.String == "PRI" {
			pKey = c.COLUMN_NAME.String
		}
	}
	if len(pKey) == 0 {
		NotFoundWithParams(w, r, fmt.Sprintf("Update table (%s), no primary key on table", tableName))
		return
	}

	q, args, err := UpdateQueryComposer(table.Name, pKey, r)
	if err != nil {
		NotFoundWithParams(w, r, err.Error())
		return
	}

	res, err := a.DB.Exec(q, args...)
	if err != nil {
		NotFoundWithParams(w, r, err.Error()+" :: "+q)
		return
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Printf("error ", err)
	}
	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf("{\"message\":\"success\", \"rows_affected\":%d}", rowsAffected)))
}

// stub
func (a *Apid) DeleteTable(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	tableName := t.ByName("table")

	if _, ok := a.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}
	table := a.Tables[tableName]

	q, args, err := DeleteQueryComposer(table.Name, r)
	if err != nil {
		NotFoundWithParams(w, r, err.Error())
		return
	}

	res, err := a.DB.Exec(q, args...)
	if err != nil {
		NotFoundWithParams(w, r, err.Error()+" :: "+q)
		return
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Printf("error ", err)
	}
	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf("{\"message\":\"success\", \"rows_affected\":%d}", rowsAffected)))
}

/*******************
 *   Apid helpers  *
 *******************/

// the meta results allow for endpoint discoverability and represent
// a table
type Meta struct {
	Description string              `json:"description"`
	SchemaType  string              `json:"type"`
	Properties  map[string]Property `json:"properties"`
	Required    []string            `json:"required"`
	Primary     string              `json:"primary"`
	Location    string              `json:"location"`
	Methods     []string            `json:"methods"`
	Title       string              `json:"title"`
	Notes       string              `json:"notes"`
}

// properties are each column on a table
type Property struct {
	Description string `json:"description"`
	DataType    string `json:"type"`
}

// displayes the meta data for a single table
func (a *Apid) TableMetaHandler(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	tableName := t.ByName("table")
	if _, ok := a.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("No table (%s) found for _meta", tableName))
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
	location := r.RequestURI[:len(r.RequestURI)-len("_meta")]
	j, err := json.Marshal(GenMeta(a.Tables[tableName], location))
	if err != nil {
		log.Printf("error making json schema ", err)
	}
	w.Write(j)
}

// displays the meta data for the whole database
func (a *Apid) MetaHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("metaHandler")
	wholeSchema := make([]Meta, 0)
	for _, t := range a.Tables {
		location := r.RequestURI[:len(r.RequestURI)-len("_meta")] + t.Name
		wholeSchema = append(wholeSchema, GenMeta(t, location))
	}
	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
	j, err := json.Marshal(wholeSchema)
	if err != nil {
		log.Printf("error making json whole schema ", err)
	}
	w.Write(j)
}

// common functionality for generating meta data
func GenMeta(table *Table, location string) Meta {
	// this could all be initialized at startup, as oppposed to each call
	properties := make(map[string]Property)
	required := make([]string, 0)
	primary := ""
	schemaType := "object"
	notes := "TODO: have each method have its own entry because PUT requires the primary key and Delete requires a limit, and POST does not want the primary key."
	// assume all methods for now. latter, we can have a table restrictions
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	// get column info for properties
	for _, c := range table.Cols {
		p := Property{}
		p.DataType = c.DATA_TYPE.String
		p.Description = c.COLUMN_COMMENT.String

		if c.COLUMN_KEY.String == "PRI" {
			log.Print("setting primary key")
			primary = c.COLUMN_NAME.String
		}

		if c.IS_NULLABLE.String == "NO" {
			required = append(required, c.COLUMN_NAME.String)
		}

		properties[c.COLUMN_NAME.String] = p
	}
	properties["limit"] = Property{DataType: "int", Description: "Used to limit the number of results returned"}
	properties["offset"] = Property{DataType: "int", Description: "Used to offset results returned"}

	// init schema struct
	schema := Meta{}
	schema.Title = table.Name
	schema.Properties = properties
	schema.Description = "MySQL Table " + table.Name
	schema.Location = location
	schema.Primary = primary
	schema.Required = required
	schema.SchemaType = schemaType
	schema.Methods = methods
	schema.Notes = notes

	return schema
}

// InsertQueryComposer creates a mysql update query
func InsertQueryComposer(table string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("insert into %v set ", table)
	set := ""
	args := make([]interface{}, 0)

	for k, v := range v {
		set += fmt.Sprintf("%v=?,", k) // better: strings.Join()
		args = append(args, v)
	}

	set = set[:len(set)-1] // remove trailing comma

	return q + set, args, nil
}

// DeleteQueryComposer creates a mysql update query
func DeleteQueryComposer(table string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("delete from %v where ", table)
	where := ""
	limit := ""
	var limitArg interface{}
	args := make([]interface{}, 0)

	for k, v := range v {
		if k == "limit" {
			limit = " limit ?"
			limitArg = v
			continue
		}
		where += fmt.Sprintf("%v=? and", k) // better: strings.Join()
		args = append(args, v)
	}

	// if limit was not populated, then err out.
	if len(limit) == 0 {
		return "", nil, errors.New("Missing limit key in delete query on " + table)
	}
	args = append(args, limitArg)
	where = where[:len(where)-3] // remove trailing and

	return q + where + limit, args, nil
}

// UpdateQueryComposer creates a mysql update query
func UpdateQueryComposer(table, pKey string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("update %v set ", table)
	set := ""
	where := ""
	var whereArg interface{}
	args := make([]interface{}, 0)

	for k, v := range v {
		// it would be nice to have table.Cols as map rather than slice
		if k == pKey {
			where = fmt.Sprintf(" where %v=? limit 1", pKey)
			whereArg = v
			continue
		}
		set += fmt.Sprintf("%v=?,", k) // better: strings.Join()
		args = append(args, v)
	}

	// if where was not populated, then we were not given the primary key in the query.
	if len(where) == 0 {
		return "", nil, errors.New("Missing primary key in query on " + table)
	}
	args = append(args, whereArg)
	set = set[:len(set)-1] // remove trailing comma

	return q + set + where, args, nil
}

// refactor to take interface with methods *.URL.RawQuery
func (a *Apid) SelectQueryComposer(table string, r *http.Request) (string, []interface{}) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		log.Print("error", err)
	}

	// init the query
	q := fmt.Sprintf("select * from %v", table)
	var where, limit, offset, orderby string
	args := make([]interface{}, 0)

	// consider creating this at start time
	cols := make(map[string]bool, 0)
	for _, c := range a.Tables[table].Cols {
		cols[c.COLUMN_NAME.String] = true
	}

	for k, v := range params {
		switch k {
		case "limit":
			// feels wrong. where are parameterized query builders?
			l, err := strconv.Atoi(v[0])
			if err != nil {
				log.Printf("skipping limit ", err)
			}
			limit = fmt.Sprintf(" limit %d", l)
		case "offset":
			l, err := strconv.Atoi(v[0])
			if err != nil {
				log.Printf("skipping offset ", err)
			}
			offset = fmt.Sprintf(" offset %d", l)
		case "orderby":
		default:
			// prolly better to use strings.Join()
			if ok := cols[k]; !ok {
				log.Print("bad column ", k) // 404 to user?
				continue
			}
			where += " " + k + "=? and"
			args = append(args, v[0])
		}
	}

	// prep `where` and remove trailing 'and'
	if len(where) > 6 {
		where = " where " + where[:len(where)-4]
	}

	// only allow offset if limit is present
	if len(limit) == 0 && len(offset) > 0 {
		offset = ""
		log.Print("Removing offset because limit is missing")
	}

	q += where + orderby + limit + offset
	log.Print(q, " ", args)
	return q, args
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

/********************
 *   Transactions   *
 ********************/

/*
how will we manage cross server transactions?
it seems that we will have to figure out which server has
the existing transaction, and then forward our request to that box
transaction id: hostname::uuid
when we POST|PUT|DELETE, check if we are the hostname, else proxy the request
we have to consider token experiation (this should clear out the query in
in mysql and issue errors on further attempts to use the token).

tim's suggestion: use a cookie to store which server has the ongoing transaction
and let the LB determine where to forward the request. No proxy mess.
maybe we can do the same with a header?
*/

func GetTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func PostTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func PutTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func DeleteTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
