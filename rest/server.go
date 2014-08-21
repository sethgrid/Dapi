package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"
	"github.com/sethgrid/dapi/db"
)

/********************************
 *   APID Struct and Handlers   *
 ********************************/
// the meta results allow for endpoint discoverability and represent
// a table
type Meta struct {
	Description string               `json:"description"`
	SchemaType  string               `json:"type"`
	Properties  map[string]*Property `json:"properties"`
	Required    []string             `json:"required"`
	Primary     string               `json:"primary"`
	Location    string               `json:"location"`
	Method      string               `json:"method"`
	Title       string               `json:"title"`
	Notes       string               `json:"notes"`
}

// properties are each column on a table
type Property struct {
	Description string `json:"description"`
	DataType    string `json:"type"`
}

// the apid struct allows us to use the db at different endpoints
type Apid struct {
	DataSource db.DataSource
	Context    db.DBContext
	Router     *httprouter.Router
}

func (a *Apid) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	a.Router.ServeHTTP(rw, r)
}

// returns all routing
func (a *Apid) NewRouter() *httprouter.Router {
	// routing
	router := httprouter.New()
	router.GET("/", RootHandler)
	router.GET("/favicon.ico", NullHandler) // chrome browser handler

	router.GET("/api/v1/crud/:table/_meta", a.TableMetaHandler)

	router.GET("/api/v1/crud/:table", a.GetTable)
	router.POST("/api/v1/crud/:table", a.PostTable)
	router.PUT("/api/v1/crud/:table/:id", a.PutTable)
	router.DELETE("/api/v1/crud/:table", a.DeleteTable)

	router.POST("/api/v1/transaction", a.PostTransaction)

	// use our own NotFound Handler
	router.NotFound = NotFound
	router.RedirectTrailingSlash = true

	return router
}

// specifically used for handling chrome browser seeking the favicon
func NullHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {}

// just handles the `/` endpoint
func RootHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("index handler")
	w.Write([]byte("Root. Available paths: /api/v1/crud/_meta, /api/v1/crud/:table, /api/v1/crud/:table/_meta"))
}

// standard 404 page
func NotFound(w http.ResponseWriter, r *http.Request) {
	log.Printf("404 - %s %s", r.Method, r.RequestURI)
	http.Error(w, "resource does not exist", http.StatusNotFound)
}

// 404 page to which we can pass a message string
func NotFoundWithParams(w http.ResponseWriter, r *http.Request, e string) {
	log.Printf("404 - %s %s", r.Method, r.RequestURI)
	http.Error(w, e, http.StatusNotFound)
}

// 500 page to which we can pass a message string
func Error(w http.ResponseWriter, r *http.Request, msg string, err error) {
	log.Printf("500 - %s %s - %s: %q", r.Method, r.RequestURI, msg, err)
	http.Error(w, fmt.Sprintf("%s: %q", msg, err.Error()), http.StatusNotFound)
}

func parseArgs(r io.ReadCloser) (map[string]interface{}, error) {
	defer r.Close()
	decoder := json.NewDecoder(r)

	v := make(map[string]interface{})
	err := decoder.Decode(&v)

	if err != nil {
		return nil, err
	}

	return v, nil
}

// GetTable forwards _meta requests onward. Otherwise, it checks
// for the existance of the table requested and returns requested
// records
func (a *Apid) GetTable(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// verify table name validity
	tableName := params.ByName("table")
	if tableName == "_meta" {
		a.MetaHandler(w, r, params)
		return
	}

	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	if _, ok := database.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}

	// query the table
	table := database.Tables[tableName]
	args := make(map[string]interface{})

	queryString, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		Error(w, r, "unable to parse query string", err)
		return
	}

	// consider creating this at start time
	cols := make(map[string]struct{}, 0)
	for _, c := range database.Tables[tableName].Columns {
		cols[c.ColumnName.String] = struct{}{}
	}

	for k, v := range queryString {
		switch k {
		case "limit":
			// TODO
		case "offset":
			// TODO
		case "orderby":
			// TODO
		default:
			if _, ok := cols[k]; ok {
				args[k] = v[0]
				continue
			}

			log.Print("bad column ", k) // 404 to user?
		}
	}

	// TODO: limit, offset, order => as query string args
	query := db.NewSelect(table, args, 0, 0, "")
	fmt.Println("query created")

	rows, err := query.Exec(a.Context)
	if err != nil {
		Error(w, r, "unable to query", err)
		return
	}

	// grab all the column names returned and prepare them
	// to receive data
	columnNames, err := rows.Columns()
	if err != nil {
		Error(w, r, "unable to get columns", err)
		return
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
			Error(w, r, "unable to read row", err)
			return
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

	encoder := json.NewEncoder(w)
	err = encoder.Encode(responses)

	if err != nil {
		Error(w, r, "unable to json encode", err)
		return
	}

	// this should be pulled out into a function. We should have success and fail handlers.
	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
}

// PostTable inserts a record
func (a *Apid) PostTable(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	tableName := params.ByName("table")
	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	if _, ok := database.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}

	table := database.Tables[tableName]
	args, err := parseArgs(r.Body)
	if err != nil {
		Error(w, r, "unable to parse args", err)
		return
	}

	query := db.NewInsert(table, args)
	res, err := query.Exec(a.Context)
	if err != nil {
		Error(w, r, "unable to execute query", err)
		return
	}

	insertId, err := res.LastInsertId()
	if err != nil {
		Error(w, r, "unable to get last id", err)
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"message\":\"success\", \"inserted_id\":%d}", insertId)
}

// PutTable looks for the primary key and errors if missing. Updates records.
func (a *Apid) PutTable(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	tableName := params.ByName("table")
	id := params.ByName("id")
	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	if _, ok := database.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}

	table := database.Tables[tableName]
	args, err := parseArgs(r.Body)
	if err != nil {
		Error(w, r, "unable to parse args", err)
		return
	}

	pKey := ""
	for _, c := range table.Columns {
		if c.ColumnKey.String == "PRI" {
			pKey = c.ColumnName.String
			break
		}
	}
	if len(pKey) == 0 {
		NotFoundWithParams(w, r, fmt.Sprintf("Update table (%s), no primary key on table", tableName))
		return
	}

	query := db.NewUpdate(table, args, map[string]interface{}{pKey: id})
	res, err := query.Exec(a.Context)
	if err != nil {
		Error(w, r, "unable to execute update", err)
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		Error(w, r, "unable to get affected rows", err)
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"message\":\"success\", \"rows_affected\":%d}", rowsAffected)
}

// Delete table looks for a limit key. Deletes records.
func (a *Apid) DeleteTable(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	tableName := params.ByName("table")
	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	if _, ok := database.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("table (%s) not found", tableName))
		return
	}

	table := database.Tables[tableName]
	args, err := parseArgs(r.Body)
	if err != nil {
		Error(w, r, "unable to parse args", err)
		return
	}

	// TODO: limit
	query := db.NewDelete(table, args, 0)
	res, err := query.Exec(a.Context)
	if err != nil {
		Error(w, r, "unable to execute delete", err)
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		Error(w, r, "unable to get rows affected", err)
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"message\":\"success\", \"rows_affected\":%d}", rowsAffected)
}

// displays the meta data for a single table
func (a *Apid) TableMetaHandler(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	tableName := params.ByName("table")
	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	if _, ok := database.Tables[tableName]; !ok {
		NotFoundWithParams(w, r, fmt.Sprintf("No table (%s) found for _meta", tableName))
		return
	}

	location := r.RequestURI[:len(r.RequestURI)-len("_meta")]
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	schema := make([]*Meta, 0)
	for _, method := range methods {
		schema = append(schema, genMeta(database.Tables[tableName], location, method))
	}

	encoder := json.NewEncoder(w)
	err = encoder.Encode(schema)
	if err != nil {
		log.Printf("error making json schema ", err)
		Error(w, r, "unable to encode json", err)
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
}

// displays the meta data for the whole database
func (a *Apid) MetaHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("metaHandler")
	db, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	wholeSchema := make([]*Meta, 0)
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	for _, t := range db.Tables {
		for _, method := range methods {
			location := r.RequestURI[:len(r.RequestURI)-len("_meta")] + t.Name
			wholeSchema = append(wholeSchema, genMeta(t, location, method))
		}
	}

	encoder := json.NewEncoder(w)
	err = encoder.Encode(wholeSchema)
	if err != nil {
		log.Printf("error making json whole schema ", err)
		Error(w, r, "unable to json encode", err)
		return
	}

	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
}

// common functionality for generating meta data
func genMeta(table *db.Table, location, method string) *Meta {
	// this could all be initialized at startup, as oppposed to each call
	properties := make(map[string]*Property)
	required := make([]string, 0)
	primary := ""
	schemaType := "object"
	notes := ""

	// get column info for properties
	for _, c := range table.Columns {
		p := &Property{
			DataType:    c.DataType.String,
			Description: c.ColumnComment.String,
		}

		if c.ColumnKey.String == "PRI" {
			primary = c.ColumnName.String
		}

		if c.IsNullable.String == "NO" {
			// TODO: refactor to get this POST logic down into the switch
			if !((method == "POST" || method == "GET") && c.ColumnKey.String == "PRI") {
				required = append(required, c.ColumnName.String)
			}
		}

		properties[c.ColumnName.String] = p
	}

	switch method {
	case "GET":
		properties["limit"] = &Property{DataType: "int", Description: "Used to limit the number of results returned"}
		properties["offset"] = &Property{DataType: "int", Description: "Used to offset results returned"}
	case "POST":
	case "PUT":
	case "DELETE":
		properties["limit"] = &Property{DataType: "int", Description: "Used to limit the number of records deleted"}
		required = append(required, "limit")
	}

	// init schema struct
	return &Meta{
		Title:       table.Name,
		Properties:  properties,
		Description: "MySQL Table " + table.Name,
		Location:    location,
		Primary:     primary,
		Required:    required,
		SchemaType:  schemaType,
		Method:      method,
		Notes:       notes,
	}
}
