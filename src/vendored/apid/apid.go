package apid

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
)

/********************************
 *   APID Struct and Handlers   *
 ********************************/

// the apid struct allows us to use the db at different endpoints
type Apid struct {
	DB     *sql.DB
	Tables map[string]*Table
}

// returns all routing
func (a *Apid) NewRouter() http.Handler {
	// routing
	router := httprouter.New()
	router.GET("/", RootHandler)
	router.GET("/favicon.ico", NullHandler) // chrome browser handler
	router.GET("/api/v1/crud/:table/_meta", a.TableMetaHandler)

	router.GET("/api/v1/crud/:table", a.GetTable)
	router.POST("/api/v1/crud/:table", a.PostTable)
	router.PUT("/api/v1/crud/:table", a.PutTable)
	router.DELETE("/api/v1/crud/:table", a.DeleteTable)

	router.GET("/api/v1/transaction", GetTransaction)
	router.POST("/api/v1/transaction", PostTransaction)
	router.PUT("/api/v1/transaction", PutTransaction)
	router.DELETE("/api/v1/transaction", DeleteTransaction)

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

	// this should be pulled out into a function. We should have success and fail handlers.
	log.Printf("200 - %s %s", r.Method, r.RequestURI)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(j))

}

// PostTable inserts a record
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

// PutTable looks for the primary key and errors if missing. Updates records.
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

// Delete table looks for a limit key. Deletes records.
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
