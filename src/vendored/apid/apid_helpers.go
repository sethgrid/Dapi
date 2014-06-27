package apid

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

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
	Method      string              `json:"method"`
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
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	schema := make([]Meta, 0)
	for _, method := range methods {
		schema = append(schema, GenMeta(a.Tables[tableName], location, method))
	}
	j, err := json.Marshal(schema)
	if err != nil {
		log.Printf("error making json schema ", err)
	}
	w.Write(j)
}

// displays the meta data for the whole database
func (a *Apid) MetaHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Print("metaHandler")
	wholeSchema := make([]Meta, 0)
	methods := []string{"GET", "POST", "PUT", "DELETE"}

	for _, t := range a.Tables {
		for _, method := range methods {
			location := r.RequestURI[:len(r.RequestURI)-len("_meta")] + t.Name
			wholeSchema = append(wholeSchema, GenMeta(t, location, method))
		}
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
func GenMeta(table *Table, location, method string) Meta {
	// this could all be initialized at startup, as oppposed to each call
	properties := make(map[string]Property)
	required := make([]string, 0)
	primary := ""
	schemaType := "object"
	notes := ""

	// get column info for properties
	for _, c := range table.Cols {
		p := Property{}
		p.DataType = c.DATA_TYPE.String
		p.Description = c.COLUMN_COMMENT.String

		if c.COLUMN_KEY.String == "PRI" {
			primary = c.COLUMN_NAME.String
		}

		if c.IS_NULLABLE.String == "NO" {
			// TODO: refactor to get this POST logic down into the switch
			if !((method == "POST" || method == "GET") && c.COLUMN_KEY.String == "PRI") {
				required = append(required, c.COLUMN_NAME.String)
			}
		}

		properties[c.COLUMN_NAME.String] = p
	}

	switch method {
	case "GET":
		properties["limit"] = Property{DataType: "int", Description: "Used to limit the number of results returned"}
		properties["offset"] = Property{DataType: "int", Description: "Used to offset results returned"}
	case "POST":
	case "PUT":
	case "DELETE":
		properties["limit"] = Property{DataType: "int", Description: "Used to limit the number of records deleted"}
		required = append(required, "limit")
	}

	// init schema struct
	schema := Meta{}
	schema.Title = table.Name
	schema.Properties = properties
	schema.Description = "MySQL Table " + table.Name
	schema.Location = location
	schema.Primary = primary
	schema.Required = required
	schema.SchemaType = schemaType
	schema.Method = method
	schema.Notes = notes

	return schema
}
