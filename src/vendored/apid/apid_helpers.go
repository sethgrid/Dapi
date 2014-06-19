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
