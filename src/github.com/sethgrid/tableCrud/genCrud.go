package tableCrud

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
)

// GenCrud goes trough all tables passed in and creates a *.go file for each table
// in the path location. write true will write the file. write false with print to stdout.
func GenCrud(allTables map[string]*Table, path string, write bool) {
	// generate table crud
	for tableName, v := range allTables {
		log.Printf("Table %s\n", tableName)
		for _, c := range v.Cols {
			log.Printf("    Column %s, Type %s, Key %s, Extra %s\n", c.COLUMN_NAME.String, c.COLUMN_TYPE.String, c.COLUMN_KEY.String, c.EXTRA.String)
		}

		funcMap := template.FuncMap{
			// The name "title" is what the function will be called in the template text.
			"title":        strings.Title,
			"join":         joinComma,
			"structInit":   structInit,
			"castType":     castType,
			"insertHelper": insertHelper,
			"updateHelper": updateHelper,
			"deleteHelper": deleteHelper,
		}

		tmpl, err := template.New("testTemplate").Funcs(funcMap).Parse(OUTPUT_FILE)
		if err != nil {
			log.Print("error using templates ", err)
		}

		var writer io.Writer
		if write {
			writer, err = os.Create(fmt.Sprintf("%s/%s.go", path, tableName))
			if err != nil {
				log.Print("error creating *.go file: ", tableName, err)
			}
		} else {
			writer = os.Stdout
		}

		err = tmpl.Execute(writer, v)
		if err != nil {
			log.Print("error executing template ", err)
		}
	}
}
