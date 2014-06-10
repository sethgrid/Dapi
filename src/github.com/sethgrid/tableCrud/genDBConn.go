package tableCrud

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
)

func GenDBConn(path string, write bool) {
	// generate db connection file
	funcMap := template.FuncMap{
		// The name "title" is what the function will be called in the template text.
		"title": strings.Title,
	}

	tmpl, err := template.New("DBTemplate").Funcs(funcMap).Parse(DB_CONNECT)
	if err != nil {
		log.Print("error using templates ", err)
	}

	var writer io.Writer
	if write {
		writer, err = os.Create(fmt.Sprintf("%s/dbconn.go", path))
		if err != nil {
			log.Print("error creating dbconn.go file: ", err)
		}
	} else {
		writer = os.Stdout
	}

	err = tmpl.Execute(writer, nil)
	if err != nil {
		log.Print("error executing template ", err)
	}
}
