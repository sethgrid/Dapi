package tableCrud

import (
	"fmt"
	"log"
	"strings"
)

const DB_CONNECT = `package crud

import (
	"log"
	"fmt"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func SetDB(){
	db_user := ""
	db_pw := ""
	db_name := "test_db"
	dataSource := ""
	if len(db_user) == 0 {
		dataSource = fmt.Sprintf("/%s", db_name)
	} else if len(db_pw) == 0 {
		dataSource = fmt.Sprintf("%s@/%s", db_user, db_name)
	} else {
		dataSource = fmt.Sprintf("%s:%s@/%s", db_user, db_pw, db_name)
	}
	var err error
	DB, err = sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal("unable to open db ", err)
	}
}
`

const OUTPUT_FILE = `{{$name := .Name}}{{$cols := .Cols}}package crud

import (
	"log"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type {{ title  .Name }}Record struct{ {{range $cols}}
	{{title .COLUMN_NAME.String }} {{ castType .DATA_TYPE.String }}{{end}}
}

type {{ title .Name }} struct{
	Tx *sql.Tx
}

// Function to help make the api feel cleaner
func (t *{{ title .Name }}) Commit() error {
	return t.Tx.Commit()
}

func (t *{{ title .Name }}) Post(u *{{ title .Name }}Record) error {
	var err error
	if t.Tx == nil {
		// new transaction
		t.Tx, err = DB.Begin()
		if err != nil {
			log.Print("error beginning transaction in *{{ title .Name }}.Post: ", err)
		}
	}

	_, err = t.Tx.Query({{ insertHelper . }})
	return err
}

func (t *{{ title .Name }}) Put(u *{{ title .Name }}Record) error {
	var err error
	if t.Tx == nil {
		// new transaction
		t.Tx, err = DB.Begin()
		if err != nil {
			log.Print("error beginning transaction in *{{ title .Name }}.Put: ", err)
		}
	}

	_, err = t.Tx.Query({{ updateHelper . }})
	return err
}

func (t *{{ title .Name }}) Delete(u *{{ title .Name }}Record) error {
	var err error
	if t.Tx == nil {
		// new transaction
		t.Tx, err = DB.Begin()
		if err != nil {
			log.Print("error beginning transaction in *{{ title .Name }}.Put: ", err)
		}
	}

	_, err = t.Tx.Query({{ deleteHelper . }})
	return err
}

{{ range $cols }}
func (t *{{ title $name}}) GetBy{{ title .COLUMN_NAME.String}}({{ .COLUMN_NAME.String }} {{ castType .DATA_TYPE.String }}) []*{{ title $name}}Record{
	r, err := DB.Query("select * from {{ $name }} where {{ .COLUMN_NAME.String }}=?", {{ .COLUMN_NAME.String}})
	if err != nil {
		// TODO: use our ln package
		log.Print("error in Query: ", err)
	}
	res := make([]*{{ title $name }}Record, 0)
	for r.Next() { {{ range $cols }}
		var {{ .COLUMN_NAME.String }} {{ castType .DATA_TYPE.String }}{{end}}
		err = r.Scan({{ join $cols }})
		if err != nil {
			log.Print("error scanning schema ", err)
		}
		s := &{{ title $name }}Record{ {{ structInit $cols }} }
		res = append(res, s)
	}
	return res
}
{{end}}
`

// Helper function to generate insert query
func insertHelper(t *Table) string {
	set := make([]string, 0)
	val := make([]string, 0)

	for _, col := range t.Cols {
		// we don't insert the primary key value if it is auto increment int
		if col.COLUMN_KEY.String == "PRI" &&
			col.EXTRA.String == "auto_increment" &&
			col.COLUMN_TYPE.String == "int" {
			continue
		}
		set = append(set, col.COLUMN_NAME.String+"=?")
		val = append(val, "u."+strings.Title(col.COLUMN_NAME.String))
	}
	return fmt.Sprintf(
		"\"insert into %s set %s\", %s",
		t.Name, strings.Join(set, ","),
		strings.Join(val, ","),
	)
}

// Helper function to generate insert query
func updateHelper(t *Table) string {
	set := make([]string, 0)
	val := make([]string, 0)
	whereQ := ""
	whereV := ""
	for _, col := range t.Cols {
		if col.COLUMN_KEY.String == "PRI" {
			whereQ = col.COLUMN_NAME.String + "=?"
			whereV = "u." + strings.Title(col.COLUMN_NAME.String)
		}
		set = append(set, col.COLUMN_NAME.String+"=?")
		val = append(val, "u."+strings.Title(col.COLUMN_NAME.String))
	}
	return fmt.Sprintf(
		"\"update %s set %s where %s\", %s, %s",
		t.Name,
		strings.Join(set, ","),
		whereQ,
		strings.Join(val, ","),
		whereV,
	)
}

// Helper function to generate insert query
func deleteHelper(t *Table) string {
	whereQ := ""
	whereV := ""
	for _, col := range t.Cols {
		if col.COLUMN_KEY.String == "PRI" {
			whereQ = col.COLUMN_NAME.String + "=?"
			whereV = "u." + strings.Title(col.COLUMN_NAME.String)
		}
	}
	return fmt.Sprintf(
		"\"delete from %s where %s\", %s",
		t.Name,
		whereQ,
		whereV,
	)
}

// Helper function to populate Scan arguments in template
func joinComma(cols []*TableSchema) string {
	conversion := make([]string, 0)
	for _, col := range cols {
		conversion = append(conversion, "&"+col.COLUMN_NAME.String)
	}
	return strings.Join(conversion, ",")
}

// Helper function to init a struct
func structInit(cols []*TableSchema) string {
	conversion := make([]string, 0)
	for _, col := range cols {
		conversion = append(conversion, strings.Title(col.COLUMN_NAME.String)+":"+col.COLUMN_NAME.String)
	}
	return strings.Join(conversion, ",")
}

// Helper function to change mysql types to go types
func castType(m string) string {
	switch m {
	// TODO: add more cases
	case "varchar":
		return "string"
	case "int":
		return "int64"
	case "tinyint":
		return "bool"
	default:
		// TODO: change this to panic after all cases are set
		log.Print("Unknown type: ", m)
		return "?"
	}
}
