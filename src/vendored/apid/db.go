package apid

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

/********************************************
 *   DB stuff; connecting, getting tables   *
 ********************************************/

// Constructs the dataSourceName string
type DataSourceName struct {
	User, Password, Host, Port, DBName, Raw string
}

// Method to get the constructed string
func (d *DataSourceName) String() string {
	// username:password@protocol(address)/dbname?param=value
	if len(d.Raw) > 0 {
		// mutate the DataSourceObject for logging elsewhere
		// we could populate the whole object, but yagni
		s := strings.Split(d.Raw, "/")
		d.DBName = s[len(s)-1]

		return d.Raw
	}

	var hostAndPort string
	if len(d.Port) > 0 {
		hostAndPort = fmt.Sprintf("@tcp(%s:%s)", d.Host, d.Port)
	} else {
		hostAndPort = d.Host
	}

	var pw string
	if len(d.Password) > 0 {
		pw = ":" + d.Password
	}

	return fmt.Sprintf("%s%s%s/%s", d.User, pw, hostAndPort, d.DBName)
}

// Simple wrapper to handle error
func OpenDB(d *DataSourceName) *sql.DB {
	db, err := sql.Open("mysql", d.String())

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
	TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT sql.NullString
}

// populates map of table information
func GetTables(db *sql.DB) map[string]*Table {
	allTables := make(map[string]*Table)

	// could also get TABLE_SCHEMA if it is important for future use
	r, err := db.Query("select table_name from information_schema.tables where table_type=\"BASE TABLE\"")
	if err != nil {
		log.Fatal("unable to reach information schema ", err)
	}

	// get all the tables, one at a time
	tables := make([]string, 0)
	for r.Next() {
		var name string
		err = r.Scan(&name)
		if err != nil {
			log.Print("error scanning schema ", err)
		}
		tables = append(tables, name)
	}

	// query each table's structure
	for _, t := range tables {
		nextTable := &Table{Name: t}
		allTables[t] = nextTable
		r, err := db.Query(
			fmt.Sprintf(
				"select "+
					"TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT "+
					"from information_schema.columns where "+
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
				&CHARACTER_SET_NAME,
				&COLLATION_NAME,
				&COLUMN_TYPE,
				&COLUMN_KEY,
				&EXTRA,
				&PRIVILEGES,
				&COLUMN_COMMENT)
			if err != nil {
				log.Fatal("error scanning column schema ", err)
			}
			info := &TableSchema{TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE, COLUMN_KEY, EXTRA, PRIVILEGES, COLUMN_COMMENT}
			allTables[t].Cols = append(allTables[t].Cols, info)
		}
	}

	return allTables
}
