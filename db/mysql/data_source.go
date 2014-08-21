package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sethgrid/dapi/db"
)

type DataSource struct {
	User, Password, Host, Port, DBName, Raw string
}

// Method to get the constructed string
func (d *DataSource) String() string {
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

func (d *DataSource) DB() (*db.DB, error) {
	database, err := sql.Open("mysql", d.String())
	if err != nil {
		log.Println(err)
		return nil, err
	}

	allTables := make(map[string]*db.Table)

	// could also get TABLE_SCHEMA if it is important for future use
	r, err := database.Query(`select table_name from information_schema.tables where table_type="BASE TABLE"`)
	if err != nil {
		log.Println("unable to reach information schema", err)
		return nil, err
	}

	// get all the tables, one at a time
	tables := make([]string, 0)
	for r.Next() {
		var name string
		err = r.Scan(&name)
		if err != nil {
			log.Println("error scanning schema", err)
			return nil, err
		}
		tables = append(tables, name)
	}

	// query each table's structure
	for _, t := range tables {
		nextTable := &db.Table{Name: t}
		allTables[t] = nextTable

		r, err := database.Query(
			fmt.Sprintf(
				`select
					TABLE_CATALOG,
					TABLE_SCHEMA,
					TABLE_NAME,
					COLUMN_NAME,
					ORDINAL_POSITION,
					COLUMN_DEFAULT,
					IS_NULLABLE,
					DATA_TYPE,
					CHARACTER_MAXIMUM_LENGTH,
					CHARACTER_OCTET_LENGTH,
					NUMERIC_PRECISION,
					NUMERIC_SCALE,
					CHARACTER_SET_NAME,
					COLLATION_NAME,
					COLUMN_TYPE,
					COLUMN_KEY,
					EXTRA,
					PRIVILEGES,
					COLUMN_COMMENT
				from	information_schema.columns
				where	table_name="%s"`,
				t))

		if err != nil {
			log.Println("unable to query table", t, err)
			return nil, err
		}

		for r.Next() {
			info := &db.Column{}

			err = r.Scan(
				&info.TableCatalog,
				&info.TableSchema,
				&info.TableName,
				&info.ColumnName,
				&info.OrdinalPosition,
				&info.ColumnDefault,
				&info.IsNullable,
				&info.DataType,
				&info.CharacterMaximumLength,
				&info.CharacterOctetLength,
				&info.NumericPrecision,
				&info.NumericScale,
				&info.CharacterSetName,
				&info.CollationName,
				&info.ColumnType,
				&info.ColumnKey,
				&info.Extra,
				&info.Privileges,
				&info.ColumnComment,
			)

			if err != nil {
				log.Println("error scanning column schema ", err)
				return nil, err
			}

			allTables[t].Columns = append(allTables[t].Columns, info)
		}
	}

	return &db.DB{
		DB:     database,
		Tables: allTables,
	}, nil
}
