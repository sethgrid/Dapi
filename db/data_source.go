package db

import "database/sql"

type DataSource interface {
	String() string
	DB() (*DB, error)
}

type DB struct {
	*sql.DB
	Tables map[string]*Table
}

type Table struct {
	Name    string
	Columns []*Column
}

type Column struct {
	TableCatalog,
	TableSchema,
	TableName,
	ColumnName,
	OrdinalPosition,
	ColumnDefault,
	IsNullable,
	DataType,
	CharacterMaximumLength,
	CharacterOctetLength,
	NumericPrecision,
	NumericScale,
	CharacterSetName,
	CollationName,
	ColumnType,
	ColumnKey,
	Extra,
	Privileges,
	ColumnComment sql.NullString
}
