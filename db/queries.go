package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
)

func generateKeyValues(w io.Writer, sep string, v map[string]interface{}) []interface{} {
	args := make([]interface{}, 0)
	i := 0
	for col, val := range v {
		if i > 0 {
			fmt.Fprint(w, sep)
		}

		fmt.Fprintf(w, "`%v`=?", col)
		args = append(args, val)
		i++
	}

	fmt.Fprint(w, " ")

	return args
}

type Query interface {
	Exec(DBContext) (*sql.Rows, error)
}

type Command interface {
	Exec(DBContext) (*sql.Result, error)
}

type Insert struct {
	table  string
	values map[string]interface{}
}

func (i *Insert) Exec(ctx DBContext) (sql.Result, error) {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "INSERT INTO `%s` SET ", i.table)
	args := generateKeyValues(buf, ", ", i.values)

	return ctx.Exec(buf.String(), args...)
}

type Delete struct {
	table string
	where map[string]interface{}
	limit int
}

func (d *Delete) Exec(ctx DBContext) (sql.Result, error) {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "DELETE FROM `%s` ", d.table)

	var args []interface{}
	if len(d.where) > 0 {
		buf.WriteString("WHERE ")
		args = generateKeyValues(buf, "and ", d.where)
	}

	if d.limit > 0 {
		fmt.Fprintf(buf, "LIMIT %d", d.limit)
	}

	return ctx.Exec(buf.String(), args...)
}

type Update struct {
	table  string
	values map[string]interface{}
	where  map[string]interface{}
}

func (u *Update) Exec(ctx DBContext) (sql.Result, error) {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "UPDATE `%s` SET ", u.table)

	var values, where []interface{}
	if len(u.values) > 0 {
		values = generateKeyValues(buf, ", ", u.values)
	}

	if len(u.where) > 0 {
		buf.WriteString("WHERE ")
		where = generateKeyValues(buf, "and ", u.where)
	}

	args := make([]interface{}, 0)
	args = append(args, values...)
	args = append(args, where...)

	query := buf.String()
	log.Println(query)

	return ctx.Exec(query, args...)
}

type Select struct {
	table  string
	where  map[string]interface{}
	limit  int
	offset int
	order  string
}

func (s *Select) Exec(ctx DBContext) (*sql.Rows, error) {
	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "SELECT * FROM `%s` ", s.table)

	var where []interface{}
	if len(s.where) > 0 {
		buf.WriteString("WHERE ")
		where = generateKeyValues(buf, "and ", s.where)
	}

	if len(s.order) > 0 {
		fmt.Fprintf(buf, "ORDER BY %s ", s.order)
	}

	if s.limit > 0 {
		fmt.Fprintf(buf, "LIMIT %d ", s.limit)
	}

	if s.offset > 0 {
		fmt.Fprintf(buf, "OFFSET %d ", s.offset)
	}

	return ctx.Query(buf.String(), where...)
}

func NewInsert(t *Table, values map[string]interface{}) *Insert {
	return &Insert{
		table:  t.Name,
		values: values,
	}
}

func NewDelete(t *Table, where map[string]interface{}, limit int) *Delete {
	return &Delete{
		table: t.Name,
		where: where,
		limit: limit,
	}
}

func NewUpdate(t *Table, values map[string]interface{}, where map[string]interface{}) *Update {
	return &Update{
		table:  t.Name,
		values: values,
		where:  where,
	}
}

func NewSelect(t *Table, where map[string]interface{}, limit int, offset int, order string) *Select {
	return &Select{
		table:  t.Name,
		where:  where,
		limit:  limit,
		offset: offset,
		order:  order,
	}
}
