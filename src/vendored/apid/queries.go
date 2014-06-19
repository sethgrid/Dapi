package apid

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

/***********************
 *   Query Composers   *
 ***********************/

// InsertQueryComposer creates a mysql update query
func InsertQueryComposer(table string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("insert into %v set ", table)
	set := ""
	args := make([]interface{}, 0)

	for k, v := range v {
		set += fmt.Sprintf("%v=?,", k) // better: strings.Join()
		args = append(args, v)
	}

	set = set[:len(set)-1] // remove trailing comma

	return q + set, args, nil
}

// DeleteQueryComposer creates a mysql update query
func DeleteQueryComposer(table string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("delete from %v where ", table)
	where := ""
	limit := ""
	var limitArg interface{}
	args := make([]interface{}, 0)

	for k, v := range v {
		if k == "limit" {
			limit = " limit ?"
			limitArg = v
			continue
		}
		where += fmt.Sprintf("%v=? and", k) // better: strings.Join()
		args = append(args, v)
	}

	// if limit was not populated, then err out.
	if len(limit) == 0 {
		return "", nil, errors.New("Missing limit key in delete query on " + table)
	}
	args = append(args, limitArg)
	where = where[:len(where)-3] // remove trailing and

	return q + where + limit, args, nil
}

// UpdateQueryComposer creates a mysql update query
func UpdateQueryComposer(table, pKey string, r *http.Request) (string, []interface{}, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}

	// the body should be key value pairs, populate them into `v`
	v := make(map[string]interface{})
	err = json.Unmarshal(body, &v)
	if err != nil {
		log.Print("error decoding json body to map ", err)
	}

	// set up the query
	q := fmt.Sprintf("update %v set ", table)
	set := ""
	where := ""
	var whereArg interface{}
	args := make([]interface{}, 0)

	for k, v := range v {
		// it would be nice to have table.Cols as map rather than slice
		if k == pKey {
			where = fmt.Sprintf(" where %v=? limit 1", pKey)
			whereArg = v
			continue
		}
		set += fmt.Sprintf("%v=?,", k) // better: strings.Join()
		args = append(args, v)
	}

	// if where was not populated, then we were not given the primary key in the query.
	if len(where) == 0 {
		return "", nil, errors.New("Missing primary key in query on " + table)
	}
	args = append(args, whereArg)
	set = set[:len(set)-1] // remove trailing comma

	return q + set + where, args, nil
}

// refactor to take interface with methods *.URL.RawQuery
func (a *Apid) SelectQueryComposer(table string, r *http.Request) (string, []interface{}) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		log.Print("error", err)
	}

	// init the query
	q := fmt.Sprintf("select * from %v", table)
	var where, limit, offset, orderby string
	args := make([]interface{}, 0)

	// consider creating this at start time
	cols := make(map[string]bool, 0)
	for _, c := range a.Tables[table].Cols {
		cols[c.COLUMN_NAME.String] = true
	}

	for k, v := range params {
		switch k {
		case "limit":
			// feels wrong. where are parameterized query builders?
			l, err := strconv.Atoi(v[0])
			if err != nil {
				log.Printf("skipping limit ", err)
			}
			limit = fmt.Sprintf(" limit %d", l)
		case "offset":
			l, err := strconv.Atoi(v[0])
			if err != nil {
				log.Printf("skipping offset ", err)
			}
			offset = fmt.Sprintf(" offset %d", l)
		case "orderby":
		default:
			// prolly better to use strings.Join()
			if ok := cols[k]; !ok {
				log.Print("bad column ", k) // 404 to user?
				continue
			}
			where += " " + k + "=? and"
			args = append(args, v[0])
		}
	}

	// prep `where` and remove trailing 'and'
	if len(where) > 6 {
		where = " where " + where[:len(where)-4]
	}

	// only allow offset if limit is present
	if len(limit) == 0 && len(offset) > 0 {
		offset = ""
		log.Print("Removing offset because limit is missing")
	}

	q += where + orderby + limit + offset
	log.Print(q, " ", args)
	return q, args
}
