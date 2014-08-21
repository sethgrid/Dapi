package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

/********************
 *   Transactions   *
 ********************/

/*
how will we manage cross server transactions?
it seems that we will have to figure out which server has
the existing transaction, and then forward our request to that box
transaction id: hostname::uuid
when we POST|PUT|DELETE, check if we are the hostname, else proxy the request
we have to consider token experiation (this should clear out the query in
in mysql and issue errors on further attempts to use the token).

tim's suggestion: use a cookie to store which server has the ongoing transaction
and let the LB determine where to forward the request. No proxy mess.
maybe we can do the same with a header?

shane's suggestion: send a series of queries in a single request, each performed as
a transaction. If the whole transaction is good, return success. The client could manage
putting it all together. Simple.
*/

type subResponseWriter struct {
	rw   http.ResponseWriter
	buf  *bytes.Buffer
	code int
}

func (s *subResponseWriter) Header() http.Header {
	return s.rw.Header()
}

func (s *subResponseWriter) Write(b []byte) (int, error) {
	return s.buf.Write(b)
}

func (s *subResponseWriter) WriteHeader(code int) {
	s.code = code
}

func (s *subResponseWriter) Success() bool {
	return s.code < 300
}

type Transaction struct {
	Requests []*Request `json:"requests"`
}

type Request struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Body   string `json:"body"`
}

func (a *Apid) PostTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	defer r.Body.Close()

	decoder := json.NewDecoder(r.Body)

	transaction := &Transaction{}
	err := decoder.Decode(&transaction)
	if err != nil {
		log.Fatal("unable to decode json", err)
	}

	log.Println("starting tx...")
	database, err := a.DataSource.DB()
	if err != nil {
		Error(w, r, "unable to connect to db", err)
		return
	}

	tx, err := database.Begin()

	if err != nil {
		Error(w, r, "unable to create transaction", err)
		return
	}

	apid := &Apid{
		DataSource: a.DataSource,
		Context:    tx,
	}
	apid.Router = apid.NewRouter()

	w.Write([]byte("["))
	defer w.Write([]byte("]"))

	for i, req := range transaction.Requests {
		if i != 0 {
			// comma separate the items of the json array response
			// but not the first item, duh
			w.Write([]byte(","))
		}

		log.Printf("\t%s %s\n", req.Method, req.URL)
		handler, params, _ := apid.Router.Lookup(req.Method, req.URL)
		if handler == nil {
			// want to return 404 or something and rollback
			tx.Rollback()
			Error(w, r, "unknown path", err)
			return
		}

		body := &bytes.Buffer{}
		body.WriteString(req.Body)

		srw := &subResponseWriter{
			rw:  w,
			buf: &bytes.Buffer{},
		}

		subReq, err := http.NewRequest(req.Method, req.URL, body)
		if err != nil {
			Error(w, r, "unable to create request", err)
			return
		}

		handler(srw, subReq, params)

		if !srw.Success() {
			log.Printf("\terror: %d\n", srw.code)
			log.Println("rolling back")

			err := tx.Rollback()
			if err != nil {
				Error(w, r, "unable to rollback", err)
				return
			}

			// todo: make this prettier, also json escape the buf value
			fmt.Fprintf(w, `{"message":"%s"}`, srw.buf.String())

			w.WriteHeader(500)

			return
		}

		fmt.Fprintf(w, srw.buf.String())
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		Error(w, r, "unable to commit", err)
		return
	}

	log.Println("tx commited")
}
