package apid

import (
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

func GetTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func PostTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func PutTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
func DeleteTransaction(w http.ResponseWriter, r *http.Request, t httprouter.Params) {
	w.Write([]byte("unimplemented"))
}
