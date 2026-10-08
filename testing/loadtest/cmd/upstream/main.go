// Command upstream is a minimal HTTP origin for load tests: it answers every
// request with 200 and a tiny body so measurements reflect gateway overhead.
package main

import (
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	addr := os.Getenv("UPSTREAM_ADDR")
	if addr == "" {
		addr = ":9000"
	}
	body := []byte("ok\n")
	srv := &http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write(body)
		}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	log.Printf("upstream listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}
