// Command upstream is a minimal HTTP origin for load tests: it answers every
// request with 200 and a tiny body so measurements reflect gateway overhead.
// Paths ending in /usage also report a random cost (0-100) in
// X-Usage-Total-Tokens, like an LLM API reporting tokens used.
package main

import (
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
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
			if strings.HasSuffix(r.URL.Path, "/usage") {
				w.Header().Set("X-Usage-Total-Tokens", strconv.Itoa(rand.IntN(101)))
			}
			_, _ = w.Write(body)
		}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	log.Printf("upstream listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}
