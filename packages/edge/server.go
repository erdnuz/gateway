package edge

import (
	"net/http"
	"strings"
)

type EdgeServer struct {
}

func (s *EdgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Path Parsing: /{prefix}/{service}/rest/of/path
	pathParts := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
	if len(pathParts) < 2 {
		http.Error(w, "Invalid route format", http.StatusNotFound)
		return
	}
	prefixName := pathParts[0]
	serviceName := pathParts[1]

}
