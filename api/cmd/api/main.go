package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	_ "modernc.org/sqlite"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	dbPath := getEnv("DB_PATH", "/app/data/cmma.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		logger.Fatalf("db open failed: %v", err)
	}
	defer db.Close()

	if err := configureSQLiteForReader(db); err != nil {
		logger.Fatalf("set sqlite pragma failed: %v", err)
	}

	historyLimit, _ := strconv.Atoi(getEnv("OHLCV_HISTORY_LIMIT", "5"))
	if historyLimit <= 0 {
		historyLimit = 5
	}

	s := &apiServer{db: db, ohlcvHistoryLimit: historyLimit}
	openAPISpec, err := buildOpenAPISpec()
	if err != nil {
		logger.Fatalf("openapi spec build failed: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.rootHandler)
	mux.HandleFunc("/volatility", s.volatilityHandler)
	mux.HandleFunc("/volume", s.volumeHandler)

	var handler http.Handler = mux
	handler = middleware.SwaggerUI(middleware.SwaggerUIOpts{
		BasePath: "/",
		Path:     "volatility/docs",
		SpecURL:  "/volatility/openapi.json",
		Title:    "CMMA API",
	}, handler)
	handler = middleware.Spec("/", openAPISpec, handler, middleware.WithSpecPath("volatility"), middleware.WithSpecDocument("openapi.json"))

	addr := ":8000"
	logger.Printf("api started on %s", addr)
	if err := http.ListenAndServe(addr, withJSONContentType(handler)); err != nil {
		logger.Fatalf("server error: %v", err)
	}
}

func withJSONContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/volatility/openapi.json" || r.URL.Path == "/volatility/docs" || r.URL.Path == "/volatility/docs/" {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}
