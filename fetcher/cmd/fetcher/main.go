package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	cfg := loadConfig()
	logger := log.New(os.Stdout, "", log.LstdFlags)

	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		logger.Fatalf("db open failed: %v", err)
	}
	defer db.Close()

	if err := configureSQLiteForWriter(db); err != nil {
		logger.Fatalf("set sqlite pragma failed: %v", err)
	}

	if err := ensureTables(db, cfg.Timeframes); err != nil {
		logger.Fatalf("ensure tables failed: %v", err)
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Printf("fetcher started, timeframes=%v interval=%ds", cfg.Timeframes, cfg.FetchIntervalSeconds)

	for {
		start := time.Now()
		if err := fetchAndStore(ctx, logger, httpClient, db, cfg); err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("fetch cycle error: %v", err)
		}
		if ctx.Err() != nil {
			logger.Printf("fetcher stopped")
			return
		}

		elapsed := time.Since(start)
		wait := time.Duration(cfg.FetchIntervalSeconds)*time.Second - elapsed
		if wait < 0 {
			wait = 0
		}
		logger.Printf("cycle complete in %.2fs, waiting %.2fs", elapsed.Seconds(), wait.Seconds())

		select {
		case <-ctx.Done():
			logger.Printf("fetcher stopped")
			return
		case <-time.After(wait):
		}
	}
}
