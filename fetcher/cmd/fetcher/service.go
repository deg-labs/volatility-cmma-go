package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
)

func fetchAndStore(ctx context.Context, logger *log.Logger, httpClient *http.Client, db *sql.DB, cfg config) error {
	symbols, err := getAllLinearSymbols(ctx, httpClient, cfg.BaseURL)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return errors.New("no symbols returned from bybit")
	}
	logger.Printf("found %d symbols", len(symbols))

	for _, timeframe := range cfg.Timeframes {
		interval, ok := timeframeMap[timeframe]
		if !ok {
			logger.Printf("skip unsupported timeframe: %s", timeframe)
			continue
		}

		logger.Printf("timeframe %s: fetching", timeframe)
		results := make(map[string][]klineRow, len(symbols))
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, cfg.ConcurrencyLimit)

		for _, symbol := range symbols {
			s := symbol
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-sem }()

				rows, fetchErr := getKlineData(ctx, httpClient, cfg.BaseURL, s, interval, cfg.OHLCVHistoryLimit)
				if fetchErr != nil {
					logger.Printf("kline error symbol=%s tf=%s: %v", s, timeframe, fetchErr)
					return
				}
				if len(rows) == 0 {
					return
				}

				mu.Lock()
				results[s] = rows
				mu.Unlock()
			}()
		}
		wg.Wait()

		if len(results) == 0 {
			logger.Printf("timeframe %s: no rows fetched", timeframe)
			continue
		}

		if err := upsertRows(db, timeframe, results); err != nil {
			return fmt.Errorf("upsert timeframe %s: %w", timeframe, err)
		}
		if err := cleanupOldRows(db, timeframe, results, cfg.OHLCVHistoryLimit); err != nil {
			return fmt.Errorf("cleanup timeframe %s: %w", timeframe, err)
		}

		logger.Printf("timeframe %s: persisted symbols=%d", timeframe, len(results))
	}

	return nil
}
