package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
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

		hasRows, err := timeframeHasRows(db, timeframe)
		if err != nil {
			return fmt.Errorf("check timeframe rows %s: %w", timeframe, err)
		}
		fetchLimit := cfg.OHLCVHistoryLimit
		if hasRows {
			fetchLimit = dynamicIncrementalFetchLimit(cfg.FetchIntervalSeconds, timeframe, cfg.OHLCVHistoryLimit)
		}

		logger.Printf("timeframe %s: fetching (limit=%d)", timeframe, fetchLimit)
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

				rows, fetchErr := getKlineData(ctx, httpClient, cfg.BaseURL, s, interval, fetchLimit)
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
		if err := cleanupOldRows(db, timeframe, cfg.OHLCVHistoryLimit); err != nil {
			return fmt.Errorf("cleanup timeframe %s: %w", timeframe, err)
		}

		logger.Printf("timeframe %s: persisted symbols=%d", timeframe, len(results))
	}

	return nil
}

func dynamicIncrementalFetchLimit(fetchIntervalSeconds int, timeframe string, historyLimit int) int {
	if historyLimit <= 0 {
		historyLimit = 1
	}

	tfSeconds, err := timeframeToSeconds(timeframe)
	if err != nil || tfSeconds <= 0 {
		return minInt(historyLimit, 10)
	}

	if fetchIntervalSeconds <= 0 {
		fetchIntervalSeconds = tfSeconds
	}

	needed := int(math.Ceil(float64(fetchIntervalSeconds)/float64(tfSeconds))) + 2
	if needed < 2 {
		needed = 2
	}
	if needed > historyLimit {
		needed = historyLimit
	}
	return needed
}

func timeframeToSeconds(s string) (int, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid timeframe: %s", s)
	}
	num, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || num <= 0 {
		return 0, fmt.Errorf("invalid timeframe: %s", s)
	}
	unit := s[len(s)-1]
	switch unit {
	case 'm':
		return num * 60, nil
	case 'h':
		return num * 60 * 60, nil
	case 'd':
		return num * 60 * 60 * 24, nil
	case 'w':
		return num * 60 * 60 * 24 * 7, nil
	case 'M':
		return num * 60 * 60 * 24 * 30, nil
	default:
		return 0, fmt.Errorf("unsupported timeframe: %s", strings.TrimSpace(s))
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
