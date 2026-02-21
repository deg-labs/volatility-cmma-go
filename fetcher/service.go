package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func fetchAndStore(ctx context.Context, logger *log.Logger, httpClient *http.Client, db *sql.DB, cfg config, fillStartupGaps bool) error {
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
		} else {
			if err := upsertRows(db, timeframe, results); err != nil {
				return fmt.Errorf("upsert timeframe %s: %w", timeframe, err)
			}
			logger.Printf("timeframe %s: persisted symbols=%d", timeframe, len(results))
		}
		if err := cleanupOldRows(db, timeframe, cfg.OHLCVHistoryLimit); err != nil {
			return fmt.Errorf("cleanup timeframe %s: %w", timeframe, err)
		}

		if !fillStartupGaps {
			continue
		}

		filledRows, missingPoints, err := backfillMissingByTimestamp(ctx, logger, httpClient, db, cfg, timeframe, interval, symbols)
		if err != nil {
			return fmt.Errorf("backfill missing timeframe %s: %w", timeframe, err)
		}
		if missingPoints > 0 {
			logger.Printf("timeframe %s: startup gap backfill complete missing_timestamps=%d filled_rows=%d", timeframe, missingPoints, filledRows)
		}
	}

	return nil
}

type missingTimeRange struct {
	startMs    int64
	endMs      int64
	timestamps []int64
}

func backfillMissingByTimestamp(
	ctx context.Context,
	logger *log.Logger,
	httpClient *http.Client,
	db *sql.DB,
	cfg config,
	timeframe string,
	interval string,
	symbols []string,
) (int, int, error) {
	stepSeconds, err := timeframeToSeconds(timeframe)
	if err != nil {
		return 0, 0, err
	}
	stepMs := int64(stepSeconds) * 1000

	missingBySymbol, err := detectMissingTimestamps(db, timeframe, cfg.OHLCVHistoryLimit, stepMs, symbols)
	if err != nil {
		return 0, 0, err
	}
	if len(missingBySymbol) == 0 {
		return 0, 0, nil
	}

	totalMissing := 0
	for _, missing := range missingBySymbol {
		totalMissing += len(missing)
	}

	filled := make(map[string][]klineRow, len(missingBySymbol))
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.ConcurrencyLimit)

	for symbol, missing := range missingBySymbol {
		s := symbol
		targetTS := append([]int64(nil), missing...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			rows, fetchErr := fetchMissingRowsForSymbol(ctx, httpClient, cfg.BaseURL, s, interval, targetTS, stepMs)
			if fetchErr != nil {
				logger.Printf("gap fill error symbol=%s tf=%s: %v", s, timeframe, fetchErr)
				return
			}
			if len(rows) == 0 {
				return
			}

			mu.Lock()
			filled[s] = rows
			mu.Unlock()
		}()
	}
	wg.Wait()

	filledRows := 0
	for _, rows := range filled {
		filledRows += len(rows)
	}
	if filledRows == 0 {
		return 0, totalMissing, nil
	}

	if err := upsertRows(db, timeframe, filled); err != nil {
		return 0, totalMissing, err
	}
	if err := cleanupOldRows(db, timeframe, cfg.OHLCVHistoryLimit); err != nil {
		return 0, totalMissing, err
	}

	return filledRows, totalMissing, nil
}

func fetchMissingRowsForSymbol(
	ctx context.Context,
	httpClient *http.Client,
	baseURL, symbol, interval string,
	missingTS []int64,
	stepMs int64,
) ([]klineRow, error) {
	if len(missingTS) == 0 {
		return nil, nil
	}

	ranges := groupMissingTimestamps(missingTS, stepMs)
	expected := make(map[int64]struct{}, len(missingTS))
	for _, ts := range missingTS {
		expected[ts] = struct{}{}
	}

	collected := make(map[int64]klineRow, len(missingTS))
	for _, r := range ranges {
		limit := len(r.timestamps)
		rows, err := getKlineDataByTimeRange(ctx, httpClient, baseURL, symbol, interval, r.startMs, r.endMs, limit)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if _, ok := expected[row.TS]; ok {
				collected[row.TS] = row
			}
		}
	}

	out := make([]klineRow, 0, len(collected))
	for _, row := range collected {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TS > out[j].TS })
	return out, nil
}

func groupMissingTimestamps(missingTS []int64, stepMs int64) []missingTimeRange {
	if len(missingTS) == 0 {
		return nil
	}

	ordered := append([]int64(nil), missingTS...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] > ordered[j] })

	ranges := make([]missingTimeRange, 0, len(ordered))
	current := []int64{ordered[0]}

	flush := func() {
		if len(current) == 0 {
			return
		}
		newest := current[0]
		oldest := current[len(current)-1]
		ranges = append(ranges, missingTimeRange{
			startMs:    oldest,
			endMs:      newest + stepMs - 1,
			timestamps: append([]int64(nil), current...),
		})
	}

	for i := 1; i < len(ordered); i++ {
		if ordered[i-1]-ordered[i] == stepMs {
			current = append(current, ordered[i])
			continue
		}
		flush()
		current = []int64{ordered[i]}
	}
	flush()
	return ranges
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
