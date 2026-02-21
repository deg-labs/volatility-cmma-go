package main

import (
	"os"
	"strconv"
	"strings"
)

func loadConfig() config {
	timeframes := strings.Split(getEnv("TIMEFRAMES", "1m,5m,15m,30m,1h,4h,1d"), ",")
	cleaned := make([]string, 0, len(timeframes))
	for _, tf := range timeframes {
		tf = strings.TrimSpace(tf)
		if tf != "" {
			cleaned = append(cleaned, tf)
		}
	}
	if len(cleaned) == 0 {
		cleaned = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
	}

	fetchInterval, _ := strconv.Atoi(getEnv("FETCH_INTERVAL_SECONDS", "300"))
	if fetchInterval <= 0 {
		fetchInterval = 300
	}

	historyLimit, _ := strconv.Atoi(getEnv("OHLCV_HISTORY_LIMIT", "1000"))
	if historyLimit <= 0 {
		historyLimit = 1000
	}

	concurrency, _ := strconv.Atoi(getEnv("CONCURRENCY_LIMIT", "10"))
	if concurrency <= 0 {
		concurrency = 10
	}

	return config{
		Timeframes:           cleaned,
		FetchIntervalSeconds: fetchInterval,
		OHLCVHistoryLimit:    historyLimit,
		ConcurrencyLimit:     concurrency,
		BaseURL:              getEnv("BYBIT_BASE_URL", "https://api.bybit.com"),
		DBPath:               getEnv("DB_PATH", "/app/data/cmma.db"),
	}
}

func getEnv(key, fallback string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	return val
}
