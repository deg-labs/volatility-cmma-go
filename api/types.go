package main

import (
	"database/sql"
	"regexp"
)

var (
	validTimeframes = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"}
	validPeriods    = []string{"1h", "6h", "12h", "24h", "1d", "7d", "1w", "1M"}
	tableNameRegex  = regexp.MustCompile(`^[0-9A-Za-z]+$`)
)

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type volatilityResponse struct {
	Count int              `json:"count"`
	Data  []volatilityItem `json:"data"`
}

type volatilityItem struct {
	Symbol    string `json:"symbol"`
	Timeframe string `json:"timeframe"`
	CandleTS  int64  `json:"candle_ts"`
	Price     struct {
		Close     float64 `json:"close"`
		PrevClose float64 `json:"prev_close"`
	} `json:"price"`
	Change struct {
		Pct       float64 `json:"pct"`
		Direction string  `json:"direction"`
	} `json:"change"`
}

type volumeResponse struct {
	Count int          `json:"count"`
	Data  []volumeItem `json:"data"`
}

type volumeItem struct {
	Symbol        string  `json:"symbol"`
	TotalVolume   float64 `json:"total_volume"`
	TotalTurnover float64 `json:"total_turnover"`
	Timeframe     string  `json:"timeframe"`
	Period        string  `json:"period"`
}

type apiServer struct {
	db                *sql.DB
	ohlcvHistoryLimit int
	marketCache       *marketDataCache
}
