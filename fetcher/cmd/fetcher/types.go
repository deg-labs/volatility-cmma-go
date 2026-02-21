package main

import "regexp"

var (
	timeframeMap = map[string]string{
		"1m": "1", "5m": "5", "15m": "15", "30m": "30",
		"1h": "60", "4h": "240", "1d": "D", "1w": "W", "1M": "M",
	}
	tableNameRegex = regexp.MustCompile(`^[0-9A-Za-z]+$`)
)

type config struct {
	Timeframes           []string
	FetchIntervalSeconds int
	OHLCVHistoryLimit    int
	ConcurrencyLimit     int
	BaseURL              string
	DBPath               string
}

type bybitInstrumentsResp struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		List []struct {
			Symbol string `json:"symbol"`
		} `json:"list"`
		NextPageCursor string `json:"nextPageCursor"`
	} `json:"result"`
}

type bybitKlineResp struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		List [][]string `json:"list"`
	} `json:"result"`
}

type klineRow struct {
	TS       int64
	Open     float64
	High     float64
	Low      float64
	Close    float64
	Volume   float64
	Turnover float64
}
