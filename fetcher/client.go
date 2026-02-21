package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var bybitRetryDelays = []time.Duration{
	60 * time.Second,
	120 * time.Second,
	360 * time.Second,
}

func getAllLinearSymbols(ctx context.Context, httpClient *http.Client, baseURL string) ([]string, error) {
	cursor := ""
	symbols := make([]string, 0, 800)

	for {
		url := fmt.Sprintf("%s/v5/market/instruments-info?category=linear&status=Trading&limit=1000", baseURL)
		if cursor != "" {
			url += "&cursor=" + cursor
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}

		var payload bybitInstrumentsResp
		if err := runBybitWithRetry(ctx, "instruments-info", func() error {
			resp, err := httpClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
				return err
			}
			if resp.StatusCode >= 300 {
				return fmt.Errorf("status=%d", resp.StatusCode)
			}
			if payload.RetCode != 0 {
				return fmt.Errorf("bybit retCode=%d retMsg=%s", payload.RetCode, payload.RetMsg)
			}
			return nil
		}); err != nil {
			return nil, err
		}

		for _, item := range payload.Result.List {
			if strings.HasSuffix(item.Symbol, "USDT") {
				symbols = append(symbols, item.Symbol)
			}
		}

		cursor = payload.Result.NextPageCursor
		if cursor == "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return symbols, nil
}

func getKlineData(ctx context.Context, httpClient *http.Client, baseURL, symbol, interval string, limit int) ([]klineRow, error) {
	url := fmt.Sprintf("%s/v5/market/kline?category=linear&symbol=%s&interval=%s&limit=%d", baseURL, symbol, interval, limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	var payload bybitKlineResp
	if err := runBybitWithRetry(ctx, "kline", func() error {
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return err
		}
		if resp.StatusCode >= 300 {
			return fmt.Errorf("status=%d", resp.StatusCode)
		}
		if payload.RetCode != 0 {
			return fmt.Errorf("retCode=%d retMsg=%s", payload.RetCode, payload.RetMsg)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	rows := make([]klineRow, 0, len(payload.Result.List))
	for _, item := range payload.Result.List {
		if len(item) < 7 {
			continue
		}
		ts, err := strconv.ParseInt(item[0], 10, 64)
		if err != nil {
			continue
		}
		op, err1 := strconv.ParseFloat(item[1], 64)
		hi, err2 := strconv.ParseFloat(item[2], 64)
		lo, err3 := strconv.ParseFloat(item[3], 64)
		cl, err4 := strconv.ParseFloat(item[4], 64)
		vol, err5 := strconv.ParseFloat(item[5], 64)
		to, err6 := strconv.ParseFloat(item[6], 64)
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil || err5 != nil || err6 != nil {
			continue
		}
		rows = append(rows, klineRow{TS: ts, Open: op, High: hi, Low: lo, Close: cl, Volume: vol, Turnover: to})
	}
	return rows, nil
}

func runBybitWithRetry(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	maxAttempts := len(bybitRetryDelays) + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == maxAttempts {
			break
		}

		wait := bybitRetryDelays[attempt-1]
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			return fmt.Errorf("bybit %s retry aborted: %w", operation, ctx.Err())
		case <-timer.C:
		}
	}

	return fmt.Errorf("bybit %s failed after %d attempts: %w", operation, maxAttempts, lastErr)
}
