package main

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *apiServer) rootHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Welcome to CMMA API v2. See /volatility/docs for details."})
}

func (s *apiServer) volatilityHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	timeframe := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	if !contains(validTimeframes, timeframe) {
		writeError(w, http.StatusBadRequest, "INVALID_TIMEFRAME", fmt.Sprintf("無効なタイムフレームです。有効な値: %s", strings.Join(validTimeframes, ", ")))
		return
	}

	threshold, err := parsePositiveFloat(r.URL.Query().Get("threshold"))
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "threshold は0より大きい数値を指定してください")
		return
	}

	offset := 1
	if offsetRaw := strings.TrimSpace(r.URL.Query().Get("offset")); offsetRaw != "" {
		offset, err = strconv.Atoi(offsetRaw)
		if err != nil || offset <= 0 {
			writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "offset は1以上の整数を指定してください")
			return
		}
	}

	direction := strings.TrimSpace(r.URL.Query().Get("direction"))
	if direction == "" {
		direction = "both"
	}
	if direction != "up" && direction != "down" && direction != "both" {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "direction は up/down/both のいずれかを指定してください")
		return
	}

	sort := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sort == "" {
		sort = "volatility_desc"
	}
	if sort != "volatility_desc" && sort != "volatility_asc" && sort != "symbol_asc" {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "sort は volatility_desc/volatility_asc/symbol_asc のいずれかを指定してください")
		return
	}

	limit := 100
	if limitRaw := strings.TrimSpace(r.URL.Query().Get("limit")); limitRaw != "" {
		limit, err = strconv.Atoi(limitRaw)
		if err != nil || limit <= 0 || limit > 500 {
			writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "limit は1以上500以下の整数を指定してください")
			return
		}
	}

	items, queryErr := s.queryVolatility(timeframe, threshold, offset, direction, sort, limit)
	if queryErr != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", queryErr.Error())
		return
	}

	writeJSON(w, http.StatusOK, volatilityResponse{Count: len(items), Data: items})
}

func (s *apiServer) queryVolatility(timeframe string, threshold float64, offset int, direction, sortKey string, limit int) ([]volatilityItem, error) {
	snapshot, err := s.marketCache.getSnapshot(timeframe)
	if err != nil {
		return nil, err
	}

	items := make([]volatilityItem, 0, len(snapshot.seriesBySymbol))
	for symbol, candles := range snapshot.seriesBySymbol {
		if len(candles) <= offset {
			continue
		}
		latest := candles[0]
		prev := candles[offset]
		if prev.Close == 0 {
			continue
		}

		pct := ((latest.Close - prev.Close) / prev.Close) * 100
		if math.Abs(pct) < threshold {
			continue
		}
		if direction == "up" && pct <= 0 {
			continue
		}
		if direction == "down" && pct >= 0 {
			continue
		}

		item := volatilityItem{
			Symbol:    symbol,
			Timeframe: timeframe,
			CandleTS:  latest.TS,
		}
		item.Price.Close = latest.Close
		item.Price.PrevClose = prev.Close
		item.Change.Pct = round4(pct)
		if pct > 0 {
			item.Change.Direction = "up"
		} else {
			item.Change.Direction = "down"
		}
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		switch sortKey {
		case "volatility_asc":
			if items[i].Change.Pct == items[j].Change.Pct {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].Change.Pct < items[j].Change.Pct
		case "symbol_asc":
			return items[i].Symbol < items[j].Symbol
		default:
			if items[i].Change.Pct == items[j].Change.Pct {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].Change.Pct > items[j].Change.Pct
		}
	})

	if limit < len(items) {
		items = items[:limit]
	}
	return items, nil
}

func (s *apiServer) volumeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
		return
	}

	timeframe := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	if !contains(validTimeframes, timeframe) {
		writeError(w, http.StatusBadRequest, "INVALID_TIMEFRAME", fmt.Sprintf("無効なタイムフレームです。有効な値: %s", strings.Join(validTimeframes, ", ")))
		return
	}

	period := strings.TrimSpace(r.URL.Query().Get("period"))
	if !contains(validPeriods, period) {
		writeError(w, http.StatusBadRequest, "INVALID_PERIOD", fmt.Sprintf("無効な期間指定です。有効な値: %s", strings.Join(validPeriods, ", ")))
		return
	}

	timeframeMinutes, err := parseTimeframeToMinutes(timeframe)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_UNIT", err.Error())
		return
	}
	periodMinutes, err := parsePeriodToMinutes(period)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_UNIT", err.Error())
		return
	}

	requiredCandles := periodMinutes / timeframeMinutes
	if requiredCandles > s.ohlcvHistoryLimit {
		msg := fmt.Sprintf("指定された期間 (%s) とタイムフレーム (%s) の組み合わせでは、%d本のローソク足が必要です。これは現在利用可能な履歴の最大本数(%d本)を超えています。より短い期間、またはより大きなタイムフレームを選択してください。", period, timeframe, requiredCandles, s.ohlcvHistoryLimit)
		writeError(w, http.StatusBadRequest, "INSUFFICIENT_HISTORY", msg)
		return
	}

	minVolume := 0.0
	if raw := strings.TrimSpace(r.URL.Query().Get("min_volume")); raw != "" {
		minVolume, err = parsePositiveFloat(raw)
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "min_volume は0より大きい数値を指定してください")
			return
		}
	}

	minVolumeTarget := strings.TrimSpace(r.URL.Query().Get("min_volume_target"))
	if minVolumeTarget == "" {
		minVolumeTarget = "turnover"
	}
	if minVolumeTarget != "volume" && minVolumeTarget != "turnover" {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "min_volume_target は volume/turnover のいずれかを指定してください")
		return
	}

	sort := strings.TrimSpace(r.URL.Query().Get("sort"))
	if sort == "" {
		sort = "volume_desc"
	}
	if sort != "volume_desc" && sort != "volume_asc" && sort != "turnover_desc" && sort != "turnover_asc" && sort != "symbol_asc" {
		writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "sort は volume_desc/volume_asc/turnover_desc/turnover_asc/symbol_asc のいずれかを指定してください")
		return
	}

	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > 500 {
			writeError(w, http.StatusUnprocessableEntity, "INVALID_INPUT", "limit は1以上500以下の整数を指定してください")
			return
		}
	}

	items, queryErr := s.queryVolume(timeframe, period, sort, limit, minVolume, minVolumeTarget)
	if queryErr != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", queryErr.Error())
		return
	}

	writeJSON(w, http.StatusOK, volumeResponse{Count: len(items), Data: items})
}

func (s *apiServer) queryVolume(timeframe, period, sortKey string, limit int, minVolume float64, minVolumeTarget string) ([]volumeItem, error) {
	snapshot, err := s.marketCache.getSnapshot(timeframe)
	if err != nil {
		return nil, err
	}
	periodSeconds, err := parsePeriodToSeconds(period)
	if err != nil {
		return nil, err
	}
	startTSMS := time.Now().UTC().Add(-time.Duration(periodSeconds) * time.Second).UnixMilli()

	items := make([]volumeItem, 0, len(snapshot.seriesBySymbol))
	for symbol, candles := range snapshot.seriesBySymbol {
		item := volumeItem{
			Symbol:    symbol,
			Timeframe: timeframe,
			Period:    period,
		}
		hasRecent := false
		for _, candle := range candles {
			if candle.TS < startTSMS {
				break
			}
			hasRecent = true
			item.TotalVolume += candle.Volume
			item.TotalTurnover += candle.Turnover
		}
		if !hasRecent {
			continue
		}
		if minVolume > 0 {
			if minVolumeTarget == "volume" && item.TotalVolume <= minVolume {
				continue
			}
			if minVolumeTarget == "turnover" && item.TotalTurnover <= minVolume {
				continue
			}
		}
		item.TotalVolume = round4(item.TotalVolume)
		item.TotalTurnover = round4(item.TotalTurnover)
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		switch sortKey {
		case "volume_asc":
			if items[i].TotalVolume == items[j].TotalVolume {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].TotalVolume < items[j].TotalVolume
		case "turnover_desc":
			if items[i].TotalTurnover == items[j].TotalTurnover {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].TotalTurnover > items[j].TotalTurnover
		case "turnover_asc":
			if items[i].TotalTurnover == items[j].TotalTurnover {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].TotalTurnover < items[j].TotalTurnover
		case "symbol_asc":
			return items[i].Symbol < items[j].Symbol
		default:
			if items[i].TotalVolume == items[j].TotalVolume {
				return items[i].Symbol < items[j].Symbol
			}
			return items[i].TotalVolume > items[j].TotalVolume
		}
	})

	if limit < len(items) {
		items = items[:limit]
	}
	return items, nil
}
