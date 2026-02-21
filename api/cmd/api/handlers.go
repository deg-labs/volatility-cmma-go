package main

import (
	"fmt"
	"net/http"
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

func (s *apiServer) queryVolatility(timeframe string, threshold float64, offset int, direction, sort string, limit int) ([]volatilityItem, error) {
	table, err := safeTableName(timeframe)
	if err != nil {
		return nil, err
	}

	orderBy := "volatility_pct DESC"
	switch sort {
	case "volatility_asc":
		orderBy = "volatility_pct ASC"
	case "symbol_asc":
		orderBy = "symbol ASC"
	}

	query := fmt.Sprintf(`
		WITH symbols AS (
			SELECT DISTINCT symbol
			FROM %s
		),
		latest_and_previous AS (
			SELECT
				s.symbol,
				(SELECT t.timestamp FROM %s t WHERE t.symbol = s.symbol ORDER BY t.timestamp DESC LIMIT 1) AS candle_ts,
				(SELECT t.close FROM %s t WHERE t.symbol = s.symbol ORDER BY t.timestamp DESC LIMIT 1) AS close,
				(SELECT t.close FROM %s t WHERE t.symbol = s.symbol ORDER BY t.timestamp DESC LIMIT 1 OFFSET ?) AS prev_close
			FROM symbols s
		)
		SELECT
			lp.symbol,
			lp.candle_ts,
			lp.close,
			lp.prev_close,
			((lp.close - lp.prev_close) / lp.prev_close) * 100 AS volatility_pct,
			? as timeframe
		FROM latest_and_previous lp
		WHERE
			lp.prev_close IS NOT NULL
			AND ABS(((lp.close - lp.prev_close) / lp.prev_close) * 100) >= ?
			AND CASE
				WHEN ? = 'up' THEN ((lp.close - lp.prev_close) / lp.prev_close) > 0
				WHEN ? = 'down' THEN ((lp.close - lp.prev_close) / lp.prev_close) < 0
				ELSE TRUE
			END
		ORDER BY %s
		LIMIT ?
	`, table, table, table, table, orderBy)

	rows, err := s.db.Query(query, offset, timeframe, threshold, direction, direction, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]volatilityItem, 0)
	for rows.Next() {
		var item volatilityItem
		var closePrice, prevClose, pct float64
		if err := rows.Scan(&item.Symbol, &item.CandleTS, &closePrice, &prevClose, &pct, &item.Timeframe); err != nil {
			return nil, err
		}
		item.Price.Close = closePrice
		item.Price.PrevClose = prevClose
		item.Change.Pct = round4(pct)
		if pct > 0 {
			item.Change.Direction = "up"
		} else {
			item.Change.Direction = "down"
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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

func (s *apiServer) queryVolume(timeframe, period, sort string, limit int, minVolume float64, minVolumeTarget string) ([]volumeItem, error) {
	table, err := safeTableName(timeframe)
	if err != nil {
		return nil, err
	}

	periodSeconds, err := parsePeriodToSeconds(period)
	if err != nil {
		return nil, err
	}
	startTSMS := time.Now().UTC().Add(-time.Duration(periodSeconds) * time.Second).UnixMilli()

	orderBy := "total_volume DESC"
	switch sort {
	case "volume_asc":
		orderBy = "total_volume ASC"
	case "turnover_desc":
		orderBy = "total_turnover DESC"
	case "turnover_asc":
		orderBy = "total_turnover ASC"
	case "symbol_asc":
		orderBy = "symbol ASC"
	}

	havingClause := ""
	args := []any{startTSMS}
	if minVolume > 0 {
		if minVolumeTarget == "volume" {
			havingClause = "HAVING SUM(volume) > ?"
		} else {
			havingClause = "HAVING SUM(turnover) > ?"
		}
		args = append(args, minVolume)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT symbol, SUM(volume) as total_volume, SUM(turnover) as total_turnover
		FROM %s
		WHERE timestamp >= ?
		GROUP BY symbol
		%s
		ORDER BY %s
		LIMIT ?
	`, table, havingClause, orderBy)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]volumeItem, 0)
	for rows.Next() {
		var item volumeItem
		if err := rows.Scan(&item.Symbol, &item.TotalVolume, &item.TotalTurnover); err != nil {
			return nil, err
		}
		item.TotalVolume = round4(item.TotalVolume)
		item.TotalTurnover = round4(item.TotalTurnover)
		item.Timeframe = timeframe
		item.Period = period
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
