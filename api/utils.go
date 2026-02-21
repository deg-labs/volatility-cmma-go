package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func parseTimeframeToMinutes(s string) (int, error) {
	if len(s) < 2 {
		return 0, errors.New("invalid timeframe")
	}
	num, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || num <= 0 {
		return 0, errors.New("invalid timeframe")
	}
	unit := s[len(s)-1]
	switch unit {
	case 'm':
		return num, nil
	case 'h':
		return num * 60, nil
	case 'd':
		return num * 60 * 24, nil
	case 'w':
		return num * 60 * 24 * 7, nil
	case 'M':
		return num * 60 * 24 * 30, nil
	default:
		return 0, fmt.Errorf("unsupported timeframe unit: %s", s)
	}
}

func parsePeriodToMinutes(s string) (int, error) {
	if len(s) < 2 {
		return 0, errors.New("invalid period")
	}
	num, err := strconv.Atoi(s[:len(s)-1])
	if err != nil || num <= 0 {
		return 0, errors.New("invalid period")
	}
	unit := s[len(s)-1]
	switch unit {
	case 'h':
		return num * 60, nil
	case 'd':
		return num * 60 * 24, nil
	case 'w':
		return num * 60 * 24 * 7, nil
	case 'M':
		return num * 60 * 24 * 30, nil
	default:
		return 0, fmt.Errorf("unsupported period unit: %s", s)
	}
}

func parsePeriodToSeconds(s string) (int, error) {
	minutes, err := parsePeriodToMinutes(s)
	if err != nil {
		return 0, err
	}
	return minutes * 60, nil
}

func parsePositiveFloat(v string) (float64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || f <= 0 {
		return 0, errors.New("invalid number")
	}
	return f, nil
}

func safeTableName(timeframe string) (string, error) {
	tf := strings.TrimSpace(timeframe)
	if tf == "" {
		return "", errors.New("empty timeframe")
	}
	if !tableNameRegex.MatchString(tf) {
		return "", fmt.Errorf("invalid timeframe format: %s", tf)
	}
	return "ohlcv_" + tf, nil
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	resp := errorResponse{}
	resp.Error.Code = code
	resp.Error.Message = message
	writeJSONStatus(w, status, resp)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	writeJSONStatus(w, status, payload)
}

func writeJSONStatus(w http.ResponseWriter, status int, payload any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func getEnv(key, fallback string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	return val
}

func configureSQLiteForReader(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA busy_timeout = 1000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA query_only = ON",
		"PRAGMA cache_size = -65536",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}
	return nil
}
