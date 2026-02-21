package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func ensureTables(db *sql.DB, timeframes []string) error {
	for _, tf := range timeframes {
		tableName, err := safeTableName(tf)
		if err != nil {
			return err
		}
		query := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				symbol TEXT NOT NULL,
				timestamp INTEGER NOT NULL,
				open REAL NOT NULL,
				high REAL NOT NULL,
				low REAL NOT NULL,
				close REAL NOT NULL,
				volume REAL NOT NULL,
				turnover REAL NOT NULL,
				PRIMARY KEY (symbol, timestamp)
			)
		`, tableName)
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func upsertRows(db *sql.DB, timeframe string, rowsBySymbol map[string][]klineRow) error {
	tableName, err := safeTableName(timeframe)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (symbol, timestamp, open, high, low, close, volume, turnover)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(symbol, timestamp) DO UPDATE SET
			open=excluded.open,
			high=excluded.high,
			low=excluded.low,
			close=excluded.close,
			volume=excluded.volume,
			turnover=excluded.turnover
	`, tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for symbol, rows := range rowsBySymbol {
		for _, row := range rows {
			if _, err := stmt.Exec(symbol, row.TS, row.Open, row.High, row.Low, row.Close, row.Volume, row.Turnover); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func cleanupOldRows(db *sql.DB, timeframe string, rowsBySymbol map[string][]klineRow, historyLimit int) error {
	tableName, err := safeTableName(timeframe)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(fmt.Sprintf(`
		DELETE FROM %s WHERE rowid IN (
			SELECT rowid FROM %s
			WHERE symbol = ?
			ORDER BY timestamp DESC
			LIMIT -1 OFFSET ?
		)
	`, tableName, tableName))
	if err != nil {
		return err
	}
	defer stmt.Close()

	for symbol := range rowsBySymbol {
		if _, err := stmt.Exec(symbol, historyLimit); err != nil {
			return err
		}
	}

	return tx.Commit()
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

func configureSQLiteForWriter(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA busy_timeout = 10000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}
	return nil
}
