package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type marketCandle struct {
	TS       int64
	Close    float64
	Volume   float64
	Turnover float64
}

type marketSnapshot struct {
	seriesBySymbol map[string][]marketCandle
	refreshedAt    time.Time
}

type marketDataCache struct {
	db           *sql.DB
	historyLimit int
	refreshEvery time.Duration

	mu         sync.RWMutex
	snapshots  map[string]marketSnapshot
	refreshing map[string]bool
}

func newMarketDataCache(db *sql.DB, historyLimit int, refreshEvery time.Duration) *marketDataCache {
	if refreshEvery <= 0 {
		refreshEvery = 5 * time.Second
	}
	return &marketDataCache{
		db:           db,
		historyLimit: historyLimit,
		refreshEvery: refreshEvery,
		snapshots:    make(map[string]marketSnapshot),
		refreshing:   make(map[string]bool),
	}
}

func (c *marketDataCache) getSnapshot(timeframe string) (marketSnapshot, error) {
	now := time.Now().UTC()

	c.mu.RLock()
	snapshot, ok := c.snapshots[timeframe]
	c.mu.RUnlock()
	if ok && now.Sub(snapshot.refreshedAt) < c.refreshEvery {
		return snapshot, nil
	}
	if ok {
		c.refreshSnapshotAsync(timeframe)
		return snapshot, nil
	}

	return c.refreshSnapshot(timeframe, now)
}

func (c *marketDataCache) refreshSnapshot(timeframe string, now time.Time) (marketSnapshot, error) {
	c.mu.RLock()
	if snapshot, ok := c.snapshots[timeframe]; ok && now.Sub(snapshot.refreshedAt) < c.refreshEvery {
		c.mu.RUnlock()
		return snapshot, nil
	}
	c.mu.RUnlock()

	tableName, err := safeTableName(timeframe)
	if err != nil {
		return marketSnapshot{}, err
	}

	query := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				symbol,
				timestamp,
				close,
				volume,
				turnover,
				ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
			FROM %s
		)
		SELECT symbol, timestamp, close, volume, turnover
		FROM ranked
		WHERE rn <= ?
		ORDER BY symbol ASC, timestamp DESC
	`, tableName)

	rows, err := c.db.Query(query, c.historyLimit)
	if err != nil {
		return marketSnapshot{}, err
	}
	defer rows.Close()

	seriesBySymbol := make(map[string][]marketCandle)
	for rows.Next() {
		var symbol string
		var candle marketCandle
		if err := rows.Scan(&symbol, &candle.TS, &candle.Close, &candle.Volume, &candle.Turnover); err != nil {
			return marketSnapshot{}, err
		}
		seriesBySymbol[symbol] = append(seriesBySymbol[symbol], candle)
	}
	if err := rows.Err(); err != nil {
		return marketSnapshot{}, err
	}

	snapshot := marketSnapshot{
		seriesBySymbol: seriesBySymbol,
		refreshedAt:    now,
	}
	c.mu.Lock()
	c.snapshots[timeframe] = snapshot
	c.mu.Unlock()
	return snapshot, nil
}

func (c *marketDataCache) refreshSnapshotAsync(timeframe string) {
	c.mu.Lock()
	if c.refreshing[timeframe] {
		c.mu.Unlock()
		return
	}
	c.refreshing[timeframe] = true
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.refreshing[timeframe] = false
			c.mu.Unlock()
		}()
		_, _ = c.refreshSnapshot(timeframe, time.Now().UTC())
	}()
}
