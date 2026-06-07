package engine

import (
	"path/filepath"
	"strings"
)

// OperationalMetrics is a read-only snapshot for health/readiness surfaces.
type OperationalMetrics struct {
	StoreMode              string
	DurableState           string
	WALCurrentLSN          int64
	WALFlushedLSN          int64
	WALFlushLagBytes       int64
	WALSizeBytes           int64
	SnapshotStatus         string
	SnapshotCount          int
	LastSnapshotUnixNano   int64
	WALBytesSinceSnapshot  int64
	RecoveryDurationMillis int64
	ResourcePressure       string
	RetrievalReady         bool
	TextUnitIndexReady     bool
	EntityIndexReady       bool
	CommunityIndexReady    bool
	EmptySeedIndexes       []string
}

// OperationalMetrics returns durability, resource pressure, and retrieval
// readiness metrics without mutating indexes.
func (e *Engine) OperationalMetrics() OperationalMetrics {
	metrics := OperationalMetrics{
		StoreMode:        e.storeMode,
		DurableState:     "serving",
		SnapshotStatus:   "not_configured",
		ResourcePressure: "ok",
	}
	if !e.healthy.Load() {
		metrics.DurableState = "unhealthy"
	}

	e.mu.RLock()
	for _, sess := range e.sessions {
		if sess.IsExpired() {
			continue
		}
		info := sess.GetInfo()
		if info.MaxMemoryBytes > 0 {
			ratio := float64(info.MemoryBytes) / float64(info.MaxMemoryBytes)
			if ratio >= 0.90 {
				metrics.ResourcePressure = "critical"
			} else if ratio >= 0.75 && metrics.ResourcePressure == "ok" {
				metrics.ResourcePressure = "warning"
			}
		}
		textUnits, entities, communities := sess.IndexCounts()
		metrics.TextUnitIndexReady = metrics.TextUnitIndexReady || textUnits > 0
		metrics.EntityIndexReady = metrics.EntityIndexReady || entities > 0
		metrics.CommunityIndexReady = metrics.CommunityIndexReady || communities > 0
	}
	e.mu.RUnlock()

	metrics.RetrievalReady = metrics.TextUnitIndexReady || metrics.EntityIndexReady || metrics.CommunityIndexReady
	if !metrics.TextUnitIndexReady {
		metrics.EmptySeedIndexes = append(metrics.EmptySeedIndexes, "textunit")
	}
	if !metrics.EntityIndexReady {
		metrics.EmptySeedIndexes = append(metrics.EmptySeedIndexes, "entity")
	}
	if !metrics.CommunityIndexReady {
		metrics.EmptySeedIndexes = append(metrics.EmptySeedIndexes, "community")
	}

	if e.durable == nil {
		return metrics
	}

	e.durable.mu.Lock()
	if e.durable.unhealthy || !e.healthy.Load() {
		metrics.DurableState = "unhealthy"
	} else if metrics.ResourcePressure != "ok" {
		metrics.DurableState = "degraded"
	}
	metrics.SnapshotStatus = "configured"
	lastSnapshotAt := e.durable.lastSnapshotAt
	lastSnapshotWALSize := e.durable.lastSnapshotWALSize
	if !lastSnapshotAt.IsZero() {
		metrics.LastSnapshotUnixNano = lastSnapshotAt.UnixNano()
	}
	metrics.RecoveryDurationMillis = e.durable.recoveryDuration.Milliseconds()
	snapshotDir := e.durable.snapshotDir
	wal := e.durable.wal
	e.durable.mu.Unlock()

	if wal != nil {
		current, flushed, sizeBytes, err := wal.Status()
		if err == nil {
			metrics.WALCurrentLSN = current
			metrics.WALFlushedLSN = flushed
			metrics.WALSizeBytes = sizeBytes
			if current > flushed {
				metrics.WALFlushLagBytes = current - flushed
			}
			if metrics.LastSnapshotUnixNano > 0 {
				metrics.WALBytesSinceSnapshot = sizeBytes - lastSnapshotWALSize
				if metrics.WALBytesSinceSnapshot < 0 {
					metrics.WALBytesSinceSnapshot = 0
				}
			} else {
				metrics.WALBytesSinceSnapshot = sizeBytes
			}
		}
	}
	if snapshotDir != "" {
		if count, err := countDurableSnapshots(snapshotDir); err == nil {
			metrics.SnapshotCount = count
		}
	}

	return metrics
}

func countDurableSnapshots(snapshotDir string) (int, error) {
	files, err := filepath.Glob(filepath.Join(snapshotDir, "snapshot-*.json"))
	return len(files), err
}

func (m OperationalMetrics) EmptySeedIndexesCSV() string {
	return strings.Join(m.EmptySeedIndexes, ",")
}

func (m OperationalMetrics) IsDegraded() bool {
	return m.ResourcePressure == "warning" || m.ResourcePressure == "critical" || m.DurableState == "degraded"
}
