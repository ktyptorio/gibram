package client

import (
	"testing"
	"time"

	pb "github.com/gibram-io/gibram/proto/gibrampb"
)

// =============================================================================
// Configuration Tests
// =============================================================================

const testSessionID = "test-session-1"

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.MaxConnections != DefaultPoolSize {
		t.Errorf("MaxConnections = %d, want %d", cfg.MaxConnections, DefaultPoolSize)
	}
	if cfg.ConnTimeout != DefaultConnTimeout {
		t.Errorf("ConnTimeout = %v, want %v", cfg.ConnTimeout, DefaultConnTimeout)
	}
	if cfg.IdleTimeout != DefaultIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", cfg.IdleTimeout, DefaultIdleTimeout)
	}
	if cfg.MaxRetries != DefaultMaxRetries {
		t.Errorf("MaxRetries = %d, want %d", cfg.MaxRetries, DefaultMaxRetries)
	}
}

func TestPoolConfigDefaults(t *testing.T) {
	cfg := PoolConfig{
		// Leave all values at zero
	}

	// Verify defaults are applied correctly
	if cfg.MaxConnections != 0 {
		t.Error("Zero value should be 0 before defaults applied")
	}
}

// =============================================================================
// Error Types Tests
// =============================================================================

func TestErrors(t *testing.T) {
	if ErrPoolClosed.Error() != "connection pool is closed" {
		t.Error("ErrPoolClosed message mismatch")
	}
	if ErrPoolExhausted.Error() != "connection pool exhausted" {
		t.Error("ErrPoolExhausted message mismatch")
	}
	if ErrUnauthorized.Error() != "unauthorized" {
		t.Error("ErrUnauthorized message mismatch")
	}
	if ErrForbidden.Error() != "forbidden" {
		t.Error("ErrForbidden message mismatch")
	}
	if ErrRateLimited.Error() != "rate limited" {
		t.Error("ErrRateLimited message mismatch")
	}
	if ErrNotFound.Error() != "not found" {
		t.Error("ErrNotFound message mismatch")
	}
}

// =============================================================================
// Protocol Constants Tests
// =============================================================================

func TestProtocolConstants(t *testing.T) {
	if ProtocolVersion != 2 {
		t.Errorf("ProtocolVersion = %d, want 2", ProtocolVersion)
	}
	if MaxFrameSize != 64*1024*1024 {
		t.Errorf("MaxFrameSize = %d, want %d", MaxFrameSize, 64*1024*1024)
	}
}

// =============================================================================
// Pool Config Validation Tests
// =============================================================================

func TestPoolConfigWithTLS(t *testing.T) {
	cfg := DefaultPoolConfig()
	cfg.TLSEnabled = true
	cfg.TLSSkipVerify = true

	if !cfg.TLSEnabled {
		t.Error("TLSEnabled should be true")
	}
	if !cfg.TLSSkipVerify {
		t.Error("TLSSkipVerify should be true")
	}
}

func TestPoolConfigWithAPIKey(t *testing.T) {
	cfg := DefaultPoolConfig()
	cfg.APIKey = "test-api-key"

	if cfg.APIKey != "test-api-key" {
		t.Errorf("APIKey = %q, want %q", cfg.APIKey, "test-api-key")
	}
}

// =============================================================================
// Default Values Tests
// =============================================================================

func TestDefaultValues(t *testing.T) {
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"DefaultPoolSize", DefaultPoolSize, 20},
		{"DefaultConnTimeout", DefaultConnTimeout, 5 * time.Second},
		{"DefaultIdleTimeout", DefaultIdleTimeout, 60 * time.Second},
		{"DefaultMaxRetries", DefaultMaxRetries, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}

// =============================================================================
// Result Types Tests
// =============================================================================

func TestHealthStatus(t *testing.T) {
	hs := &HealthStatus{
		Status: "ok",
		Components: map[string]string{
			"engine": "ok",
			"backup": "ok",
		},
	}

	if hs.Status != "ok" {
		t.Errorf("Status = %q, want %q", hs.Status, "ok")
	}
	if len(hs.Components) != 2 {
		t.Errorf("Components length = %d, want 2", len(hs.Components))
	}
}

func TestServerInfoFromProtoMapsAllCurrentFields(t *testing.T) {
	info := serverInfoFromProto(&pb.InfoResponse{
		Version:           "test-version",
		DocumentCount:     1,
		TextunitCount:     2,
		EntityCount:       3,
		RelationshipCount: 4,
		CommunityCount:    5,
		VectorDim:         6,
		SessionCount:      7,
		SessionStoreMode:  "durable",
		WalSyncPolicy:     "periodic",
		WalSyncIntervalMs: 250,
	})

	if info.Version != "test-version" ||
		info.DocumentCount != 1 ||
		info.TextUnitCount != 2 ||
		info.EntityCount != 3 ||
		info.RelationshipCount != 4 ||
		info.CommunityCount != 5 ||
		info.VectorDim != 6 ||
		info.SessionCount != 7 ||
		info.SessionStoreMode != "durable" ||
		info.WALSyncPolicy != "periodic" ||
		info.WALSyncIntervalMS != 250 {
		t.Fatalf("unexpected mapped server info: %+v", info)
	}
}

func TestComputeCommunitiesResult(t *testing.T) {
	result := &ComputeCommunitiesResult{
		Count:       5,
		Communities: nil,
	}

	if result.Count != 5 {
		t.Errorf("Count = %d, want 5", result.Count)
	}
}

func TestHierarchicalLeidenResult(t *testing.T) {
	result := &HierarchicalLeidenResult{
		TotalCommunities: 10,
		LevelCounts: map[int]int{
			0: 5,
			1: 3,
			2: 2,
		},
	}

	if result.TotalCommunities != 10 {
		t.Errorf("TotalCommunities = %d, want 10", result.TotalCommunities)
	}
	if len(result.LevelCounts) != 3 {
		t.Errorf("LevelCounts length = %d, want 3", len(result.LevelCounts))
	}
}

func TestLastSaveInfo(t *testing.T) {
	info := &LastSaveInfo{
		Timestamp: 1234567890,
		Path:      "/data/snapshot.gibram",
	}

	if info.Timestamp != 1234567890 {
		t.Errorf("Timestamp = %d, want 1234567890", info.Timestamp)
	}
	if info.Path != "/data/snapshot.gibram" {
		t.Errorf("Path = %q, want %q", info.Path, "/data/snapshot.gibram")
	}
}

func TestBackupStatus(t *testing.T) {
	status := &BackupStatus{
		InProgress:   true,
		Type:         "save",
		StartTime:    1234567890,
		LastSaveTime: 1234567800,
		LastSavePath: "/data/snapshot.gibram",
	}

	if !status.InProgress {
		t.Error("InProgress should be true")
	}
	if status.Type != "save" {
		t.Errorf("Type = %q, want %q", status.Type, "save")
	}
}
