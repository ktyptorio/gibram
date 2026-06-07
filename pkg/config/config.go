// Package config provides configuration management for GibRAM server
package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// =============================================================================
// Path Security
// =============================================================================

// ValidatePath ensures a path is safe and within allowed boundaries
// Returns the cleaned absolute path or error
func ValidatePath(basePath, targetPath string) (string, error) {
	// Clean and resolve base path
	absBase, err := filepath.Abs(basePath)
	if err != nil {
		return "", fmt.Errorf("invalid base path: %w", err)
	}
	absBase = filepath.Clean(absBase)

	// Clean and resolve target path
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return "", fmt.Errorf("invalid target path: %w", err)
	}
	absTarget = filepath.Clean(absTarget)

	// Check for path traversal - target must be within base
	if !strings.HasPrefix(absTarget, absBase+string(filepath.Separator)) && absTarget != absBase {
		return "", fmt.Errorf("path escapes base directory: %s", targetPath)
	}

	realBase, err := filepath.EvalSymlinks(absBase)
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("resolve base path: %w", err)
	}
	if os.IsNotExist(err) {
		realBase = absBase
	}

	realTarget, err := filepath.EvalSymlinks(absTarget)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("resolve target path: %w", err)
		}
		existingParent := filepath.Dir(absTarget)
		missingSuffix := filepath.Base(absTarget)
		for {
			realParent, parentErr := filepath.EvalSymlinks(existingParent)
			if parentErr == nil {
				realTarget = filepath.Join(realParent, missingSuffix)
				break
			}
			if !os.IsNotExist(parentErr) {
				return "", fmt.Errorf("resolve target parent: %w", parentErr)
			}
			nextParent := filepath.Dir(existingParent)
			if nextParent == existingParent {
				realTarget = absTarget
				break
			}
			missingSuffix = filepath.Join(filepath.Base(existingParent), missingSuffix)
			existingParent = nextParent
		}
	}

	// Verify the real paths still maintain containment
	if !strings.HasPrefix(realTarget, realBase+string(filepath.Separator)) && realTarget != realBase {
		return "", fmt.Errorf("symlink escapes base directory")
	}

	return absTarget, nil
}

// SanitizeDataDir validates and sanitizes the data directory path
func SanitizeDataDir(dataDir string) (string, error) {
	// Get absolute path
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("invalid data directory: %w", err)
	}

	// Clean the path
	cleanPath := filepath.Clean(absPath)

	// Check for suspicious patterns
	if strings.Contains(cleanPath, "..") {
		return "", fmt.Errorf("data directory contains path traversal: %s", dataDir)
	}

	// Don't allow root or system directories
	dangerousPaths := []string{"/", "/etc", "/usr", "/bin", "/sbin", "/var", "/tmp", "/root"}
	for _, dangerous := range dangerousPaths {
		if cleanPath == dangerous || strings.HasPrefix(cleanPath, dangerous+"/") {
			// Allow /var/lib/gmr etc but not /var directly
			if cleanPath == dangerous {
				return "", fmt.Errorf("data directory cannot be system path: %s", cleanPath)
			}
		}
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(cleanPath, 0750); err != nil {
		return "", fmt.Errorf("cannot create data directory: %w", err)
	}

	return cleanPath, nil
}

// =============================================================================
// Configuration Structures
// =============================================================================

// Config is the main configuration structure
type Config struct {
	Server       ServerConfig       `yaml:"server"`
	SessionStore SessionStoreConfig `yaml:"session_store"`
	TLS          TLSConfig          `yaml:"tls"`
	Auth         AuthConfig         `yaml:"auth"`
	Security     SecurityConfig     `yaml:"security"`
	Logging      LoggingConfig      `yaml:"logging"`
}

// ServerConfig contains server settings
type ServerConfig struct {
	Addr      string `yaml:"addr"`
	DataDir   string `yaml:"data_dir"`
	VectorDim int    `yaml:"vector_dim"`
}

// SessionStoreConfig contains session store durability settings.
type SessionStoreConfig struct {
	Mode                 string        `yaml:"mode"`                    // ephemeral, durable
	WALDir               string        `yaml:"wal_dir"`                 // optional; defaults under data_dir
	WALSyncPolicy        string        `yaml:"wal_sync_policy"`         // every_write, periodic, never
	WALSyncInterval      time.Duration `yaml:"wal_sync_interval"`       // used by periodic policy
	SnapshotDir          string        `yaml:"snapshot_dir"`            // optional; defaults under data_dir
	SnapshotInterval     time.Duration `yaml:"snapshot_interval"`       // 0 disables interval snapshots
	SnapshotWALSizeBytes int64         `yaml:"snapshot_wal_size_bytes"` // 0 disables WAL-growth snapshots
}

// TLSConfig contains TLS settings
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	AutoCert bool   `yaml:"auto_cert"` // Auto-generate self-signed cert
}

// AuthConfig contains authentication settings
type AuthConfig struct {
	Keys []APIKeyConfig `yaml:"keys"`
}

// APIKeyConfig represents an API key
type APIKeyConfig struct {
	ID          string   `yaml:"id"`
	Key         string   `yaml:"key"`         // Plain text in config
	KeyHash     string   `yaml:"key_hash"`    // Or bcrypt hash (if Key is empty)
	Permissions []string `yaml:"permissions"` // admin, write, read
	ExpiresAt   string   `yaml:"expires_at"`  // Optional: RFC3339 format
}

// SecurityConfig contains security settings
type SecurityConfig struct {
	MaxFrameSize            int           `yaml:"max_frame_size"`    // Max frame size in bytes
	MaxContentBytes         int           `yaml:"max_content_bytes"` // Max text content per write
	MaxMemoryBytes          int64         `yaml:"max_memory_bytes"`  // Max admitted session memory bytes
	MaxSessionDocuments     int           `yaml:"max_session_documents"`
	MaxSessionEntities      int           `yaml:"max_session_entities"`
	MaxSessionRelationships int           `yaml:"max_session_relationships"`
	RateLimit               int           `yaml:"rate_limit"`       // Requests per second per key
	RateBurst               int           `yaml:"rate_burst"`       // Burst allowance
	IdleTimeout             time.Duration `yaml:"idle_timeout"`     // Idle connection timeout
	UnauthTimeout           time.Duration `yaml:"unauth_timeout"`   // Timeout for unauthenticated
	MaxConnsPerIP           int           `yaml:"max_conns_per_ip"` // Max connections per IP
}

// LoggingConfig contains logging settings
type LoggingConfig struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json, text
	Output string `yaml:"output"` // stdout, file
	File   string `yaml:"file"`   // Log file path if output=file
}

// =============================================================================
// Default Configuration
// =============================================================================

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Addr:      ":6161",
			DataDir:   "./data",
			VectorDim: 1536,
		},
		SessionStore: SessionStoreConfig{
			Mode:            "ephemeral",
			WALSyncPolicy:   "every_write",
			WALSyncInterval: time.Second,
		},
		TLS: TLSConfig{
			CertFile: "",
			KeyFile:  "",
			AutoCert: true, // Auto-generate for dev
		},
		Auth: AuthConfig{
			Keys: []APIKeyConfig{},
		},
		Security: SecurityConfig{
			MaxFrameSize:            4 * 1024 * 1024, // 4MB
			MaxContentBytes:         1 * 1024 * 1024, // 1MB text fields
			MaxMemoryBytes:          0,
			MaxSessionDocuments:     0,
			MaxSessionEntities:      0,
			MaxSessionRelationships: 0,
			RateLimit:               1000, // 1000 req/s
			RateBurst:               100,
			IdleTimeout:             300 * time.Second,
			UnauthTimeout:           10 * time.Second,
			MaxConnsPerIP:           50,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
			Output: "stdout",
			File:   "",
		},
	}
}

// =============================================================================
// Configuration Loading
// =============================================================================

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	// Validate and sanitize data directory
	sanitizedDir, err := SanitizeDataDir(cfg.Server.DataDir)
	if err != nil {
		return nil, fmt.Errorf("invalid data_dir: %w", err)
	}
	cfg.Server.DataDir = sanitizedDir

	if err := cfg.NormalizeSessionStore(); err != nil {
		return nil, err
	}

	// Process API keys - hash plain text keys
	for i := range cfg.Auth.Keys {
		key := &cfg.Auth.Keys[i]
		if key.Key != "" && key.KeyHash == "" {
			hash, err := bcrypt.GenerateFromPassword([]byte(key.Key), bcrypt.DefaultCost)
			if err != nil {
				return nil, fmt.Errorf("hash api key %s: %w", key.ID, err)
			}
			key.KeyHash = string(hash)
			// Clear plain text key from memory
			key.Key = ""
		}
	}

	return cfg, nil
}

// NormalizeSessionStore validates session store configuration and fills durable defaults.
func (cfg *Config) NormalizeSessionStore() error {
	mode := strings.ToLower(strings.TrimSpace(cfg.SessionStore.Mode))
	if mode == "" {
		mode = "ephemeral"
	}
	switch mode {
	case "ephemeral", "durable":
		cfg.SessionStore.Mode = mode
	default:
		return fmt.Errorf("invalid session_store.mode %q: expected ephemeral or durable", cfg.SessionStore.Mode)
	}

	policy := strings.ToLower(strings.TrimSpace(cfg.SessionStore.WALSyncPolicy))
	if policy == "" {
		policy = "every_write"
	}
	switch policy {
	case "every_write", "periodic", "never":
		cfg.SessionStore.WALSyncPolicy = policy
	default:
		return fmt.Errorf("invalid session_store.wal_sync_policy %q: expected every_write, periodic, or never", cfg.SessionStore.WALSyncPolicy)
	}

	if cfg.SessionStore.WALSyncInterval <= 0 {
		cfg.SessionStore.WALSyncInterval = time.Second
	}
	if cfg.SessionStore.Mode == "durable" && cfg.SessionStore.WALDir == "" {
		cfg.SessionStore.WALDir = filepath.Join(cfg.Server.DataDir, "session_wal")
	}
	if cfg.SessionStore.Mode == "durable" && cfg.SessionStore.SnapshotDir == "" {
		cfg.SessionStore.SnapshotDir = filepath.Join(cfg.Server.DataDir, "session_snapshots")
	}
	return nil
}

// SaveConfig saves configuration to a YAML file
func SaveConfig(cfg *Config, path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}

	return nil
}

// =============================================================================
// API Key Management
// =============================================================================

// Permission constants
const (
	PermAdmin = "admin"
	PermWrite = "write"
	PermRead  = "read"
)

// APIKeyStore manages API keys in memory
type APIKeyStore struct {
	keys map[string]*APIKey // key hash -> APIKey
}

// APIKey represents a validated API key
type APIKey struct {
	ID          string
	Hash        string
	Permissions map[string]bool
	ExpiresAt   time.Time
}

// NewAPIKeyStore creates a new API key store from config
func NewAPIKeyStore(cfg *AuthConfig) (*APIKeyStore, error) {
	store := &APIKeyStore{
		keys: make(map[string]*APIKey),
	}

	for _, keyCfg := range cfg.Keys {
		if keyCfg.KeyHash == "" {
			continue
		}

		apiKey := &APIKey{
			ID:          keyCfg.ID,
			Hash:        keyCfg.KeyHash,
			Permissions: make(map[string]bool),
		}

		for _, perm := range keyCfg.Permissions {
			apiKey.Permissions[perm] = true
		}

		if keyCfg.ExpiresAt != "" {
			t, err := time.Parse(time.RFC3339, keyCfg.ExpiresAt)
			if err != nil {
				return nil, fmt.Errorf("parse expiry for key %s: %w", keyCfg.ID, err)
			}
			apiKey.ExpiresAt = t
		}

		store.keys[keyCfg.KeyHash] = apiKey
	}

	return store, nil
}

// Validate validates an API key and returns the key info if valid
func (s *APIKeyStore) Validate(plainKey string) (*APIKey, error) {
	// Check each stored key hash
	for _, apiKey := range s.keys {
		if err := bcrypt.CompareHashAndPassword([]byte(apiKey.Hash), []byte(plainKey)); err == nil {
			// Key matches
			if !apiKey.ExpiresAt.IsZero() && time.Now().After(apiKey.ExpiresAt) {
				return nil, fmt.Errorf("api key expired")
			}
			return apiKey, nil
		}
	}
	return nil, fmt.Errorf("invalid api key")
}

// HasPermission checks if a key has a specific permission
func (k *APIKey) HasPermission(perm string) bool {
	// Admin has all permissions
	if k.Permissions[PermAdmin] {
		return true
	}
	// Write includes read
	if perm == PermRead && k.Permissions[PermWrite] {
		return true
	}
	return k.Permissions[perm]
}

// =============================================================================
// Key Generation Utilities
// =============================================================================

// GenerateAPIKey generates a new random API key
func GenerateAPIKey() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "gibram_" + hex.EncodeToString(bytes), nil
}

// HashAPIKey creates a bcrypt hash of an API key
func HashAPIKey(key string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(key), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// =============================================================================
// CLI Override Support
// =============================================================================

// CLIOverrides contains command-line overrides
type CLIOverrides struct {
	Addr      string
	DataDir   string
	VectorDim int
	Insecure  bool // Disable TLS + Auth (dev only)
	LogLevel  string
}

// ApplyOverrides applies CLI overrides to config
func (cfg *Config) ApplyOverrides(overrides CLIOverrides) {
	if overrides.Addr != "" {
		cfg.Server.Addr = overrides.Addr
	}
	if overrides.DataDir != "" {
		cfg.Server.DataDir = overrides.DataDir
	}
	if overrides.VectorDim > 0 {
		cfg.Server.VectorDim = overrides.VectorDim
	}
	if overrides.LogLevel != "" {
		cfg.Logging.Level = overrides.LogLevel
	}
	if cfg.SessionStore.Mode == "" {
		cfg.SessionStore.Mode = "ephemeral"
	}
}

// IsInsecure returns true if running in insecure mode
func (cfg *Config) IsInsecure(insecureFlag bool) bool {
	return insecureFlag
}

// HasTLS returns true if TLS is configured (either via cert files or auto-cert)
func (cfg *Config) HasTLS() bool {
	return (cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "") || cfg.TLS.AutoCert
}

// HasAuth returns true if authentication is configured
func (cfg *Config) HasAuth() bool {
	return len(cfg.Auth.Keys) > 0
}
