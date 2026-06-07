// Package types defines session management for GibRAM
package types

import (
	"errors"
	"sync"
	"time"
)

// Quota errors
var (
	ErrEntityQuotaExceeded       = errors.New("entity quota exceeded")
	ErrRelationshipQuotaExceeded = errors.New("relationship quota exceeded")
	ErrDocumentQuotaExceeded     = errors.New("document quota exceeded")
	ErrMemoryQuotaExceeded       = errors.New("memory quota exceeded")
)

// =============================================================================
// Session - Represents an isolated data context
// =============================================================================

// Session represents an isolated user session with its own data and TTL
type Session struct {
	mu sync.RWMutex

	// Identity
	ID         string `json:"id"`          // session identifier (external, application-provided)
	CreatedAt  int64  `json:"created_at"`  // unix timestamp in nanoseconds
	LastAccess int64  `json:"last_access"` // unix timestamp in nanoseconds

	// TTL (session-level only, values in nanoseconds)
	TTL     int64 `json:"ttl,omitempty"`      // absolute TTL in nanoseconds (0 = no expiry)
	IdleTTL int64 `json:"idle_ttl,omitempty"` // idle TTL in nanoseconds (0 = no idle expiry)

	// Resource quotas (0 = unlimited)
	MaxEntities      int   `json:"max_entities,omitempty"`      // max entities per session
	MaxRelationships int   `json:"max_relationships,omitempty"` // max relationships per session
	MaxDocuments     int   `json:"max_documents,omitempty"`     // max documents per session
	MaxMemoryBytes   int64 `json:"max_memory_bytes,omitempty"`  // max memory per session in bytes

	// Current usage (tracked for quota enforcement)
	EntityCount       int   `json:"entity_count"`
	RelationshipCount int   `json:"relationship_count"`
	DocumentCount     int   `json:"document_count"`
	MemoryBytes       int64 `json:"memory_bytes"` // approximate memory usage
}

// NewSession creates a new session with the given ID
func NewSession(id string) *Session {
	now := time.Now().UnixNano()
	return &Session{
		ID:         id,
		CreatedAt:  now,
		LastAccess: now,
	}
}

// Touch updates the last access time
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastAccess = time.Now().UnixNano()
}

// SetTTL sets the absolute TTL for the session
func (s *Session) SetTTL(ttl int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TTL = ttl
}

// SetTTLSeconds sets the absolute TTL in seconds (convenience method)
func (s *Session) SetTTLSeconds(seconds int64) {
	s.SetTTL(seconds * int64(time.Second))
}

// SetIdleTTL sets the idle TTL for the session
func (s *Session) SetIdleTTL(idleTTL int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IdleTTL = idleTTL
}

// SetIdleTTLSeconds sets the idle TTL in seconds (convenience method)
func (s *Session) SetIdleTTLSeconds(seconds int64) {
	s.SetIdleTTL(seconds * int64(time.Second))
}

// SetQuotas sets resource quotas for the session
func (s *Session) SetQuotas(maxEntities, maxRelationships, maxDocuments int, maxMemoryBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MaxEntities = maxEntities
	s.MaxRelationships = maxRelationships
	s.MaxDocuments = maxDocuments
	s.MaxMemoryBytes = maxMemoryBytes
}

// CheckEntityQuota checks if adding count entities would exceed quota
func (s *Session) CheckEntityQuota(count int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.MaxEntities > 0 && s.EntityCount+count > s.MaxEntities {
		return ErrEntityQuotaExceeded
	}
	return nil
}

// CheckRelationshipQuota checks if adding count relationships would exceed quota
func (s *Session) CheckRelationshipQuota(count int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.MaxRelationships > 0 && s.RelationshipCount+count > s.MaxRelationships {
		return ErrRelationshipQuotaExceeded
	}
	return nil
}

// CheckDocumentQuota checks if adding count documents would exceed quota
func (s *Session) CheckDocumentQuota(count int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.MaxDocuments > 0 && s.DocumentCount+count > s.MaxDocuments {
		return ErrDocumentQuotaExceeded
	}
	return nil
}

// CheckMemoryQuota checks if adding bytes of memory would exceed quota
func (s *Session) CheckMemoryQuota(bytes int64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.MaxMemoryBytes > 0 && s.MemoryBytes+bytes > s.MaxMemoryBytes {
		return ErrMemoryQuotaExceeded
	}
	return nil
}

// IncrementEntity increments entity count (call after successful insert)
func (s *Session) IncrementEntity(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EntityCount += count
}

// DecrementEntity decrements entity count (call after delete)
func (s *Session) DecrementEntity(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EntityCount -= count
	if s.EntityCount < 0 {
		s.EntityCount = 0
	}
}

// IncrementRelationship increments relationship count
func (s *Session) IncrementRelationship(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RelationshipCount += count
}

// DecrementRelationship decrements relationship count
func (s *Session) DecrementRelationship(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.RelationshipCount -= count
	if s.RelationshipCount < 0 {
		s.RelationshipCount = 0
	}
}

// IncrementDocument increments document count
func (s *Session) IncrementDocument(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DocumentCount += count
}

// DecrementDocument decrements document count
func (s *Session) DecrementDocument(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.DocumentCount -= count
	if s.DocumentCount < 0 {
		s.DocumentCount = 0
	}
}

// AddMemory adds to memory usage tracking
func (s *Session) AddMemory(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MemoryBytes += bytes
}

// SubMemory subtracts from memory usage tracking
func (s *Session) SubMemory(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MemoryBytes -= bytes
	if s.MemoryBytes < 0 {
		s.MemoryBytes = 0
	}
}

// SetUsage replaces tracked usage counters after restore or rebuild.
func (s *Session) SetUsage(entityCount, relationshipCount, documentCount int, memoryBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EntityCount = entityCount
	s.RelationshipCount = relationshipCount
	s.DocumentCount = documentCount
	s.MemoryBytes = memoryBytes
}

// MemoryUsage returns the current approximate memory usage.
func (s *Session) MemoryUsage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MemoryBytes
}

// QuotaSnapshot returns the configured session quotas.
func (s *Session) QuotaSnapshot() (maxEntities, maxRelationships, maxDocuments int, maxMemoryBytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.MaxEntities, s.MaxRelationships, s.MaxDocuments, s.MaxMemoryBytes
}

// IsExpired checks if the session has expired
func (s *Session) IsExpired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UnixNano()

	// Check absolute TTL
	if s.TTL > 0 && s.CreatedAt+s.TTL < now {
		return true
	}

	// Check idle TTL
	if s.IdleTTL > 0 && s.LastAccess+s.IdleTTL < now {
		return true
	}

	return false
}

// GetExpireAt returns the next expiration time in nanoseconds (0 if no expiry)
func (s *Session) GetExpireAt() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var expireAt int64 = 0

	if s.TTL > 0 {
		expireAt = s.CreatedAt + s.TTL
	}

	if s.IdleTTL > 0 {
		idleExpire := s.LastAccess + s.IdleTTL
		if expireAt == 0 || idleExpire < expireAt {
			expireAt = idleExpire
		}
	}

	return expireAt
}

// GetTTLRemaining returns remaining TTL in nanoseconds (-1 if no expiry)
func (s *Session) GetTTLRemaining() int64 {
	expireAt := s.GetExpireAt()
	if expireAt == 0 {
		return -1 // no expiry
	}

	remaining := expireAt - time.Now().UnixNano()
	if remaining < 0 {
		return 0
	}
	return remaining
}

// GetInfo returns a read-only copy of session metadata
func (s *Session) GetInfo() SessionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SessionInfo{
		ID:         s.ID,
		CreatedAt:  s.CreatedAt,
		LastAccess: s.LastAccess,
		TTL:        s.TTL,
		IdleTTL:    s.IdleTTL,
	}
}

// =============================================================================
// SessionInfo - Read-only session metadata for responses
// =============================================================================

// SessionInfo contains read-only session information
type SessionInfo struct {
	ID                string `json:"id"`
	CreatedAt         int64  `json:"created_at"`
	LastAccess        int64  `json:"last_access"`
	TTL               int64  `json:"ttl"`
	IdleTTL           int64  `json:"idle_ttl"`
	DocumentCount     int    `json:"document_count"`
	TextUnitCount     int    `json:"text_unit_count"`
	EntityCount       int    `json:"entity_count"`
	RelationshipCount int    `json:"relationship_count"`
	CommunityCount    int    `json:"community_count"`
	MemoryBytes       int64  `json:"memory_bytes"`
	MaxEntities       int    `json:"max_entities,omitempty"`
	MaxRelationships  int    `json:"max_relationships,omitempty"`
	MaxDocuments      int    `json:"max_documents,omitempty"`
	MaxMemoryBytes    int64  `json:"max_memory_bytes,omitempty"`
}
