// Package types defines all core data structures for GibRAM
package types

import (
	"sync/atomic"
	"time"
)

// =============================================================================
// ID Generator
// =============================================================================

type IDGenerator struct {
	documentCounter     uint64
	textUnitCounter     uint64
	entityCounter       uint64
	relationshipCounter uint64
	communityCounter    uint64
	queryCounter        uint64
}

func NewIDGenerator() *IDGenerator {
	return &IDGenerator{}
}

func (g *IDGenerator) NextDocumentID() uint64 {
	return atomic.AddUint64(&g.documentCounter, 1)
}

func (g *IDGenerator) NextTextUnitID() uint64 {
	return atomic.AddUint64(&g.textUnitCounter, 1)
}

func (g *IDGenerator) NextEntityID() uint64 {
	return atomic.AddUint64(&g.entityCounter, 1)
}

func (g *IDGenerator) NextRelationshipID() uint64 {
	return atomic.AddUint64(&g.relationshipCounter, 1)
}

func (g *IDGenerator) NextCommunityID() uint64 {
	return atomic.AddUint64(&g.communityCounter, 1)
}

func (g *IDGenerator) NextQueryID() uint64 {
	return atomic.AddUint64(&g.queryCounter, 1)
}

// SetCounters restores counters from snapshot
func (g *IDGenerator) SetCounters(doc, tu, ent, rel, comm, query uint64) {
	atomic.StoreUint64(&g.documentCounter, doc)
	atomic.StoreUint64(&g.textUnitCounter, tu)
	atomic.StoreUint64(&g.entityCounter, ent)
	atomic.StoreUint64(&g.relationshipCounter, rel)
	atomic.StoreUint64(&g.communityCounter, comm)
	atomic.StoreUint64(&g.queryCounter, query)
}

// GetCounters returns current counter values for snapshot
func (g *IDGenerator) GetCounters() (doc, tu, ent, rel, comm, query uint64) {
	return atomic.LoadUint64(&g.documentCounter),
		atomic.LoadUint64(&g.textUnitCounter),
		atomic.LoadUint64(&g.entityCounter),
		atomic.LoadUint64(&g.relationshipCounter),
		atomic.LoadUint64(&g.communityCounter),
		atomic.LoadUint64(&g.queryCounter)
}

// CurrentDocumentID returns the current document counter value
func (g *IDGenerator) CurrentDocumentID() uint64 {
	return atomic.LoadUint64(&g.documentCounter)
}

// CurrentTextUnitID returns the current text unit counter value
func (g *IDGenerator) CurrentTextUnitID() uint64 {
	return atomic.LoadUint64(&g.textUnitCounter)
}

// CurrentEntityID returns the current entity counter value
func (g *IDGenerator) CurrentEntityID() uint64 {
	return atomic.LoadUint64(&g.entityCounter)
}

// CurrentRelationshipID returns the current relationship counter value
func (g *IDGenerator) CurrentRelationshipID() uint64 {
	return atomic.LoadUint64(&g.relationshipCounter)
}

// CurrentCommunityID returns the current community counter value
func (g *IDGenerator) CurrentCommunityID() uint64 {
	return atomic.LoadUint64(&g.communityCounter)
}

// RestoreState restores the ID generator state from a map
func (g *IDGenerator) RestoreState(state map[string]uint64) {
	if v, ok := state["document"]; ok {
		atomic.StoreUint64(&g.documentCounter, v)
	}
	if v, ok := state["textunit"]; ok {
		atomic.StoreUint64(&g.textUnitCounter, v)
	}
	if v, ok := state["entity"]; ok {
		atomic.StoreUint64(&g.entityCounter, v)
	}
	if v, ok := state["relationship"]; ok {
		atomic.StoreUint64(&g.relationshipCounter, v)
	}
	if v, ok := state["community"]; ok {
		atomic.StoreUint64(&g.communityCounter, v)
	}
}

// =============================================================================
// Document - Metadata for uploaded files
// =============================================================================

type DocumentStatus string

const (
	DocStatusUploaded   DocumentStatus = "uploaded"
	DocStatusProcessing DocumentStatus = "processing"
	DocStatusReady      DocumentStatus = "ready"
)

type Document struct {
	ID         uint64            `json:"id"`
	ExternalID string            `json:"external_id"` // "doc-uuid-001"
	Filename   string            `json:"filename"`    // "kebijakan_bi_2024.pdf"
	Status     DocumentStatus    `json:"status"`
	Attrs      map[string]string `json:"attrs,omitempty"`
	CreatedAt  int64             `json:"created_at"`
}

// NewDocument creates a new document with auto-set timestamp
func NewDocument(id uint64, extID, filename string) *Document {
	return &Document{
		ID:         id,
		ExternalID: extID,
		Filename:   filename,
		Status:     DocStatusUploaded,
		CreatedAt:  time.Now().Unix(),
	}
}

// =============================================================================
// TextUnit (Chunk) - Text segments for retrieval
// =============================================================================

type TextUnit struct {
	ID         uint64   `json:"id"`
	ExternalID string   `json:"external_id"` // "chunk-001"
	DocumentID uint64   `json:"document_id"` // parent document
	Content    string   `json:"content"`     // full text
	EntityIDs  []uint64 `json:"entity_ids"`  // linked entities
	TokenCount int      `json:"token_count"`
	CreatedAt  int64    `json:"created_at"`
}

// NewTextUnit creates a new text unit with auto-set timestamp
func NewTextUnit(id uint64, extID string, docID uint64, content string, tokenCount int) *TextUnit {
	return &TextUnit{
		ID:         id,
		ExternalID: extID,
		DocumentID: docID,
		Content:    content,
		TokenCount: tokenCount,
		CreatedAt:  time.Now().Unix(),
	}
}

func (t *TextUnit) AddEntityID(entityID uint64) {
	for _, id := range t.EntityIDs {
		if id == entityID {
			return
		}
	}
	t.EntityIDs = append(t.EntityIDs, entityID)
}

func (t *TextUnit) RemoveEntityID(entityID uint64) {
	for i, id := range t.EntityIDs {
		if id == entityID {
			t.EntityIDs = append(t.EntityIDs[:i], t.EntityIDs[i+1:]...)
			return
		}
	}
}

// =============================================================================
// Entity - Extracted entities with semantic description
// =============================================================================

type Entity struct {
	ID          uint64            `json:"id"`
	ExternalID  string            `json:"external_id"` // "ent-001"
	Title       string            `json:"title"`       // "BANK INDONESIA" (uppercase for dedup)
	Type        string            `json:"type"`        // "organization", "person", "location", "concept"
	Description string            `json:"description"` // semantic content for embedding
	Attrs       map[string]string `json:"attrs,omitempty"`
	TextUnitIDs []uint64          `json:"text_unit_ids"` // linked chunks
	CreatedAt   int64             `json:"created_at"`
}

// NewEntity creates a new entity with auto-set timestamp
func NewEntity(id uint64, extID, title, entType, description string) *Entity {
	return &Entity{
		ID:          id,
		ExternalID:  extID,
		Title:       title,
		Type:        entType,
		Description: description,
		CreatedAt:   time.Now().Unix(),
	}
}

func (e *Entity) AddTextUnitID(tuID uint64) {
	for _, id := range e.TextUnitIDs {
		if id == tuID {
			return
		}
	}
	e.TextUnitIDs = append(e.TextUnitIDs, tuID)
}

func (e *Entity) RemoveTextUnitID(tuID uint64) {
	for i, id := range e.TextUnitIDs {
		if id == tuID {
			e.TextUnitIDs = append(e.TextUnitIDs[:i], e.TextUnitIDs[i+1:]...)
			return
		}
	}
}

// =============================================================================
// Relationship - Edge between entities with description (not embedded)
// =============================================================================

type Relationship struct {
	ID          uint64   `json:"id"`
	ExternalID  string   `json:"external_id,omitempty"`
	SourceID    uint64   `json:"source_id"`   // entity ID
	TargetID    uint64   `json:"target_id"`   // entity ID
	Type        string   `json:"type"`        // "PRESIDENT_OF", "BORN_IN"
	Description string   `json:"description"` // for explain (not embedded)
	Weight      float32  `json:"weight"`
	TextUnitIDs []uint64 `json:"text_unit_ids"` // provenance chunks
	CreatedAt   int64    `json:"created_at"`
}

// NewRelationship creates a new relationship with auto-set timestamp
func NewRelationship(id uint64, extID string, sourceID, targetID uint64, relType, description string, weight float32) *Relationship {
	return &Relationship{
		ID:          id,
		ExternalID:  extID,
		SourceID:    sourceID,
		TargetID:    targetID,
		Type:        relType,
		Description: description,
		Weight:      weight,
		CreatedAt:   time.Now().Unix(),
	}
}

func (r *Relationship) AddTextUnitID(tuID uint64) {
	for _, id := range r.TextUnitIDs {
		if id == tuID {
			return
		}
	}
	r.TextUnitIDs = append(r.TextUnitIDs, tuID)
}

// =============================================================================
// Community - Result of Leiden clustering with LLM summary
// =============================================================================

type Community struct {
	ID              uint64   `json:"id"`
	ExternalID      string   `json:"external_id,omitempty"`
	Title           string   `json:"title"` // "Bank Indonesia, Perry Warjiyo, Suku Bunga"
	Level           int      `json:"level"` // hierarchy level (0 = leaf)
	EntityIDs       []uint64 `json:"entity_ids"`
	RelationshipIDs []uint64 `json:"relationship_ids"`
	Summary         string   `json:"summary"`      // short summary for embedding
	FullContent     string   `json:"full_content"` // full report
	CreatedAt       int64    `json:"created_at"`
}

// NewCommunity creates a new community with auto-set timestamp
func NewCommunity(id uint64, extID, title, summary, fullContent string, level int, entityIDs, relIDs []uint64) *Community {
	return &Community{
		ID:              id,
		ExternalID:      extID,
		Title:           title,
		Level:           level,
		EntityIDs:       entityIDs,
		RelationshipIDs: relIDs,
		Summary:         summary,
		FullContent:     fullContent,
		CreatedAt:       time.Now().Unix(),
	}
}

// =============================================================================
// Query Types
// =============================================================================

type SearchType string

const (
	SearchTypeTextUnit  SearchType = "textunit"
	SearchTypeEntity    SearchType = "entity"
	SearchTypeCommunity SearchType = "community"
)

type QuerySpec struct {
	QueryVector    []float32    `json:"query_vector"`
	SearchTypes    []SearchType `json:"search_types"` // which indices to search
	TopK           int          `json:"top_k"`
	KHops          int          `json:"k_hops"`
	MaxEntities    int          `json:"max_entities"`
	MaxTextUnits   int          `json:"max_text_units"`
	MaxCommunities int          `json:"max_communities"`
	DeadlineMs     int          `json:"deadline_ms"`
}

func DefaultQuerySpec() QuerySpec {
	return QuerySpec{
		SearchTypes:    []SearchType{SearchTypeTextUnit, SearchTypeEntity, SearchTypeCommunity},
		TopK:           10,
		KHops:          2,
		MaxEntities:    50,
		MaxTextUnits:   10,
		MaxCommunities: 5,
		DeadlineMs:     100,
	}
}

// =============================================================================
// Query Results
// =============================================================================

type TextUnitResult struct {
	TextUnit   *TextUnit `json:"text_unit"`
	Score      float32   `json:"score"`
	Similarity float32   `json:"similarity"`
	Hop        int       `json:"hop"`
}

type EntityResult struct {
	Entity     *Entity `json:"entity"`
	Score      float32 `json:"score"`
	Similarity float32 `json:"similarity"`
	Hop        int     `json:"hop"`
}

type CommunityResult struct {
	Community  *Community `json:"community"`
	Score      float32    `json:"score"`
	Similarity float32    `json:"similarity"`
}

type RelationshipResult struct {
	Relationship *Relationship `json:"relationship"`
	SourceTitle  string        `json:"source_title"`
	TargetTitle  string        `json:"target_title"`
}

type QueryStats struct {
	TextUnitsSearched   int      `json:"text_units_searched"`
	EntitiesSearched    int      `json:"entities_searched"`
	CommunitiesSearched int      `json:"communities_searched"`
	EdgesScanned        int      `json:"edges_scanned"`
	DurationMicros      int64    `json:"duration_micros"`
	SkippedSeedIndexes  []string `json:"skipped_seed_indexes"`
}

type ContextPack struct {
	QueryID       uint64               `json:"query_id"`
	TextUnits     []TextUnitResult     `json:"text_units"`
	Entities      []EntityResult       `json:"entities"`
	Communities   []CommunityResult    `json:"communities"`
	Relationships []RelationshipResult `json:"relationships"`
	Stats         QueryStats           `json:"stats"`
}

// =============================================================================
// Explain Types
// =============================================================================

type SeedInfo struct {
	Type       SearchType `json:"type"`
	ID         uint64     `json:"id"`
	ExternalID string     `json:"external_id"`
	Similarity float32    `json:"similarity"`
	LinkedIDs  []uint64   `json:"linked_ids"`
}

type TraversalStep struct {
	FromEntityID   uint64  `json:"from_entity_id"`
	ToEntityID     uint64  `json:"to_entity_id"`
	RelationshipID uint64  `json:"relationship_id"`
	RelType        string  `json:"rel_type"`
	Weight         float32 `json:"weight"`
	Hop            int     `json:"hop"`
	Cumulative     float32 `json:"cumulative_score"`
}

type ExplainPack struct {
	QueryID   uint64          `json:"query_id"`
	Seeds     []SeedInfo      `json:"seeds"`
	Traversal []TraversalStep `json:"traversal"`
}

// =============================================================================
// Server Info
// =============================================================================

type ServerInfo struct {
	Version             string `json:"version"`
	DocumentCount       int    `json:"document_count"`
	TextUnitCount       int    `json:"text_unit_count"`
	EntityCount         int    `json:"entity_count"`
	RelationshipCount   int    `json:"relationship_count"`
	CommunityCount      int    `json:"community_count"`
	VectorDim           int    `json:"vector_dim"`
	SessionCount        int    `json:"session_count"`
	SessionStoreMode    string `json:"session_store_mode"`
	WALSyncPolicy       string `json:"wal_sync_policy,omitempty"`
	WALSyncIntervalMS   int64  `json:"wal_sync_interval_ms,omitempty"`
	SessionStoreHealthy bool   `json:"session_store_healthy"`
}

// =============================================================================
// Bulk Operation Input Types
// =============================================================================

// BulkDocumentInput represents input for bulk document creation.
type BulkDocumentInput struct {
	ExternalID string
	Filename   string
}

// BulkTextUnitInput represents input for bulk text unit creation.
type BulkTextUnitInput struct {
	ExternalID string
	DocumentID uint64
	Content    string
	Embedding  []float32
	TokenCount int
}

// BulkEntityInput represents input for bulk entity creation.
type BulkEntityInput struct {
	ExternalID  string
	Title       string
	Type        string
	Description string
	Embedding   []float32
}

// BulkRelationshipInput represents input for bulk relationship creation.
type BulkRelationshipInput struct {
	ExternalID  string
	SourceID    uint64
	TargetID    uint64
	Type        string
	Description string
	Weight      float32
}
