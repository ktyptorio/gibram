// Package store provides session-based storage for GibRAM with data isolation
package store

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/gibram-io/gibram/pkg/types"
	"github.com/gibram-io/gibram/pkg/vector"
)

// =============================================================================
// SessionStore - Partitioned storage per session
// =============================================================================

// SessionStore holds all data for a single session with isolated indexes
type SessionStore struct {
	mu sync.RWMutex

	// Session metadata
	session *types.Session

	// ID Generator (per-session)
	idGen *types.IDGenerator

	// Data stores
	documents     map[uint64]*types.Document
	docByExtID    map[string]uint64
	docByFilename map[string][]uint64

	textUnits map[uint64]*types.TextUnit
	tuByExtID map[string]uint64
	tuByDocID map[uint64][]uint64

	entities   map[uint64]*types.Entity
	entByExtID map[string]uint64
	entByTitle map[string]uint64

	relationships     map[uint64]*types.Relationship
	relByExtID        map[string]uint64
	relBySourceTarget map[string]uint64
	outEdges          map[uint64][]uint64
	inEdges           map[uint64][]uint64

	communities map[uint64]*types.Community
	commByExtID map[string]uint64
	commByLevel map[int][]uint64

	// Vector indices (per-session, lazy initialized)
	textUnitIndex  vector.Index
	entityIndex    vector.Index
	communityIndex vector.Index
	vectorDim      int

	// Canonical embeddings. Vector indexes are derived from these maps.
	textUnitEmbeddings  map[uint64][]float32
	entityEmbeddings    map[uint64][]float32
	communityEmbeddings map[uint64][]float32
}

func requireExternalID(kind, extID string) error {
	if strings.TrimSpace(extID) == "" {
		return fmt.Errorf("%s external_id is required", kind)
	}
	return nil
}

func (s *SessionStore) validateOptionalEmbedding(kind string, embedding []float32) error {
	if len(embedding) == 0 {
		return nil
	}
	if len(embedding) != s.vectorDim {
		return fmt.Errorf("%s embedding dimension mismatch: expected %d, got %d", kind, s.vectorDim, len(embedding))
	}
	for i, value := range embedding {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return fmt.Errorf("%s embedding contains non-finite value at index %d", kind, i)
		}
	}
	return nil
}

// NewSessionStore creates a new session store
func NewSessionStore(sessionID string, vectorDim int) *SessionStore {
	return &SessionStore{
		session:   types.NewSession(sessionID),
		idGen:     types.NewIDGenerator(),
		vectorDim: vectorDim,

		// Documents
		documents:     make(map[uint64]*types.Document),
		docByExtID:    make(map[string]uint64),
		docByFilename: make(map[string][]uint64),

		// TextUnits
		textUnits: make(map[uint64]*types.TextUnit),
		tuByExtID: make(map[string]uint64),
		tuByDocID: make(map[uint64][]uint64),

		// Entities
		entities:   make(map[uint64]*types.Entity),
		entByExtID: make(map[string]uint64),
		entByTitle: make(map[string]uint64),

		// Relationships
		relationships:     make(map[uint64]*types.Relationship),
		relByExtID:        make(map[string]uint64),
		relBySourceTarget: make(map[string]uint64),
		outEdges:          make(map[uint64][]uint64),
		inEdges:           make(map[uint64][]uint64),

		// Communities
		communities: make(map[uint64]*types.Community),
		commByExtID: make(map[string]uint64),
		commByLevel: make(map[int][]uint64),

		textUnitEmbeddings:  make(map[uint64][]float32),
		entityEmbeddings:    make(map[uint64][]float32),
		communityEmbeddings: make(map[uint64][]float32),
	}
}

// =============================================================================
// Session Management
// =============================================================================

// GetSession returns session metadata
func (s *SessionStore) GetSession() *types.Session {
	return s.session
}

// GetSessionID returns the session ID
func (s *SessionStore) GetSessionID() string {
	return s.session.ID
}

// Touch updates session last access time
func (s *SessionStore) Touch() {
	s.session.Touch()
}

// IsExpired checks if session has expired
func (s *SessionStore) IsExpired() bool {
	return s.session.IsExpired()
}

// SetTTL sets session absolute TTL
func (s *SessionStore) SetTTL(ttl int64) {
	s.session.SetTTL(ttl)
}

// SetIdleTTL sets session idle TTL
func (s *SessionStore) SetIdleTTL(idleTTL int64) {
	s.session.SetIdleTTL(idleTTL)
}

func (s *SessionStore) SetQuotas(maxEntities, maxRelationships, maxDocuments int, maxMemoryBytes int64) {
	s.session.SetQuotas(maxEntities, maxRelationships, maxDocuments, maxMemoryBytes)
}

func estimateStringsMemory(values ...string) int64 {
	var total int64
	for _, value := range values {
		total += int64(len(value))
	}
	return total
}

func estimateEmbeddingMemory(embedding []float32) int64 {
	return int64(len(embedding) * 4)
}

func (s *SessionStore) estimateDocumentMemory(extID, filename string) int64 {
	return 128 + estimateStringsMemory(extID, filename)
}

func (s *SessionStore) estimateDocumentObject(doc *types.Document) int64 {
	if doc == nil {
		return 0
	}
	return s.estimateDocumentMemory(doc.ExternalID, doc.Filename)
}

func (s *SessionStore) estimateTextUnitMemory(extID, content string, embedding []float32) int64 {
	return 160 + estimateStringsMemory(extID, content) + estimateEmbeddingMemory(embedding)
}

func (s *SessionStore) estimateTextUnitObject(tu *types.TextUnit, embedding []float32) int64 {
	if tu == nil {
		return 0
	}
	return s.estimateTextUnitMemory(tu.ExternalID, tu.Content, embedding)
}

func (s *SessionStore) estimateEntityMemory(extID, title, entType, description string, embedding []float32) int64 {
	return 192 + estimateStringsMemory(extID, title, entType, description) + estimateEmbeddingMemory(embedding)
}

func (s *SessionStore) estimateEntityObject(ent *types.Entity, embedding []float32) int64 {
	if ent == nil {
		return 0
	}
	return s.estimateEntityMemory(ent.ExternalID, ent.Title, ent.Type, ent.Description, embedding)
}

func (s *SessionStore) estimateRelationshipMemory(extID, relType, description string) int64 {
	return 160 + estimateStringsMemory(extID, relType, description)
}

func (s *SessionStore) estimateRelationshipObject(rel *types.Relationship) int64 {
	if rel == nil {
		return 0
	}
	return s.estimateRelationshipMemory(rel.ExternalID, rel.Type, rel.Description)
}

func (s *SessionStore) estimateCommunityMemory(extID, title, summary, fullContent string, entityIDs, relIDs []uint64, embedding []float32) int64 {
	return 192 + estimateStringsMemory(extID, title, summary, fullContent) + int64((len(entityIDs)+len(relIDs))*8) + estimateEmbeddingMemory(embedding)
}

func (s *SessionStore) estimateCommunityObject(comm *types.Community, embedding []float32) int64 {
	if comm == nil {
		return 0
	}
	return s.estimateCommunityMemory(comm.ExternalID, comm.Title, comm.Summary, comm.FullContent, comm.EntityIDs, comm.RelationshipIDs, embedding)
}

func (s *SessionStore) checkAdmission(entityDelta, relationshipDelta, documentDelta int, memoryDelta int64) error {
	if documentDelta > 0 {
		if err := s.session.CheckDocumentQuota(documentDelta); err != nil {
			return err
		}
	}
	if entityDelta > 0 {
		if err := s.session.CheckEntityQuota(entityDelta); err != nil {
			return err
		}
	}
	if relationshipDelta > 0 {
		if err := s.session.CheckRelationshipQuota(relationshipDelta); err != nil {
			return err
		}
	}
	if memoryDelta > 0 {
		if err := s.session.CheckMemoryQuota(memoryDelta); err != nil {
			return err
		}
	}
	return nil
}

func (s *SessionStore) recordAdmission(entityDelta, relationshipDelta, documentDelta int, memoryDelta int64) {
	if documentDelta > 0 {
		s.session.IncrementDocument(documentDelta)
	}
	if entityDelta > 0 {
		s.session.IncrementEntity(entityDelta)
	}
	if relationshipDelta > 0 {
		s.session.IncrementRelationship(relationshipDelta)
	}
	if memoryDelta > 0 {
		s.session.AddMemory(memoryDelta)
	}
}

func (s *SessionStore) recalculateSessionUsageLocked() {
	var memoryBytes int64
	for _, doc := range s.documents {
		memoryBytes += s.estimateDocumentObject(doc)
	}
	for id, tu := range s.textUnits {
		memoryBytes += s.estimateTextUnitObject(tu, s.textUnitEmbeddings[id])
	}
	for id, ent := range s.entities {
		memoryBytes += s.estimateEntityObject(ent, s.entityEmbeddings[id])
	}
	for _, rel := range s.relationships {
		memoryBytes += s.estimateRelationshipObject(rel)
	}
	for id, comm := range s.communities {
		memoryBytes += s.estimateCommunityObject(comm, s.communityEmbeddings[id])
	}
	s.session.SetUsage(len(s.entities), len(s.relationships), len(s.documents), memoryBytes)
}

// GetInfo returns session info with counts
func (s *SessionStore) GetInfo() types.SessionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info := s.session.GetInfo()
	info.DocumentCount = len(s.documents)
	info.TextUnitCount = len(s.textUnits)
	info.EntityCount = len(s.entities)
	info.RelationshipCount = len(s.relationships)
	info.CommunityCount = len(s.communities)
	info.MemoryBytes = s.session.MemoryUsage()
	info.MaxEntities, info.MaxRelationships, info.MaxDocuments, info.MaxMemoryBytes = s.session.QuotaSnapshot()
	return info
}

// GetIDGenerator returns the ID generator
func (s *SessionStore) GetIDGenerator() *types.IDGenerator {
	return s.idGen
}

// =============================================================================
// Vector Index Management (lazy initialization)
// =============================================================================

func (s *SessionStore) getTextUnitIndex() vector.Index {
	if s.textUnitIndex == nil {
		s.textUnitIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
	}
	return s.textUnitIndex
}

func (s *SessionStore) getEntityIndex() vector.Index {
	if s.entityIndex == nil {
		s.entityIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
	}
	return s.entityIndex
}

func (s *SessionStore) getCommunityIndex() vector.Index {
	if s.communityIndex == nil {
		s.communityIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
	}
	return s.communityIndex
}

// GetTextUnitIndex returns the text unit vector index
func (s *SessionStore) GetTextUnitIndex() vector.Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getTextUnitIndex()
}

// GetEntityIndex returns the entity vector index
func (s *SessionStore) GetEntityIndex() vector.Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getEntityIndex()
}

// GetCommunityIndex returns the community vector index
func (s *SessionStore) GetCommunityIndex() vector.Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCommunityIndex()
}

// IndexCounts returns vector index sizes without lazily creating missing indexes.
func (s *SessionStore) IndexCounts() (textUnits, entities, communities int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.textUnitIndex != nil {
		textUnits = s.textUnitIndex.Count()
	}
	if s.entityIndex != nil {
		entities = s.entityIndex.Count()
	}
	if s.communityIndex != nil {
		communities = s.communityIndex.Count()
	}
	return textUnits, entities, communities
}

// =============================================================================
// Document Operations
// =============================================================================

// AddDocument adds a document to the session
func (s *SessionStore) AddDocument(extID, filename string) (*types.Document, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := requireExternalID("document", extID); err != nil {
		return nil, err
	}
	if _, exists := s.docByExtID[extID]; exists {
		return nil, fmt.Errorf("document with external_id %s already exists", extID)
	}
	memoryBytes := s.estimateDocumentMemory(extID, filename)
	if err := s.checkAdmission(0, 0, 1, memoryBytes); err != nil {
		return nil, err
	}

	doc := types.NewDocument(s.idGen.NextDocumentID(), extID, filename)
	s.documents[doc.ID] = doc
	s.docByExtID[extID] = doc.ID
	if filename != "" {
		s.docByFilename[filename] = append(s.docByFilename[filename], doc.ID)
	}
	s.recordAdmission(0, 0, 1, memoryBytes)

	s.session.Touch()
	return doc, nil
}

// GetDocument retrieves a document by ID
func (s *SessionStore) GetDocument(id uint64) (*types.Document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	doc, ok := s.documents[id]
	if ok {
		s.session.Touch()
	}
	return doc, ok
}

// GetDocumentByExternalID retrieves a document by external ID
func (s *SessionStore) GetDocumentByExternalID(extID string) (*types.Document, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.docByExtID[extID]
	if !ok {
		return nil, false
	}
	return s.documents[id], true
}

func (s *SessionStore) removeDocumentFilenameIndex(filename string, id uint64) {
	if filename == "" {
		return
	}

	ids := s.docByFilename[filename]
	for i, docID := range ids {
		if docID == id {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	if len(ids) == 0 {
		delete(s.docByFilename, filename)
		return
	}
	s.docByFilename[filename] = ids
}

// DeleteDocumentChecked removes a document if it has no dependent text units.
func (s *SessionStore) DeleteDocumentChecked(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, ok := s.documents[id]
	if !ok {
		return fmt.Errorf("document not found")
	}
	if len(s.tuByDocID[id]) > 0 {
		return fmt.Errorf("document %d has dependent text units", id)
	}

	delete(s.docByExtID, doc.ExternalID)
	s.removeDocumentFilenameIndex(doc.Filename, id)
	delete(s.documents, id)
	s.session.DecrementDocument(1)
	s.session.SubMemory(s.estimateDocumentObject(doc))

	s.session.Touch()
	return nil
}

// DeleteDocument removes a document
func (s *SessionStore) DeleteDocument(id uint64) bool {
	return s.DeleteDocumentChecked(id) == nil
}

// GetAllDocuments returns all documents
func (s *SessionStore) GetAllDocuments() []*types.Document {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.Document, 0, len(s.documents))
	for _, doc := range s.documents {
		result = append(result, doc)
	}
	return result
}

// DocumentCount returns the number of documents
func (s *SessionStore) DocumentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.documents)
}

// =============================================================================
// TextUnit Operations
// =============================================================================

// AddTextUnit adds a text unit to the session
func (s *SessionStore) AddTextUnit(extID string, docID uint64, content string, embedding []float32, tokenCount int) (*types.TextUnit, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := requireExternalID("textunit", extID); err != nil {
		return nil, err
	}
	if _, exists := s.documents[docID]; !exists {
		return nil, fmt.Errorf("textunit document_id %d does not exist", docID)
	}
	if err := s.validateOptionalEmbedding("textunit", embedding); err != nil {
		return nil, err
	}
	if _, exists := s.tuByExtID[extID]; exists {
		return nil, fmt.Errorf("textunit with external_id %s already exists", extID)
	}
	memoryBytes := s.estimateTextUnitMemory(extID, content, embedding)
	if err := s.checkAdmission(0, 0, 0, memoryBytes); err != nil {
		return nil, err
	}

	tu := types.NewTextUnit(s.idGen.NextTextUnitID(), extID, docID, content, tokenCount)
	s.textUnits[tu.ID] = tu
	s.tuByExtID[extID] = tu.ID
	s.tuByDocID[docID] = append(s.tuByDocID[docID], tu.ID)

	// Add to vector index
	if len(embedding) > 0 {
		if err := s.getTextUnitIndex().Add(tu.ID, embedding); err != nil {
			delete(s.textUnits, tu.ID)
			delete(s.tuByExtID, extID)
			s.removeTextUnitDocumentIndex(tu.DocumentID, tu.ID)
			return nil, err
		}
		s.textUnitEmbeddings[tu.ID] = append([]float32(nil), embedding...)
	}
	s.recordAdmission(0, 0, 0, memoryBytes)

	s.session.Touch()
	return tu, nil
}

// GetTextUnit retrieves a text unit by ID
func (s *SessionStore) GetTextUnit(id uint64) (*types.TextUnit, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tu, ok := s.textUnits[id]
	if ok {
		s.session.Touch()
	}
	return tu, ok
}

// GetTextUnitsByDocumentID retrieves all text units for a document
func (s *SessionStore) GetTextUnitsByDocumentID(docID uint64) []*types.TextUnit {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.tuByDocID[docID]
	result := make([]*types.TextUnit, 0, len(ids))
	for _, id := range ids {
		if tu, ok := s.textUnits[id]; ok {
			result = append(result, tu)
		}
	}
	return result
}

func (s *SessionStore) removeTextUnitDocumentIndex(docID, id uint64) {
	ids := s.tuByDocID[docID]
	for i, tid := range ids {
		if tid == id {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	if len(ids) == 0 {
		delete(s.tuByDocID, docID)
		return
	}
	s.tuByDocID[docID] = ids
}

// DeleteTextUnitChecked removes a text unit if it is not linked to entities.
func (s *SessionStore) DeleteTextUnitChecked(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tu, ok := s.textUnits[id]
	if !ok {
		return fmt.Errorf("textunit not found")
	}
	if len(tu.EntityIDs) > 0 {
		return fmt.Errorf("textunit %d is linked to entities", id)
	}
	embedding := s.textUnitEmbeddings[id]

	delete(s.tuByExtID, tu.ExternalID)

	s.removeTextUnitDocumentIndex(tu.DocumentID, id)

	delete(s.textUnits, id)
	delete(s.textUnitEmbeddings, id)

	if s.textUnitIndex != nil {
		s.textUnitIndex.Remove(id)
	}
	s.session.SubMemory(s.estimateTextUnitObject(tu, embedding))

	s.session.Touch()
	return nil
}

// DeleteTextUnit removes a text unit
func (s *SessionStore) DeleteTextUnit(id uint64) bool {
	return s.DeleteTextUnitChecked(id) == nil
}

// LinkTextUnitToEntity links a text unit to an entity
func (s *SessionStore) LinkTextUnitToEntity(tuID, entityID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	tu, ok := s.textUnits[tuID]
	if !ok {
		return false
	}

	ent, ok := s.entities[entityID]
	if !ok {
		return false
	}

	tu.AddEntityID(entityID)
	ent.AddTextUnitID(tuID)

	s.session.Touch()
	return true
}

// GetAllTextUnits returns all text units
func (s *SessionStore) GetAllTextUnits() []*types.TextUnit {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.TextUnit, 0, len(s.textUnits))
	for _, tu := range s.textUnits {
		result = append(result, tu)
	}
	return result
}

// TextUnitCount returns the number of text units
func (s *SessionStore) TextUnitCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.textUnits)
}

// =============================================================================
// Entity Operations
// =============================================================================

// AddEntity adds an entity to the session
func (s *SessionStore) AddEntity(extID, title, entType, description string, embedding []float32) (*types.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := requireExternalID("entity", extID); err != nil {
		return nil, err
	}
	normalizedTitle := strings.ToUpper(strings.TrimSpace(title))

	if _, exists := s.entByTitle[normalizedTitle]; exists {
		return nil, fmt.Errorf("entity with title %s already exists", title)
	}
	if _, exists := s.entByExtID[extID]; exists {
		return nil, fmt.Errorf("entity with external_id %s already exists", extID)
	}
	if err := s.validateOptionalEmbedding("entity", embedding); err != nil {
		return nil, err
	}
	memoryBytes := s.estimateEntityMemory(extID, normalizedTitle, entType, description, embedding)
	if err := s.checkAdmission(1, 0, 0, memoryBytes); err != nil {
		return nil, err
	}

	ent := types.NewEntity(s.idGen.NextEntityID(), extID, normalizedTitle, entType, description)
	s.entities[ent.ID] = ent
	s.entByTitle[normalizedTitle] = ent.ID
	s.entByExtID[extID] = ent.ID

	// Add to vector index
	if len(embedding) > 0 {
		if err := s.getEntityIndex().Add(ent.ID, embedding); err != nil {
			delete(s.entities, ent.ID)
			delete(s.entByTitle, normalizedTitle)
			delete(s.entByExtID, extID)
			return nil, err
		}
		s.entityEmbeddings[ent.ID] = append([]float32(nil), embedding...)
	}
	s.recordAdmission(1, 0, 0, memoryBytes)

	s.session.Touch()
	return ent, nil
}

// GetEntity retrieves an entity by ID
func (s *SessionStore) GetEntity(id uint64) (*types.Entity, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ent, ok := s.entities[id]
	if ok {
		s.session.Touch()
	}
	return ent, ok
}

// GetEntityByTitle retrieves an entity by title
func (s *SessionStore) GetEntityByTitle(title string) (*types.Entity, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	normalizedTitle := strings.ToUpper(strings.TrimSpace(title))
	id, ok := s.entByTitle[normalizedTitle]
	if !ok {
		return nil, false
	}
	return s.entities[id], true
}

// UpdateEntityDescription updates an entity's description
func (s *SessionStore) UpdateEntityDescription(id uint64, description string, embedding []float32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	ent, ok := s.entities[id]
	if !ok {
		return false
	}

	ent.Description = description

	// Update vector index
	if len(embedding) > 0 && s.entityIndex != nil {
		s.entityIndex.Remove(id)
		if err := s.entityIndex.Add(id, embedding); err != nil {
			return false
		}
		s.entityEmbeddings[id] = append([]float32(nil), embedding...)
	}

	s.session.Touch()
	return true
}

// DeleteEntityChecked removes an entity if no text units, relationships, or
// communities still reference it.
func (s *SessionStore) DeleteEntityChecked(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ent, ok := s.entities[id]
	if !ok {
		return fmt.Errorf("entity not found")
	}
	if len(ent.TextUnitIDs) > 0 {
		return fmt.Errorf("entity %d is linked to text units", id)
	}
	if len(s.outEdges[id]) > 0 || len(s.inEdges[id]) > 0 {
		return fmt.Errorf("entity %d is linked to relationships", id)
	}
	if s.communityReferencesEntity(id) {
		return fmt.Errorf("entity %d is referenced by communities", id)
	}
	embedding := s.entityEmbeddings[id]

	delete(s.entByTitle, ent.Title)
	delete(s.entByExtID, ent.ExternalID)
	delete(s.entities, id)
	delete(s.entityEmbeddings, id)

	if s.entityIndex != nil {
		s.entityIndex.Remove(id)
	}
	s.session.DecrementEntity(1)
	s.session.SubMemory(s.estimateEntityObject(ent, embedding))

	s.session.Touch()
	return nil
}

// DeleteEntity removes an entity
func (s *SessionStore) DeleteEntity(id uint64) bool {
	return s.DeleteEntityChecked(id) == nil
}

// GetAllEntities returns all entities
func (s *SessionStore) GetAllEntities() []*types.Entity {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.Entity, 0, len(s.entities))
	for _, ent := range s.entities {
		result = append(result, ent)
	}
	return result
}

// ListEntities returns entities after the given cursor, up to limit, in ID order.
func (s *SessionStore) ListEntities(afterID uint64, limit int) ([]*types.Entity, uint64) {
	if limit <= 0 {
		limit = 1000
	}

	s.mu.RLock()
	ids := make([]uint64, 0, len(s.entities))
	for id := range s.entities {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	start := sort.Search(len(ids), func(i int) bool { return ids[i] > afterID })

	results := make([]*types.Entity, 0, limit)
	i := start
	var lastID uint64

	s.mu.RLock()
	for ; i < len(ids) && len(results) < limit; i++ {
		lastID = ids[i]
		if ent, ok := s.entities[lastID]; ok {
			results = append(results, ent)
		}
	}
	s.mu.RUnlock()

	s.session.Touch()

	if i < len(ids) {
		return results, lastID
	}
	return results, 0
}

// EntityCount returns the number of entities
func (s *SessionStore) EntityCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entities)
}

// =============================================================================
// Relationship Operations
// =============================================================================

func (s *SessionStore) makeRelKey(sourceID, targetID uint64, relType string) string {
	return fmt.Sprintf("%d|%d|%s", sourceID, targetID, relType)
}

// AddRelationship adds a relationship to the session
func (s *SessionStore) AddRelationship(extID string, sourceID, targetID uint64, relType, description string, weight float32) (*types.Relationship, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entities[sourceID]; !exists {
		return nil, fmt.Errorf("relationship source_id %d does not exist", sourceID)
	}
	if _, exists := s.entities[targetID]; !exists {
		return nil, fmt.Errorf("relationship target_id %d does not exist", targetID)
	}

	key := s.makeRelKey(sourceID, targetID, relType)
	if _, exists := s.relBySourceTarget[key]; exists {
		return nil, fmt.Errorf("relationship from %d to %d with type %s already exists", sourceID, targetID, relType)
	}
	if extID != "" {
		if _, exists := s.relByExtID[extID]; exists {
			return nil, fmt.Errorf("relationship with external_id %s already exists", extID)
		}
	}

	if weight == 0 {
		weight = 1.0
	}
	memoryBytes := s.estimateRelationshipMemory(extID, relType, description)
	if err := s.checkAdmission(0, 1, 0, memoryBytes); err != nil {
		return nil, err
	}

	rel := types.NewRelationship(s.idGen.NextRelationshipID(), extID, sourceID, targetID, relType, description, weight)
	s.relationships[rel.ID] = rel
	s.relBySourceTarget[key] = rel.ID
	if extID != "" {
		s.relByExtID[extID] = rel.ID
	}
	s.outEdges[sourceID] = append(s.outEdges[sourceID], rel.ID)
	s.inEdges[targetID] = append(s.inEdges[targetID], rel.ID)
	s.recordAdmission(0, 1, 0, memoryBytes)

	s.session.Touch()
	return rel, nil
}

// GetRelationship retrieves a relationship by ID
func (s *SessionStore) GetRelationship(id uint64) (*types.Relationship, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rel, ok := s.relationships[id]
	if ok {
		s.session.Touch()
	}
	return rel, ok
}

// GetRelationshipBySourceTarget retrieves a relationship by source and target
func (s *SessionStore) GetRelationshipBySourceTarget(sourceID, targetID uint64) (*types.Relationship, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, id := range s.outEdges[sourceID] {
		rel, ok := s.relationships[id]
		if ok && rel.TargetID == targetID {
			return rel, true
		}
	}
	return nil, false
}

// GetOutgoingRelationships retrieves outgoing relationships for an entity
func (s *SessionStore) GetOutgoingRelationships(entityID uint64) []*types.Relationship {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.outEdges[entityID]
	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if rel, ok := s.relationships[id]; ok {
			result = append(result, rel)
		}
	}
	return result
}

// GetIncomingRelationships retrieves incoming relationships for an entity
func (s *SessionStore) GetIncomingRelationships(entityID uint64) []*types.Relationship {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.inEdges[entityID]
	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if rel, ok := s.relationships[id]; ok {
			result = append(result, rel)
		}
	}
	return result
}

// GetNeighbors returns all neighboring entity IDs
func (s *SessionStore) GetNeighbors(entityID uint64) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	neighborSet := make(map[uint64]struct{})

	for _, relID := range s.outEdges[entityID] {
		if rel, ok := s.relationships[relID]; ok {
			neighborSet[rel.TargetID] = struct{}{}
		}
	}
	for _, relID := range s.inEdges[entityID] {
		if rel, ok := s.relationships[relID]; ok {
			neighborSet[rel.SourceID] = struct{}{}
		}
	}

	result := make([]uint64, 0, len(neighborSet))
	for id := range neighborSet {
		result = append(result, id)
	}
	return result
}

// DeleteRelationshipChecked removes a relationship if no communities still
// reference it.
func (s *SessionStore) DeleteRelationshipChecked(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rel, ok := s.relationships[id]
	if !ok {
		return fmt.Errorf("relationship not found")
	}
	if s.communityReferencesRelationship(id) {
		return fmt.Errorf("relationship %d is referenced by communities", id)
	}

	key := s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)
	delete(s.relBySourceTarget, key)
	delete(s.relByExtID, rel.ExternalID)

	// Remove from outEdges
	outIDs := s.outEdges[rel.SourceID]
	for i, rid := range outIDs {
		if rid == id {
			s.outEdges[rel.SourceID] = append(outIDs[:i], outIDs[i+1:]...)
			break
		}
	}

	// Remove from inEdges
	inIDs := s.inEdges[rel.TargetID]
	for i, rid := range inIDs {
		if rid == id {
			s.inEdges[rel.TargetID] = append(inIDs[:i], inIDs[i+1:]...)
			break
		}
	}

	delete(s.relationships, id)
	s.session.DecrementRelationship(1)
	s.session.SubMemory(s.estimateRelationshipObject(rel))

	s.session.Touch()
	return nil
}

// DeleteRelationship removes a relationship
func (s *SessionStore) DeleteRelationship(id uint64) bool {
	return s.DeleteRelationshipChecked(id) == nil
}

// GetAllRelationships returns all relationships
func (s *SessionStore) GetAllRelationships() []*types.Relationship {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.Relationship, 0, len(s.relationships))
	for _, rel := range s.relationships {
		result = append(result, rel)
	}
	return result
}

// ListRelationships returns relationships after the given cursor, up to limit, in ID order.
func (s *SessionStore) ListRelationships(afterID uint64, limit int) ([]*types.Relationship, uint64) {
	if limit <= 0 {
		limit = 1000
	}

	s.mu.RLock()
	ids := make([]uint64, 0, len(s.relationships))
	for id := range s.relationships {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	start := sort.Search(len(ids), func(i int) bool { return ids[i] > afterID })

	results := make([]*types.Relationship, 0, limit)
	i := start
	var lastID uint64

	s.mu.RLock()
	for ; i < len(ids) && len(results) < limit; i++ {
		lastID = ids[i]
		if rel, ok := s.relationships[lastID]; ok {
			results = append(results, rel)
		}
	}
	s.mu.RUnlock()

	s.session.Touch()

	if i < len(ids) {
		return results, lastID
	}
	return results, 0
}

// RelationshipCount returns the number of relationships
func (s *SessionStore) RelationshipCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.relationships)
}

// =============================================================================
// Community Operations
// =============================================================================

// AddCommunity adds a community to the session
func (s *SessionStore) AddCommunity(extID, title, summary, fullContent string, level int, entityIDs, relIDs []uint64, embedding []float32) (*types.Community, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if extID != "" {
		if _, exists := s.commByExtID[extID]; exists {
			return nil, fmt.Errorf("community with external_id %s already exists", extID)
		}
	}
	for _, entityID := range entityIDs {
		if _, exists := s.entities[entityID]; !exists {
			return nil, fmt.Errorf("community entity_id %d does not exist", entityID)
		}
	}
	for _, relID := range relIDs {
		if _, exists := s.relationships[relID]; !exists {
			return nil, fmt.Errorf("community relationship_id %d does not exist", relID)
		}
	}
	if err := s.validateOptionalEmbedding("community", embedding); err != nil {
		return nil, err
	}
	memoryBytes := s.estimateCommunityMemory(extID, title, summary, fullContent, entityIDs, relIDs, embedding)
	if err := s.checkAdmission(0, 0, 0, memoryBytes); err != nil {
		return nil, err
	}

	comm := types.NewCommunity(s.idGen.NextCommunityID(), extID, title, summary, fullContent, level, entityIDs, relIDs)
	s.communities[comm.ID] = comm
	if extID != "" {
		s.commByExtID[extID] = comm.ID
	}
	s.commByLevel[level] = append(s.commByLevel[level], comm.ID)

	// Add to vector index
	if len(embedding) > 0 {
		if err := s.getCommunityIndex().Add(comm.ID, embedding); err != nil {
			delete(s.communities, comm.ID)
			delete(s.commByExtID, extID)
			s.removeCommunityLevelIndex(comm.Level, comm.ID)
			return nil, err
		}
		s.communityEmbeddings[comm.ID] = append([]float32(nil), embedding...)
	}
	s.recordAdmission(0, 0, 0, memoryBytes)

	s.session.Touch()
	return comm, nil
}

// GetCommunity retrieves a community by ID
func (s *SessionStore) GetCommunity(id uint64) (*types.Community, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	comm, ok := s.communities[id]
	if ok {
		s.session.Touch()
	}
	return comm, ok
}

// GetCommunitiesByLevel retrieves communities at a level
func (s *SessionStore) GetCommunitiesByLevel(level int) []*types.Community {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := s.commByLevel[level]
	result := make([]*types.Community, 0, len(ids))
	for _, id := range ids {
		if comm, ok := s.communities[id]; ok {
			result = append(result, comm)
		}
	}
	return result
}

func (s *SessionStore) communityReferencesEntity(id uint64) bool {
	for _, comm := range s.communities {
		for _, entityID := range comm.EntityIDs {
			if entityID == id {
				return true
			}
		}
	}
	return false
}

func (s *SessionStore) communityReferencesRelationship(id uint64) bool {
	for _, comm := range s.communities {
		for _, relID := range comm.RelationshipIDs {
			if relID == id {
				return true
			}
		}
	}
	return false
}

func (s *SessionStore) removeCommunityLevelIndex(level int, id uint64) {
	ids := s.commByLevel[level]
	for i, commID := range ids {
		if commID == id {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	if len(ids) == 0 {
		delete(s.commByLevel, level)
		return
	}
	s.commByLevel[level] = ids
}

// DeleteCommunityChecked removes a community.
func (s *SessionStore) DeleteCommunityChecked(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	comm, ok := s.communities[id]
	if !ok {
		return fmt.Errorf("community not found")
	}
	embedding := s.communityEmbeddings[id]

	delete(s.commByExtID, comm.ExternalID)

	s.removeCommunityLevelIndex(comm.Level, id)

	delete(s.communities, id)
	delete(s.communityEmbeddings, id)

	if s.communityIndex != nil {
		s.communityIndex.Remove(id)
	}
	s.session.SubMemory(s.estimateCommunityObject(comm, embedding))

	s.session.Touch()
	return nil
}

// DeleteCommunity removes a community
func (s *SessionStore) DeleteCommunity(id uint64) bool {
	return s.DeleteCommunityChecked(id) == nil
}

// ClearCommunities removes all communities (useful before re-computing)
func (s *SessionStore) ClearCommunities() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.communities = make(map[uint64]*types.Community)
	s.commByExtID = make(map[string]uint64)
	s.commByLevel = make(map[int][]uint64)
	s.communityEmbeddings = make(map[uint64][]float32)

	if s.communityIndex != nil {
		s.communityIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
	}
	s.recalculateSessionUsageLocked()
}

// GetAllCommunities returns all communities
func (s *SessionStore) GetAllCommunities() []*types.Community {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*types.Community, 0, len(s.communities))
	for _, comm := range s.communities {
		result = append(result, comm)
	}
	return result
}

// CommunityCount returns the number of communities
func (s *SessionStore) CommunityCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.communities)
}

// RebuildVectorIndices rebuilds derived vector index structures without
// changing canonical session data.
func (s *SessionStore) RebuildVectorIndices() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.textUnitIndex = nil
	if len(s.textUnitEmbeddings) > 0 {
		s.textUnitIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
		for id, embedding := range s.textUnitEmbeddings {
			if err := s.textUnitIndex.Add(id, embedding); err != nil {
				return err
			}
		}
	}
	s.entityIndex = nil
	if len(s.entityEmbeddings) > 0 {
		s.entityIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
		for id, embedding := range s.entityEmbeddings {
			if err := s.entityIndex.Add(id, embedding); err != nil {
				return err
			}
		}
	}
	s.communityIndex = nil
	if len(s.communityEmbeddings) > 0 {
		s.communityIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
		for id, embedding := range s.communityEmbeddings {
			if err := s.communityIndex.Add(id, embedding); err != nil {
				return err
			}
		}
	}

	s.session.Touch()
	return nil
}

// =============================================================================
// Bulk Operations
// =============================================================================

func atomicBulkError(kind string, index int, err error) error {
	return fmt.Errorf("atomic bulk %s failed at index %d: %w; no items committed", kind, index, err)
}

func (s *SessionStore) MSetDocumentsAtomic(inputs []types.BulkDocumentInput) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if err := requireExternalID("document", input.ExternalID); err != nil {
			return nil, atomicBulkError("documents", i, err)
		}
		if _, exists := s.docByExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("documents", i, fmt.Errorf("document with external_id %s already exists", input.ExternalID))
		}
		if _, exists := seenExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("documents", i, fmt.Errorf("document with external_id %s already exists in bulk request", input.ExternalID))
		}
		seenExtID[input.ExternalID] = struct{}{}
		memoryBytes += s.estimateDocumentMemory(input.ExternalID, input.Filename)
	}
	if err := s.checkAdmission(0, 0, len(inputs), memoryBytes); err != nil {
		return nil, atomicBulkError("documents", 0, err)
	}

	ids := make([]uint64, 0, len(inputs))
	for _, input := range inputs {
		doc := types.NewDocument(s.idGen.NextDocumentID(), input.ExternalID, input.Filename)
		s.documents[doc.ID] = doc
		s.docByExtID[doc.ExternalID] = doc.ID
		if doc.Filename != "" {
			s.docByFilename[doc.Filename] = append(s.docByFilename[doc.Filename], doc.ID)
		}
		ids = append(ids, doc.ID)
	}

	s.recordAdmission(0, 0, len(inputs), memoryBytes)
	s.session.Touch()
	return ids, nil
}

func (s *SessionStore) MSetTextUnitsAtomic(inputs []types.BulkTextUnitInput) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if err := requireExternalID("textunit", input.ExternalID); err != nil {
			return nil, atomicBulkError("textunits", i, err)
		}
		if _, exists := s.tuByExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("textunits", i, fmt.Errorf("textunit with external_id %s already exists", input.ExternalID))
		}
		if _, exists := seenExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("textunits", i, fmt.Errorf("textunit with external_id %s already exists in bulk request", input.ExternalID))
		}
		if _, exists := s.documents[input.DocumentID]; !exists {
			return nil, atomicBulkError("textunits", i, fmt.Errorf("textunit document_id %d does not exist", input.DocumentID))
		}
		if err := s.validateOptionalEmbedding("textunit", input.Embedding); err != nil {
			return nil, atomicBulkError("textunits", i, err)
		}
		seenExtID[input.ExternalID] = struct{}{}
		memoryBytes += s.estimateTextUnitMemory(input.ExternalID, input.Content, input.Embedding)
	}
	if err := s.checkAdmission(0, 0, 0, memoryBytes); err != nil {
		return nil, atomicBulkError("textunits", 0, err)
	}

	ids := make([]uint64, 0, len(inputs))
	for i, input := range inputs {
		tu := types.NewTextUnit(s.idGen.NextTextUnitID(), input.ExternalID, input.DocumentID, input.Content, input.TokenCount)
		s.textUnits[tu.ID] = tu
		s.tuByExtID[tu.ExternalID] = tu.ID
		s.tuByDocID[tu.DocumentID] = append(s.tuByDocID[tu.DocumentID], tu.ID)
		ids = append(ids, tu.ID)

		if len(input.Embedding) > 0 {
			if err := s.getTextUnitIndex().Add(tu.ID, input.Embedding); err != nil {
				s.rollbackTextUnits(ids)
				return nil, atomicBulkError("textunits", i, err)
			}
			s.textUnitEmbeddings[tu.ID] = append([]float32(nil), input.Embedding...)
		}
	}

	s.recordAdmission(0, 0, 0, memoryBytes)
	s.session.Touch()
	return ids, nil
}

func (s *SessionStore) MSetEntitiesAtomic(inputs []types.BulkEntityInput) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	seenTitle := make(map[string]struct{}, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if err := requireExternalID("entity", input.ExternalID); err != nil {
			return nil, atomicBulkError("entities", i, err)
		}
		normalizedTitle := strings.ToUpper(strings.TrimSpace(input.Title))
		if _, exists := s.entByTitle[normalizedTitle]; exists {
			return nil, atomicBulkError("entities", i, fmt.Errorf("entity with title %s already exists", input.Title))
		}
		if _, exists := seenTitle[normalizedTitle]; exists {
			return nil, atomicBulkError("entities", i, fmt.Errorf("entity with title %s already exists in bulk request", input.Title))
		}
		if _, exists := s.entByExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("entities", i, fmt.Errorf("entity with external_id %s already exists", input.ExternalID))
		}
		if _, exists := seenExtID[input.ExternalID]; exists {
			return nil, atomicBulkError("entities", i, fmt.Errorf("entity with external_id %s already exists in bulk request", input.ExternalID))
		}
		if err := s.validateOptionalEmbedding("entity", input.Embedding); err != nil {
			return nil, atomicBulkError("entities", i, err)
		}
		seenExtID[input.ExternalID] = struct{}{}
		seenTitle[normalizedTitle] = struct{}{}
		memoryBytes += s.estimateEntityMemory(input.ExternalID, normalizedTitle, input.Type, input.Description, input.Embedding)
	}
	if err := s.checkAdmission(len(inputs), 0, 0, memoryBytes); err != nil {
		return nil, atomicBulkError("entities", 0, err)
	}

	ids := make([]uint64, 0, len(inputs))
	for i, input := range inputs {
		normalizedTitle := strings.ToUpper(strings.TrimSpace(input.Title))
		ent := types.NewEntity(s.idGen.NextEntityID(), input.ExternalID, normalizedTitle, input.Type, input.Description)
		s.entities[ent.ID] = ent
		s.entByTitle[normalizedTitle] = ent.ID
		s.entByExtID[ent.ExternalID] = ent.ID
		ids = append(ids, ent.ID)

		if len(input.Embedding) > 0 {
			if err := s.getEntityIndex().Add(ent.ID, input.Embedding); err != nil {
				s.rollbackEntities(ids)
				return nil, atomicBulkError("entities", i, err)
			}
			s.entityEmbeddings[ent.ID] = append([]float32(nil), input.Embedding...)
		}
	}

	s.recordAdmission(len(inputs), 0, 0, memoryBytes)
	s.session.Touch()
	return ids, nil
}

func (s *SessionStore) MSetRelationshipsAtomic(inputs []types.BulkRelationshipInput) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	seenIdentity := make(map[string]struct{}, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if _, exists := s.entities[input.SourceID]; !exists {
			return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship source_id %d does not exist", input.SourceID))
		}
		if _, exists := s.entities[input.TargetID]; !exists {
			return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship target_id %d does not exist", input.TargetID))
		}
		key := s.makeRelKey(input.SourceID, input.TargetID, input.Type)
		if _, exists := s.relBySourceTarget[key]; exists {
			return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship from %d to %d with type %s already exists", input.SourceID, input.TargetID, input.Type))
		}
		if _, exists := seenIdentity[key]; exists {
			return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship from %d to %d with type %s already exists in bulk request", input.SourceID, input.TargetID, input.Type))
		}
		if input.ExternalID != "" {
			if _, exists := s.relByExtID[input.ExternalID]; exists {
				return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship with external_id %s already exists", input.ExternalID))
			}
			if _, exists := seenExtID[input.ExternalID]; exists {
				return nil, atomicBulkError("relationships", i, fmt.Errorf("relationship with external_id %s already exists in bulk request", input.ExternalID))
			}
			seenExtID[input.ExternalID] = struct{}{}
		}
		seenIdentity[key] = struct{}{}
		memoryBytes += s.estimateRelationshipMemory(input.ExternalID, input.Type, input.Description)
	}
	if err := s.checkAdmission(0, len(inputs), 0, memoryBytes); err != nil {
		return nil, atomicBulkError("relationships", 0, err)
	}

	ids := make([]uint64, 0, len(inputs))
	for _, input := range inputs {
		weight := input.Weight
		if weight == 0 {
			weight = 1.0
		}
		rel := types.NewRelationship(s.idGen.NextRelationshipID(), input.ExternalID, input.SourceID, input.TargetID, input.Type, input.Description, weight)
		s.relationships[rel.ID] = rel
		s.relBySourceTarget[s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)] = rel.ID
		if rel.ExternalID != "" {
			s.relByExtID[rel.ExternalID] = rel.ID
		}
		s.outEdges[rel.SourceID] = append(s.outEdges[rel.SourceID], rel.ID)
		s.inEdges[rel.TargetID] = append(s.inEdges[rel.TargetID], rel.ID)
		ids = append(ids, rel.ID)
	}

	s.recordAdmission(0, len(inputs), 0, memoryBytes)
	s.session.Touch()
	return ids, nil
}

func (s *SessionStore) rollbackTextUnits(ids []uint64) {
	for _, id := range ids {
		tu := s.textUnits[id]
		if tu == nil {
			continue
		}
		delete(s.tuByExtID, tu.ExternalID)
		s.removeTextUnitDocumentIndex(tu.DocumentID, tu.ID)
		delete(s.textUnits, tu.ID)
		delete(s.textUnitEmbeddings, tu.ID)
		if s.textUnitIndex != nil {
			s.textUnitIndex.Remove(tu.ID)
		}
	}
}

func (s *SessionStore) rollbackEntities(ids []uint64) {
	for _, id := range ids {
		ent := s.entities[id]
		if ent == nil {
			continue
		}
		delete(s.entByTitle, ent.Title)
		delete(s.entByExtID, ent.ExternalID)
		delete(s.entities, ent.ID)
		delete(s.entityEmbeddings, ent.ID)
		if s.entityIndex != nil {
			s.entityIndex.Remove(ent.ID)
		}
	}
}

// Clear removes all data from the session store
func (s *SessionStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.documents = make(map[uint64]*types.Document)
	s.docByExtID = make(map[string]uint64)
	s.docByFilename = make(map[string][]uint64)

	s.textUnits = make(map[uint64]*types.TextUnit)
	s.tuByExtID = make(map[string]uint64)
	s.tuByDocID = make(map[uint64][]uint64)

	s.entities = make(map[uint64]*types.Entity)
	s.entByExtID = make(map[string]uint64)
	s.entByTitle = make(map[string]uint64)

	s.relationships = make(map[uint64]*types.Relationship)
	s.relByExtID = make(map[string]uint64)
	s.relBySourceTarget = make(map[string]uint64)
	s.outEdges = make(map[uint64][]uint64)
	s.inEdges = make(map[uint64][]uint64)

	s.communities = make(map[uint64]*types.Community)
	s.commByExtID = make(map[string]uint64)
	s.commByLevel = make(map[int][]uint64)

	// Reset vector indices
	s.textUnitIndex = nil
	s.entityIndex = nil
	s.communityIndex = nil
	s.textUnitEmbeddings = make(map[uint64][]float32)
	s.entityEmbeddings = make(map[uint64][]float32)
	s.communityEmbeddings = make(map[uint64][]float32)

	// Reset ID generator
	s.idGen = types.NewIDGenerator()
	s.session.SetUsage(0, 0, 0, 0)
}

// =============================================================================
// Snapshot/Restore Support
// =============================================================================

// SessionSnapshot contains all session state for serialization
type SessionSnapshot struct {
	SessionID        string                `json:"session_id"`
	Session          *types.Session        `json:"session"`
	Documents        []*types.Document     `json:"documents"`
	TextUnits        []*types.TextUnit     `json:"text_units"`
	Entities         []*types.Entity       `json:"entities"`
	Relationships    []*types.Relationship `json:"relationships"`
	Communities      []*types.Community    `json:"communities"`
	IDGeneratorState map[string]uint64     `json:"id_generator_state"`
	TextUnitVectors  map[uint64][]float32  `json:"text_unit_vectors"`
	EntityVectors    map[uint64][]float32  `json:"entity_vectors"`
	CommunityVectors map[uint64][]float32  `json:"community_vectors"`
}

// Snapshot creates a snapshot of the session
func (s *SessionStore) Snapshot() *SessionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := &SessionSnapshot{
		SessionID:        s.session.ID,
		Session:          s.session,
		Documents:        s.GetAllDocuments(),
		TextUnits:        s.GetAllTextUnits(),
		Entities:         s.GetAllEntities(),
		Relationships:    s.GetAllRelationships(),
		Communities:      s.GetAllCommunities(),
		IDGeneratorState: make(map[string]uint64),
	}

	// Save ID generator state
	doc, tu, ent, rel, comm, _ := s.idGen.GetCounters()
	snapshot.IDGeneratorState["document"] = doc
	snapshot.IDGeneratorState["textunit"] = tu
	snapshot.IDGeneratorState["entity"] = ent
	snapshot.IDGeneratorState["relationship"] = rel
	snapshot.IDGeneratorState["community"] = comm

	// Save canonical embeddings. Vector indexes are derived state.
	if len(s.textUnitEmbeddings) > 0 {
		snapshot.TextUnitVectors = make(map[uint64][]float32, len(s.textUnitEmbeddings))
		for id, embedding := range s.textUnitEmbeddings {
			snapshot.TextUnitVectors[id] = append([]float32(nil), embedding...)
		}
	}
	if len(s.entityEmbeddings) > 0 {
		snapshot.EntityVectors = make(map[uint64][]float32, len(s.entityEmbeddings))
		for id, embedding := range s.entityEmbeddings {
			snapshot.EntityVectors[id] = append([]float32(nil), embedding...)
		}
	}
	if len(s.communityEmbeddings) > 0 {
		snapshot.CommunityVectors = make(map[uint64][]float32, len(s.communityEmbeddings))
		for id, embedding := range s.communityEmbeddings {
			snapshot.CommunityVectors[id] = append([]float32(nil), embedding...)
		}
	}

	return snapshot
}

// RestoreFromSnapshot restores a session from a snapshot
func (s *SessionStore) RestoreFromSnapshot(snapshot *SessionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Restore session metadata
	s.session = snapshot.Session

	// Clear and restore documents
	s.documents = make(map[uint64]*types.Document)
	s.docByExtID = make(map[string]uint64)
	s.docByFilename = make(map[string][]uint64)
	for _, doc := range snapshot.Documents {
		s.documents[doc.ID] = doc
		s.docByExtID[doc.ExternalID] = doc.ID
		if doc.Filename != "" {
			s.docByFilename[doc.Filename] = append(s.docByFilename[doc.Filename], doc.ID)
		}
	}

	// Clear and restore text units
	s.textUnits = make(map[uint64]*types.TextUnit)
	s.tuByExtID = make(map[string]uint64)
	s.tuByDocID = make(map[uint64][]uint64)
	for _, tu := range snapshot.TextUnits {
		s.textUnits[tu.ID] = tu
		s.tuByExtID[tu.ExternalID] = tu.ID
		s.tuByDocID[tu.DocumentID] = append(s.tuByDocID[tu.DocumentID], tu.ID)
	}

	// Clear and restore entities
	s.entities = make(map[uint64]*types.Entity)
	s.entByExtID = make(map[string]uint64)
	s.entByTitle = make(map[string]uint64)
	for _, ent := range snapshot.Entities {
		s.entities[ent.ID] = ent
		s.entByTitle[ent.Title] = ent.ID
		if ent.ExternalID != "" {
			s.entByExtID[ent.ExternalID] = ent.ID
		}
	}

	// Clear and restore relationships
	s.relationships = make(map[uint64]*types.Relationship)
	s.relByExtID = make(map[string]uint64)
	s.relBySourceTarget = make(map[string]uint64)
	s.outEdges = make(map[uint64][]uint64)
	s.inEdges = make(map[uint64][]uint64)
	for _, rel := range snapshot.Relationships {
		s.relationships[rel.ID] = rel
		key := s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)
		s.relBySourceTarget[key] = rel.ID
		if rel.ExternalID != "" {
			s.relByExtID[rel.ExternalID] = rel.ID
		}
		s.outEdges[rel.SourceID] = append(s.outEdges[rel.SourceID], rel.ID)
		s.inEdges[rel.TargetID] = append(s.inEdges[rel.TargetID], rel.ID)
	}

	// Clear and restore communities
	s.communities = make(map[uint64]*types.Community)
	s.commByExtID = make(map[string]uint64)
	s.commByLevel = make(map[int][]uint64)
	for _, comm := range snapshot.Communities {
		s.communities[comm.ID] = comm
		if comm.ExternalID != "" {
			s.commByExtID[comm.ExternalID] = comm.ID
		}
		s.commByLevel[comm.Level] = append(s.commByLevel[comm.Level], comm.ID)
	}

	// Restore ID generator
	if snapshot.IDGeneratorState != nil {
		s.idGen.RestoreState(snapshot.IDGeneratorState)
	}

	// Restore vector indices
	s.textUnitIndex = nil
	s.entityIndex = nil
	s.communityIndex = nil
	s.textUnitEmbeddings = make(map[uint64][]float32)
	s.entityEmbeddings = make(map[uint64][]float32)
	s.communityEmbeddings = make(map[uint64][]float32)

	if len(snapshot.TextUnitVectors) > 0 {
		idx := s.getTextUnitIndex()
		for id, vec := range snapshot.TextUnitVectors {
			s.textUnitEmbeddings[id] = append([]float32(nil), vec...)
			if err := idx.Add(id, vec); err != nil {
				return err
			}
		}
	}
	if len(snapshot.EntityVectors) > 0 {
		idx := s.getEntityIndex()
		for id, vec := range snapshot.EntityVectors {
			s.entityEmbeddings[id] = append([]float32(nil), vec...)
			if err := idx.Add(id, vec); err != nil {
				return err
			}
		}
	}
	if len(snapshot.CommunityVectors) > 0 {
		idx := s.getCommunityIndex()
		for id, vec := range snapshot.CommunityVectors {
			s.communityEmbeddings[id] = append([]float32(nil), vec...)
			if err := idx.Add(id, vec); err != nil {
				return err
			}
		}
	}

	s.recalculateSessionUsageLocked()
	return nil
}
