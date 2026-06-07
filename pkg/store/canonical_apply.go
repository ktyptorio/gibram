package store

import (
	"fmt"
	"strings"

	"github.com/gibram-io/gibram/pkg/types"
	"github.com/gibram-io/gibram/pkg/vector"
)

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func cloneDocument(doc *types.Document) *types.Document {
	if doc == nil {
		return nil
	}
	cp := *doc
	if doc.Attrs != nil {
		cp.Attrs = make(map[string]string, len(doc.Attrs))
		for k, v := range doc.Attrs {
			cp.Attrs[k] = v
		}
	}
	return &cp
}

func cloneTextUnit(tu *types.TextUnit) *types.TextUnit {
	if tu == nil {
		return nil
	}
	cp := *tu
	cp.EntityIDs = append([]uint64(nil), tu.EntityIDs...)
	return &cp
}

func cloneEntity(ent *types.Entity) *types.Entity {
	if ent == nil {
		return nil
	}
	cp := *ent
	if ent.Attrs != nil {
		cp.Attrs = make(map[string]string, len(ent.Attrs))
		for k, v := range ent.Attrs {
			cp.Attrs[k] = v
		}
	}
	cp.TextUnitIDs = append([]uint64(nil), ent.TextUnitIDs...)
	return &cp
}

func cloneRelationship(rel *types.Relationship) *types.Relationship {
	if rel == nil {
		return nil
	}
	cp := *rel
	cp.TextUnitIDs = append([]uint64(nil), rel.TextUnitIDs...)
	return &cp
}

func cloneCommunity(comm *types.Community) *types.Community {
	if comm == nil {
		return nil
	}
	cp := *comm
	cp.EntityIDs = append([]uint64(nil), comm.EntityIDs...)
	cp.RelationshipIDs = append([]uint64(nil), comm.RelationshipIDs...)
	return &cp
}

func (s *SessionStore) advanceIDCounters(doc, tu, ent, rel uint64) {
	curDoc, curTU, curEnt, curRel, curComm, curQuery := s.idGen.GetCounters()
	s.idGen.SetCounters(
		maxUint64(curDoc, doc),
		maxUint64(curTU, tu),
		maxUint64(curEnt, ent),
		maxUint64(curRel, rel),
		curComm,
		curQuery,
	)
}

func (s *SessionStore) advanceCommunityCounter(comm uint64) {
	curDoc, curTU, curEnt, curRel, curComm, curQuery := s.idGen.GetCounters()
	s.idGen.SetCounters(curDoc, curTU, curEnt, curRel, maxUint64(curComm, comm), curQuery)
}

func (s *SessionStore) PrepareDocument(extID, filename string) (*types.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := requireExternalID("document", extID); err != nil {
		return nil, err
	}
	if _, exists := s.docByExtID[extID]; exists {
		return nil, fmt.Errorf("document with external_id %s already exists", extID)
	}
	if err := s.checkAdmission(0, 0, 1, s.estimateDocumentMemory(extID, filename)); err != nil {
		return nil, err
	}
	return types.NewDocument(s.idGen.CurrentDocumentID()+1, extID, filename), nil
}

func (s *SessionStore) PrepareDocumentsBatch(inputs []types.BulkDocumentInput) ([]*types.Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	nextID := s.idGen.CurrentDocumentID()
	docs := make([]*types.Document, 0, len(inputs))
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
		nextID++
		docs = append(docs, types.NewDocument(nextID, input.ExternalID, input.Filename))
		memoryBytes += s.estimateDocumentMemory(input.ExternalID, input.Filename)
	}
	if err := s.checkAdmission(0, 0, len(inputs), memoryBytes); err != nil {
		return nil, atomicBulkError("documents", 0, err)
	}
	return docs, nil
}

func (s *SessionStore) ApplyDocument(doc *types.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc = cloneDocument(doc)
	if doc == nil {
		return fmt.Errorf("document is required")
	}
	if doc.ID == 0 {
		return fmt.Errorf("document id is required")
	}
	if _, exists := s.documents[doc.ID]; exists {
		return fmt.Errorf("document id %d already exists", doc.ID)
	}
	if _, exists := s.docByExtID[doc.ExternalID]; exists {
		return fmt.Errorf("document external_id %s already exists", doc.ExternalID)
	}
	s.documents[doc.ID] = doc
	s.docByExtID[doc.ExternalID] = doc.ID
	if doc.Filename != "" {
		s.docByFilename[doc.Filename] = append(s.docByFilename[doc.Filename], doc.ID)
	}
	s.advanceIDCounters(doc.ID, 0, 0, 0)
	s.recordAdmission(0, 0, 1, s.estimateDocumentObject(doc))
	s.session.Touch()
	return nil
}

func (s *SessionStore) ApplyDocumentsBatch(docs []*types.Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenID := make(map[uint64]struct{}, len(docs))
	seenExtID := make(map[string]struct{}, len(docs))
	clones := make([]*types.Document, 0, len(docs))
	for _, original := range docs {
		doc := cloneDocument(original)
		if doc == nil {
			return fmt.Errorf("document is required")
		}
		if doc.ID == 0 {
			return fmt.Errorf("document id is required")
		}
		if _, exists := seenID[doc.ID]; exists {
			return fmt.Errorf("document id %d already exists in batch", doc.ID)
		}
		if _, exists := s.documents[doc.ID]; exists {
			return fmt.Errorf("document id %d already exists", doc.ID)
		}
		if _, exists := seenExtID[doc.ExternalID]; exists {
			return fmt.Errorf("document external_id %s already exists in batch", doc.ExternalID)
		}
		if _, exists := s.docByExtID[doc.ExternalID]; exists {
			return fmt.Errorf("document external_id %s already exists", doc.ExternalID)
		}
		seenID[doc.ID] = struct{}{}
		seenExtID[doc.ExternalID] = struct{}{}
		clones = append(clones, doc)
	}
	for _, doc := range clones {
		s.documents[doc.ID] = doc
		s.docByExtID[doc.ExternalID] = doc.ID
		if doc.Filename != "" {
			s.docByFilename[doc.Filename] = append(s.docByFilename[doc.Filename], doc.ID)
		}
		s.advanceIDCounters(doc.ID, 0, 0, 0)
	}
	var memoryBytes int64
	for _, doc := range clones {
		memoryBytes += s.estimateDocumentObject(doc)
	}
	s.recordAdmission(0, 0, len(clones), memoryBytes)
	s.session.Touch()
	return nil
}

func (s *SessionStore) PrepareTextUnit(extID string, docID uint64, content string, embedding []float32, tokenCount int) (*types.TextUnit, []float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := requireExternalID("textunit", extID); err != nil {
		return nil, nil, err
	}
	if _, exists := s.documents[docID]; !exists {
		return nil, nil, fmt.Errorf("textunit document_id %d does not exist", docID)
	}
	if err := s.validateOptionalEmbedding("textunit", embedding); err != nil {
		return nil, nil, err
	}
	if _, exists := s.tuByExtID[extID]; exists {
		return nil, nil, fmt.Errorf("textunit with external_id %s already exists", extID)
	}
	if err := s.checkAdmission(0, 0, 0, s.estimateTextUnitMemory(extID, content, embedding)); err != nil {
		return nil, nil, err
	}
	return types.NewTextUnit(s.idGen.CurrentTextUnitID()+1, extID, docID, content, tokenCount), append([]float32(nil), embedding...), nil
}

func (s *SessionStore) PrepareTextUnitsBatch(inputs []types.BulkTextUnitInput) ([]*types.TextUnit, [][]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	nextID := s.idGen.CurrentTextUnitID()
	textUnits := make([]*types.TextUnit, 0, len(inputs))
	embeddings := make([][]float32, 0, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if err := requireExternalID("textunit", input.ExternalID); err != nil {
			return nil, nil, atomicBulkError("textunits", i, err)
		}
		if _, exists := s.tuByExtID[input.ExternalID]; exists {
			return nil, nil, atomicBulkError("textunits", i, fmt.Errorf("textunit with external_id %s already exists", input.ExternalID))
		}
		if _, exists := seenExtID[input.ExternalID]; exists {
			return nil, nil, atomicBulkError("textunits", i, fmt.Errorf("textunit with external_id %s already exists in bulk request", input.ExternalID))
		}
		if _, exists := s.documents[input.DocumentID]; !exists {
			return nil, nil, atomicBulkError("textunits", i, fmt.Errorf("textunit document_id %d does not exist", input.DocumentID))
		}
		if err := s.validateOptionalEmbedding("textunit", input.Embedding); err != nil {
			return nil, nil, atomicBulkError("textunits", i, err)
		}
		seenExtID[input.ExternalID] = struct{}{}
		nextID++
		textUnits = append(textUnits, types.NewTextUnit(nextID, input.ExternalID, input.DocumentID, input.Content, input.TokenCount))
		embeddings = append(embeddings, append([]float32(nil), input.Embedding...))
		memoryBytes += s.estimateTextUnitMemory(input.ExternalID, input.Content, input.Embedding)
	}
	if err := s.checkAdmission(0, 0, 0, memoryBytes); err != nil {
		return nil, nil, atomicBulkError("textunits", 0, err)
	}
	return textUnits, embeddings, nil
}

func (s *SessionStore) ApplyTextUnit(tu *types.TextUnit, embedding []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tu = cloneTextUnit(tu)
	if tu == nil {
		return fmt.Errorf("textunit is required")
	}
	if tu.ID == 0 {
		return fmt.Errorf("textunit id is required")
	}
	if _, exists := s.documents[tu.DocumentID]; !exists {
		return fmt.Errorf("textunit document_id %d does not exist", tu.DocumentID)
	}
	if _, exists := s.textUnits[tu.ID]; exists {
		return fmt.Errorf("textunit id %d already exists", tu.ID)
	}
	if _, exists := s.tuByExtID[tu.ExternalID]; exists {
		return fmt.Errorf("textunit external_id %s already exists", tu.ExternalID)
	}
	s.textUnits[tu.ID] = tu
	s.tuByExtID[tu.ExternalID] = tu.ID
	s.tuByDocID[tu.DocumentID] = append(s.tuByDocID[tu.DocumentID], tu.ID)
	if len(embedding) > 0 {
		if err := s.getTextUnitIndex().Add(tu.ID, embedding); err != nil {
			delete(s.textUnits, tu.ID)
			delete(s.tuByExtID, tu.ExternalID)
			s.removeTextUnitDocumentIndex(tu.DocumentID, tu.ID)
			return err
		}
		s.textUnitEmbeddings[tu.ID] = append([]float32(nil), embedding...)
	}
	s.advanceIDCounters(0, tu.ID, 0, 0)
	s.recordAdmission(0, 0, 0, s.estimateTextUnitObject(tu, embedding))
	s.session.Touch()
	return nil
}

func (s *SessionStore) ApplyTextUnitsBatch(textUnits []*types.TextUnit, embeddings [][]float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(textUnits) != len(embeddings) {
		return fmt.Errorf("textunit batch length mismatch")
	}
	seenID := make(map[uint64]struct{}, len(textUnits))
	seenExtID := make(map[string]struct{}, len(textUnits))
	clones := make([]*types.TextUnit, 0, len(textUnits))
	for _, original := range textUnits {
		tu := cloneTextUnit(original)
		if tu == nil {
			return fmt.Errorf("textunit is required")
		}
		if tu.ID == 0 {
			return fmt.Errorf("textunit id is required")
		}
		if _, exists := seenID[tu.ID]; exists {
			return fmt.Errorf("textunit id %d already exists in batch", tu.ID)
		}
		if _, exists := s.documents[tu.DocumentID]; !exists {
			return fmt.Errorf("textunit document_id %d does not exist", tu.DocumentID)
		}
		if _, exists := s.textUnits[tu.ID]; exists {
			return fmt.Errorf("textunit id %d already exists", tu.ID)
		}
		if _, exists := seenExtID[tu.ExternalID]; exists {
			return fmt.Errorf("textunit external_id %s already exists in batch", tu.ExternalID)
		}
		if _, exists := s.tuByExtID[tu.ExternalID]; exists {
			return fmt.Errorf("textunit external_id %s already exists", tu.ExternalID)
		}
		seenID[tu.ID] = struct{}{}
		seenExtID[tu.ExternalID] = struct{}{}
		clones = append(clones, tu)
	}
	ids := make([]uint64, 0, len(textUnits))
	for i, tu := range clones {
		s.textUnits[tu.ID] = tu
		s.tuByExtID[tu.ExternalID] = tu.ID
		s.tuByDocID[tu.DocumentID] = append(s.tuByDocID[tu.DocumentID], tu.ID)
		ids = append(ids, tu.ID)

		if len(embeddings[i]) > 0 {
			if err := s.getTextUnitIndex().Add(tu.ID, embeddings[i]); err != nil {
				s.rollbackTextUnits(ids)
				return err
			}
			s.textUnitEmbeddings[tu.ID] = append([]float32(nil), embeddings[i]...)
		}
	}
	for _, tu := range clones {
		s.advanceIDCounters(0, tu.ID, 0, 0)
	}
	var memoryBytes int64
	for i, tu := range clones {
		memoryBytes += s.estimateTextUnitObject(tu, embeddings[i])
	}
	s.recordAdmission(0, 0, 0, memoryBytes)
	s.session.Touch()
	return nil
}

func (s *SessionStore) PrepareEntity(extID, title, entType, description string, embedding []float32) (*types.Entity, []float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := requireExternalID("entity", extID); err != nil {
		return nil, nil, err
	}
	normalizedTitle := strings.ToUpper(strings.TrimSpace(title))
	if _, exists := s.entByTitle[normalizedTitle]; exists {
		return nil, nil, fmt.Errorf("entity with title %s already exists", title)
	}
	if _, exists := s.entByExtID[extID]; exists {
		return nil, nil, fmt.Errorf("entity with external_id %s already exists", extID)
	}
	if err := s.validateOptionalEmbedding("entity", embedding); err != nil {
		return nil, nil, err
	}
	if err := s.checkAdmission(1, 0, 0, s.estimateEntityMemory(extID, normalizedTitle, entType, description, embedding)); err != nil {
		return nil, nil, err
	}
	return types.NewEntity(s.idGen.CurrentEntityID()+1, extID, normalizedTitle, entType, description), append([]float32(nil), embedding...), nil
}

func (s *SessionStore) PrepareEntitiesBatch(inputs []types.BulkEntityInput) ([]*types.Entity, [][]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	seenTitle := make(map[string]struct{}, len(inputs))
	nextID := s.idGen.CurrentEntityID()
	entities := make([]*types.Entity, 0, len(inputs))
	embeddings := make([][]float32, 0, len(inputs))
	var memoryBytes int64
	for i, input := range inputs {
		if err := requireExternalID("entity", input.ExternalID); err != nil {
			return nil, nil, atomicBulkError("entities", i, err)
		}
		normalizedTitle := strings.ToUpper(strings.TrimSpace(input.Title))
		if _, exists := s.entByTitle[normalizedTitle]; exists {
			return nil, nil, atomicBulkError("entities", i, fmt.Errorf("entity with title %s already exists", input.Title))
		}
		if _, exists := seenTitle[normalizedTitle]; exists {
			return nil, nil, atomicBulkError("entities", i, fmt.Errorf("entity with title %s already exists in bulk request", input.Title))
		}
		if _, exists := s.entByExtID[input.ExternalID]; exists {
			return nil, nil, atomicBulkError("entities", i, fmt.Errorf("entity with external_id %s already exists", input.ExternalID))
		}
		if _, exists := seenExtID[input.ExternalID]; exists {
			return nil, nil, atomicBulkError("entities", i, fmt.Errorf("entity with external_id %s already exists in bulk request", input.ExternalID))
		}
		if err := s.validateOptionalEmbedding("entity", input.Embedding); err != nil {
			return nil, nil, atomicBulkError("entities", i, err)
		}
		seenExtID[input.ExternalID] = struct{}{}
		seenTitle[normalizedTitle] = struct{}{}
		nextID++
		entities = append(entities, types.NewEntity(nextID, input.ExternalID, normalizedTitle, input.Type, input.Description))
		embeddings = append(embeddings, append([]float32(nil), input.Embedding...))
		memoryBytes += s.estimateEntityMemory(input.ExternalID, normalizedTitle, input.Type, input.Description, input.Embedding)
	}
	if err := s.checkAdmission(len(inputs), 0, 0, memoryBytes); err != nil {
		return nil, nil, atomicBulkError("entities", 0, err)
	}
	return entities, embeddings, nil
}

func (s *SessionStore) ApplyEntity(ent *types.Entity, embedding []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ent = cloneEntity(ent)
	if ent == nil {
		return fmt.Errorf("entity is required")
	}
	if ent.ID == 0 {
		return fmt.Errorf("entity id is required")
	}
	if _, exists := s.entities[ent.ID]; exists {
		return fmt.Errorf("entity id %d already exists", ent.ID)
	}
	if _, exists := s.entByExtID[ent.ExternalID]; exists {
		return fmt.Errorf("entity external_id %s already exists", ent.ExternalID)
	}
	if _, exists := s.entByTitle[ent.Title]; exists {
		return fmt.Errorf("entity title %s already exists", ent.Title)
	}
	s.entities[ent.ID] = ent
	s.entByExtID[ent.ExternalID] = ent.ID
	s.entByTitle[ent.Title] = ent.ID
	if len(embedding) > 0 {
		if err := s.getEntityIndex().Add(ent.ID, embedding); err != nil {
			delete(s.entities, ent.ID)
			delete(s.entByExtID, ent.ExternalID)
			delete(s.entByTitle, ent.Title)
			return err
		}
		s.entityEmbeddings[ent.ID] = append([]float32(nil), embedding...)
	}
	s.advanceIDCounters(0, 0, ent.ID, 0)
	s.recordAdmission(1, 0, 0, s.estimateEntityObject(ent, embedding))
	s.session.Touch()
	return nil
}

func (s *SessionStore) ApplyEntitiesBatch(entities []*types.Entity, embeddings [][]float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(entities) != len(embeddings) {
		return fmt.Errorf("entity batch length mismatch")
	}
	seenID := make(map[uint64]struct{}, len(entities))
	seenExtID := make(map[string]struct{}, len(entities))
	seenTitle := make(map[string]struct{}, len(entities))
	clones := make([]*types.Entity, 0, len(entities))
	for _, original := range entities {
		ent := cloneEntity(original)
		if ent == nil {
			return fmt.Errorf("entity is required")
		}
		if ent.ID == 0 {
			return fmt.Errorf("entity id is required")
		}
		if _, exists := seenID[ent.ID]; exists {
			return fmt.Errorf("entity id %d already exists in batch", ent.ID)
		}
		if _, exists := s.entities[ent.ID]; exists {
			return fmt.Errorf("entity id %d already exists", ent.ID)
		}
		if _, exists := seenExtID[ent.ExternalID]; exists {
			return fmt.Errorf("entity external_id %s already exists in batch", ent.ExternalID)
		}
		if _, exists := s.entByExtID[ent.ExternalID]; exists {
			return fmt.Errorf("entity external_id %s already exists", ent.ExternalID)
		}
		if _, exists := seenTitle[ent.Title]; exists {
			return fmt.Errorf("entity title %s already exists in batch", ent.Title)
		}
		if _, exists := s.entByTitle[ent.Title]; exists {
			return fmt.Errorf("entity title %s already exists", ent.Title)
		}
		seenID[ent.ID] = struct{}{}
		seenExtID[ent.ExternalID] = struct{}{}
		seenTitle[ent.Title] = struct{}{}
		clones = append(clones, ent)
	}
	ids := make([]uint64, 0, len(entities))
	for i, ent := range clones {
		s.entities[ent.ID] = ent
		s.entByExtID[ent.ExternalID] = ent.ID
		s.entByTitle[ent.Title] = ent.ID
		ids = append(ids, ent.ID)

		if len(embeddings[i]) > 0 {
			if err := s.getEntityIndex().Add(ent.ID, embeddings[i]); err != nil {
				s.rollbackEntities(ids)
				return err
			}
			s.entityEmbeddings[ent.ID] = append([]float32(nil), embeddings[i]...)
		}
	}
	for _, ent := range clones {
		s.advanceIDCounters(0, 0, ent.ID, 0)
	}
	var memoryBytes int64
	for i, ent := range clones {
		memoryBytes += s.estimateEntityObject(ent, embeddings[i])
	}
	s.recordAdmission(len(clones), 0, 0, memoryBytes)
	s.session.Touch()
	return nil
}

func (s *SessionStore) PrepareRelationship(extID string, sourceID, targetID uint64, relType, description string, weight float32) (*types.Relationship, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
	if err := s.checkAdmission(0, 1, 0, s.estimateRelationshipMemory(extID, relType, description)); err != nil {
		return nil, err
	}
	return types.NewRelationship(s.idGen.CurrentRelationshipID()+1, extID, sourceID, targetID, relType, description, weight), nil
}

func (s *SessionStore) PrepareRelationshipsBatch(inputs []types.BulkRelationshipInput) ([]*types.Relationship, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seenExtID := make(map[string]struct{}, len(inputs))
	seenIdentity := make(map[string]struct{}, len(inputs))
	nextID := s.idGen.CurrentRelationshipID()
	relationships := make([]*types.Relationship, 0, len(inputs))
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
		weight := input.Weight
		if weight == 0 {
			weight = 1.0
		}
		nextID++
		relationships = append(relationships, types.NewRelationship(nextID, input.ExternalID, input.SourceID, input.TargetID, input.Type, input.Description, weight))
		memoryBytes += s.estimateRelationshipMemory(input.ExternalID, input.Type, input.Description)
	}
	if err := s.checkAdmission(0, len(inputs), 0, memoryBytes); err != nil {
		return nil, atomicBulkError("relationships", 0, err)
	}
	return relationships, nil
}

func (s *SessionStore) ApplyRelationship(rel *types.Relationship) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rel = cloneRelationship(rel)
	if rel == nil {
		return fmt.Errorf("relationship is required")
	}
	if rel.ID == 0 {
		return fmt.Errorf("relationship id is required")
	}
	if _, exists := s.entities[rel.SourceID]; !exists {
		return fmt.Errorf("relationship source_id %d does not exist", rel.SourceID)
	}
	if _, exists := s.entities[rel.TargetID]; !exists {
		return fmt.Errorf("relationship target_id %d does not exist", rel.TargetID)
	}
	if _, exists := s.relationships[rel.ID]; exists {
		return fmt.Errorf("relationship id %d already exists", rel.ID)
	}
	key := s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)
	if _, exists := s.relBySourceTarget[key]; exists {
		return fmt.Errorf("relationship from %d to %d with type %s already exists", rel.SourceID, rel.TargetID, rel.Type)
	}
	if rel.ExternalID != "" {
		if _, exists := s.relByExtID[rel.ExternalID]; exists {
			return fmt.Errorf("relationship external_id %s already exists", rel.ExternalID)
		}
		s.relByExtID[rel.ExternalID] = rel.ID
	}
	s.relationships[rel.ID] = rel
	s.relBySourceTarget[key] = rel.ID
	s.outEdges[rel.SourceID] = append(s.outEdges[rel.SourceID], rel.ID)
	s.inEdges[rel.TargetID] = append(s.inEdges[rel.TargetID], rel.ID)
	s.advanceIDCounters(0, 0, 0, rel.ID)
	s.recordAdmission(0, 1, 0, s.estimateRelationshipObject(rel))
	s.session.Touch()
	return nil
}

func (s *SessionStore) ApplyRelationshipsBatch(relationships []*types.Relationship) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	seenID := make(map[uint64]struct{}, len(relationships))
	seenExtID := make(map[string]struct{}, len(relationships))
	seenIdentity := make(map[string]struct{}, len(relationships))
	clones := make([]*types.Relationship, 0, len(relationships))
	for _, original := range relationships {
		rel := cloneRelationship(original)
		if rel == nil {
			return fmt.Errorf("relationship is required")
		}
		if rel.ID == 0 {
			return fmt.Errorf("relationship id is required")
		}
		if _, exists := seenID[rel.ID]; exists {
			return fmt.Errorf("relationship id %d already exists in batch", rel.ID)
		}
		if _, exists := s.entities[rel.SourceID]; !exists {
			return fmt.Errorf("relationship source_id %d does not exist", rel.SourceID)
		}
		if _, exists := s.entities[rel.TargetID]; !exists {
			return fmt.Errorf("relationship target_id %d does not exist", rel.TargetID)
		}
		if _, exists := s.relationships[rel.ID]; exists {
			return fmt.Errorf("relationship id %d already exists", rel.ID)
		}
		key := s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)
		if _, exists := seenIdentity[key]; exists {
			return fmt.Errorf("relationship from %d to %d with type %s already exists in batch", rel.SourceID, rel.TargetID, rel.Type)
		}
		if _, exists := s.relBySourceTarget[key]; exists {
			return fmt.Errorf("relationship from %d to %d with type %s already exists", rel.SourceID, rel.TargetID, rel.Type)
		}
		if rel.ExternalID != "" {
			if _, exists := seenExtID[rel.ExternalID]; exists {
				return fmt.Errorf("relationship external_id %s already exists in batch", rel.ExternalID)
			}
			if _, exists := s.relByExtID[rel.ExternalID]; exists {
				return fmt.Errorf("relationship external_id %s already exists", rel.ExternalID)
			}
			seenExtID[rel.ExternalID] = struct{}{}
		}
		seenID[rel.ID] = struct{}{}
		seenIdentity[key] = struct{}{}
		clones = append(clones, rel)
	}
	for _, rel := range clones {
		key := s.makeRelKey(rel.SourceID, rel.TargetID, rel.Type)
		if rel.ExternalID != "" {
			s.relByExtID[rel.ExternalID] = rel.ID
		}
		s.relationships[rel.ID] = rel
		s.relBySourceTarget[key] = rel.ID
		s.outEdges[rel.SourceID] = append(s.outEdges[rel.SourceID], rel.ID)
		s.inEdges[rel.TargetID] = append(s.inEdges[rel.TargetID], rel.ID)
		s.advanceIDCounters(0, 0, 0, rel.ID)
	}
	var memoryBytes int64
	for _, rel := range clones {
		memoryBytes += s.estimateRelationshipObject(rel)
	}
	s.recordAdmission(0, len(clones), 0, memoryBytes)
	s.session.Touch()
	return nil
}

func (s *SessionStore) ValidateDeleteDocument(id uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.documents[id]; !ok {
		return fmt.Errorf("document not found")
	}
	if len(s.tuByDocID[id]) > 0 {
		return fmt.Errorf("document %d has dependent text units", id)
	}
	return nil
}

func (s *SessionStore) ValidateDeleteTextUnit(id uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tu, ok := s.textUnits[id]
	if !ok {
		return fmt.Errorf("textunit not found")
	}
	if len(tu.EntityIDs) > 0 {
		return fmt.Errorf("textunit %d is linked to entities", id)
	}
	return nil
}

func (s *SessionStore) ValidateDeleteEntity(id uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
	return nil
}

func (s *SessionStore) ValidateDeleteRelationship(id uint64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.relationships[id]; !ok {
		return fmt.Errorf("relationship not found")
	}
	if s.communityReferencesRelationship(id) {
		return fmt.Errorf("relationship %d is referenced by communities", id)
	}
	return nil
}

func (s *SessionStore) ReplaceCommunities(communities []*types.Community, embeddings [][]float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(embeddings) > 0 && len(communities) != len(embeddings) {
		return fmt.Errorf("community replacement length mismatch")
	}
	seenID := make(map[uint64]struct{}, len(communities))
	seenExtID := make(map[string]struct{}, len(communities))
	clones := make([]*types.Community, 0, len(communities))
	var newMemoryBytes int64
	for i, original := range communities {
		comm := cloneCommunity(original)
		if comm == nil {
			return fmt.Errorf("community is required")
		}
		if comm.ID == 0 {
			return fmt.Errorf("community id is required")
		}
		if _, exists := seenID[comm.ID]; exists {
			return fmt.Errorf("community id %d already exists in replacement", comm.ID)
		}
		if comm.ExternalID != "" {
			if _, exists := seenExtID[comm.ExternalID]; exists {
				return fmt.Errorf("community external_id %s already exists in replacement", comm.ExternalID)
			}
			seenExtID[comm.ExternalID] = struct{}{}
		}
		for _, entityID := range comm.EntityIDs {
			if _, exists := s.entities[entityID]; !exists {
				return fmt.Errorf("community entity_id %d does not exist", entityID)
			}
		}
		for _, relID := range comm.RelationshipIDs {
			if _, exists := s.relationships[relID]; !exists {
				return fmt.Errorf("community relationship_id %d does not exist", relID)
			}
		}
		if len(embeddings) > 0 {
			if err := s.validateOptionalEmbedding("community", embeddings[i]); err != nil {
				return err
			}
		}
		seenID[comm.ID] = struct{}{}
		clones = append(clones, comm)
		if len(embeddings) > 0 {
			newMemoryBytes += s.estimateCommunityObject(comm, embeddings[i])
		} else {
			newMemoryBytes += s.estimateCommunityObject(comm, nil)
		}
	}
	var oldMemoryBytes int64
	for id, comm := range s.communities {
		oldMemoryBytes += s.estimateCommunityObject(comm, s.communityEmbeddings[id])
	}
	if delta := newMemoryBytes - oldMemoryBytes; delta > 0 {
		if err := s.checkAdmission(0, 0, 0, delta); err != nil {
			return err
		}
	}

	newCommunities := make(map[uint64]*types.Community, len(clones))
	newByExtID := make(map[string]uint64)
	newByLevel := make(map[int][]uint64)
	newEmbeddings := make(map[uint64][]float32)
	var newIndex vector.Index
	if len(embeddings) > 0 {
		for _, embedding := range embeddings {
			if len(embedding) > 0 {
				newIndex = vector.NewHNSWIndex(s.vectorDim, vector.DefaultHNSWConfig())
				break
			}
		}
	}

	for i, comm := range clones {
		newCommunities[comm.ID] = comm
		if comm.ExternalID != "" {
			newByExtID[comm.ExternalID] = comm.ID
		}
		newByLevel[comm.Level] = append(newByLevel[comm.Level], comm.ID)
		if newIndex != nil && len(embeddings[i]) > 0 {
			if err := newIndex.Add(comm.ID, embeddings[i]); err != nil {
				return err
			}
			newEmbeddings[comm.ID] = append([]float32(nil), embeddings[i]...)
		}
	}

	s.communities = newCommunities
	s.commByExtID = newByExtID
	s.commByLevel = newByLevel
	s.communityIndex = newIndex
	s.communityEmbeddings = newEmbeddings
	for _, comm := range clones {
		s.advanceCommunityCounter(comm.ID)
	}
	s.recalculateSessionUsageLocked()
	s.session.Touch()
	return nil
}
