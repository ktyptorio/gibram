// Package store - unit tests for session store
package store

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/gibram-io/gibram/pkg/types"
)

const testVectorDim = 64

// =============================================================================
// Session Management Tests
// =============================================================================

func TestNewSessionStore(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	if store == nil {
		t.Fatal("NewSessionStore returned nil")
	}

	if store.GetSessionID() != "test-session" {
		t.Errorf("Expected session ID 'test-session', got '%s'", store.GetSessionID())
	}

	if store.vectorDim != testVectorDim {
		t.Errorf("Expected vectorDim %d, got %d", testVectorDim, store.vectorDim)
	}

	// Check initial counts
	info := store.GetInfo()
	if info.DocumentCount != 0 {
		t.Errorf("Expected 0 documents, got %d", info.DocumentCount)
	}
	if info.TextUnitCount != 0 {
		t.Errorf("Expected 0 text units, got %d", info.TextUnitCount)
	}
	if info.EntityCount != 0 {
		t.Errorf("Expected 0 entities, got %d", info.EntityCount)
	}
}

func TestSessionTouch(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	session := store.GetSession()
	initialAccess := session.LastAccess

	// Sleep for more than 1 second to ensure Unix timestamp changes
	time.Sleep(1100 * time.Millisecond)

	// Touch should update last access
	store.Touch()

	updatedSession := store.GetSession()
	if updatedSession.LastAccess <= initialAccess {
		t.Errorf("Touch should update LastAccess time: before=%d, after=%d", initialAccess, updatedSession.LastAccess)
	}
}

func TestSessionTTL(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	// Set TTL
	store.SetTTL(3600)
	if store.GetSession().TTL != 3600 {
		t.Errorf("Expected TTL 3600, got %d", store.GetSession().TTL)
	}

	// Set Idle TTL
	store.SetIdleTTL(1800)
	if store.GetSession().IdleTTL != 1800 {
		t.Errorf("Expected IdleTTL 1800, got %d", store.GetSession().IdleTTL)
	}
}

// =============================================================================
// Document Operations Tests
// =============================================================================

func TestAddDocument(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc, err := store.AddDocument("doc-001", "test.pdf")
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if doc == nil {
		t.Fatal("AddDocument returned nil document")
	}

	if doc.ExternalID != "doc-001" {
		t.Errorf("Expected ExternalID 'doc-001', got '%s'", doc.ExternalID)
	}

	if doc.Filename != "test.pdf" {
		t.Errorf("Expected Filename 'test.pdf', got '%s'", doc.Filename)
	}

	if store.DocumentCount() != 1 {
		t.Errorf("Expected 1 document, got %d", store.DocumentCount())
	}
}

func TestAddDocumentRequiresExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	_, err := store.AddDocument("", "test.pdf")
	if err == nil {
		t.Fatal("Expected AddDocument to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "document external_id is required") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if store.DocumentCount() != 0 {
		t.Fatalf("Expected no document to be created, got %d", store.DocumentCount())
	}
}

func TestSessionStoreQuotasRejectBeforeMutation(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)
	store.SetQuotas(0, 0, 1, 0)

	if _, err := store.AddDocument("doc-1", "one.txt"); err != nil {
		t.Fatalf("first AddDocument failed: %v", err)
	}
	if _, err := store.AddDocument("doc-2", "two.txt"); err == nil {
		t.Fatal("expected document quota error")
	} else if err != types.ErrDocumentQuotaExceeded {
		t.Fatalf("expected document quota error, got %v", err)
	}
	if got := store.DocumentCount(); got != 1 {
		t.Fatalf("quota rejection mutated document count: got %d", got)
	}
	info := store.GetInfo()
	if info.DocumentCount != 1 || info.MemoryBytes == 0 {
		t.Fatalf("unexpected session info after quota rejection: %+v", info)
	}
}

func TestSessionStoreBulkQuotaIsAtomic(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)
	store.SetQuotas(0, 0, 1, 0)

	_, err := store.MSetDocumentsAtomic([]types.BulkDocumentInput{
		{ExternalID: "doc-1", Filename: "one.txt"},
		{ExternalID: "doc-2", Filename: "two.txt"},
	})
	if err == nil {
		t.Fatal("expected bulk document quota error")
	}
	if got := store.DocumentCount(); got != 0 {
		t.Fatalf("bulk quota rejection should commit no documents, got %d", got)
	}
}

func TestSessionStoreMemoryAdmissionRejectsUnboundedWrites(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)
	store.SetQuotas(0, 0, 0, 200)

	if _, err := store.AddDocument("doc-1", "one.txt"); err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}
	_, err := store.AddTextUnit("tu-1", 1, strings.Repeat("x", 256), nil, 1)
	if err == nil {
		t.Fatal("expected memory quota error")
	}
	if err != types.ErrMemoryQuotaExceeded {
		t.Fatalf("expected memory quota error, got %v", err)
	}
	if got := store.TextUnitCount(); got != 0 {
		t.Fatalf("memory quota rejection mutated textunit count: got %d", got)
	}
}

func TestAddDocumentAllowsDuplicateFilename(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc1 := mustAddDocument(t, store, "doc-001", "shared.pdf")
	doc2 := mustAddDocument(t, store, "doc-002", "shared.pdf")

	if doc1.ID == doc2.ID {
		t.Fatal("Expected duplicate filename documents to have distinct IDs")
	}
	if store.DocumentCount() != 2 {
		t.Fatalf("Expected 2 documents, got %d", store.DocumentCount())
	}
	if _, ok := store.GetDocumentByExternalID("doc-001"); !ok {
		t.Fatal("Expected first document to remain addressable by external_id")
	}
	if _, ok := store.GetDocumentByExternalID("doc-002"); !ok {
		t.Fatal("Expected second document to remain addressable by external_id")
	}

	ids := store.docByFilename["shared.pdf"]
	if len(ids) != 2 {
		t.Fatalf("Expected filename index to keep both document IDs, got %v", ids)
	}
	if ids[0] != doc1.ID || ids[1] != doc2.ID {
		t.Fatalf("Expected filename index [%d %d], got %v", doc1.ID, doc2.ID, ids)
	}
}

func TestAddDocumentDuplicate(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	_, err := store.AddDocument("doc-001", "test.pdf")
	if err != nil {
		t.Fatalf("First AddDocument failed: %v", err)
	}

	// Try to add duplicate
	_, err = store.AddDocument("doc-001", "another.pdf")
	if err == nil {
		t.Error("Expected error when adding duplicate document")
	}

	_, err = store.AddDocument("doc-001", "test.pdf")
	if err == nil {
		t.Error("Expected duplicate external_id to fail even when filename matches")
	}
}

func TestGetDocument(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")

	// Get by ID
	retrieved, ok := store.GetDocument(doc.ID)
	if !ok {
		t.Error("GetDocument should find the document")
	}

	if retrieved.ExternalID != "doc-001" {
		t.Errorf("Expected ExternalID 'doc-001', got '%s'", retrieved.ExternalID)
	}

	// Get non-existent
	_, ok = store.GetDocument(99999)
	if ok {
		t.Error("GetDocument should return false for non-existent ID")
	}
}

func TestGetDocumentByExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	mustAddDocument(t, store, "doc-001", "test.pdf")

	doc, ok := store.GetDocumentByExternalID("doc-001")
	if !ok {
		t.Error("GetDocumentByExternalID should find the document")
	}

	if doc.Filename != "test.pdf" {
		t.Errorf("Expected Filename 'test.pdf', got '%s'", doc.Filename)
	}

	// Get non-existent
	_, ok = store.GetDocumentByExternalID("non-existent")
	if ok {
		t.Error("GetDocumentByExternalID should return false for non-existent")
	}
}

func TestDeleteDocument(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")

	// Delete
	ok := store.DeleteDocument(doc.ID)
	if !ok {
		t.Error("DeleteDocument should return true")
	}

	if store.DocumentCount() != 0 {
		t.Errorf("Expected 0 documents after delete, got %d", store.DocumentCount())
	}

	// Try to get deleted document
	_, ok = store.GetDocument(doc.ID)
	if ok {
		t.Error("Deleted document should not be found")
	}

	// Delete non-existent
	ok = store.DeleteDocument(99999)
	if ok {
		t.Error("DeleteDocument should return false for non-existent ID")
	}
}

func TestDeleteDocumentRestrictedWithTextUnits(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	mustAddTextUnit(t, store, "tu-001", doc.ID, "Content", make([]float32, testVectorDim), 5)

	err := store.DeleteDocumentChecked(doc.ID)
	if err == nil {
		t.Fatal("Expected document delete with dependent text units to fail")
	}
	if !strings.Contains(err.Error(), "document 1 has dependent text units") {
		t.Fatalf("Expected dependent text units error, got %v", err)
	}
	if store.DocumentCount() != 1 {
		t.Fatalf("Expected document to remain, got %d documents", store.DocumentCount())
	}
}

func TestGetAllDocuments(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	mustAddDocument(t, store, "doc-001", "test1.pdf")
	mustAddDocument(t, store, "doc-002", "test2.pdf")
	mustAddDocument(t, store, "doc-003", "test3.pdf")

	docs := store.GetAllDocuments()
	if len(docs) != 3 {
		t.Errorf("Expected 3 documents, got %d", len(docs))
	}
}

// =============================================================================
// TextUnit Operations Tests
// =============================================================================

func TestAddTextUnit(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")

	embedding := make([]float32, testVectorDim)
	for i := range embedding {
		embedding[i] = float32(i) / float32(testVectorDim)
	}

	tu, err := store.AddTextUnit("tu-001", doc.ID, "Test content", embedding, 5)
	if err != nil {
		t.Fatalf("AddTextUnit failed: %v", err)
	}

	if tu.ExternalID != "tu-001" {
		t.Errorf("Expected ExternalID 'tu-001', got '%s'", tu.ExternalID)
	}

	if tu.Content != "Test content" {
		t.Errorf("Expected content 'Test content', got '%s'", tu.Content)
	}

	if tu.TokenCount != 5 {
		t.Errorf("Expected TokenCount 5, got %d", tu.TokenCount)
	}

	if store.TextUnitCount() != 1 {
		t.Errorf("Expected 1 text unit, got %d", store.TextUnitCount())
	}
}

func TestAddTextUnitRequiresExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)
	doc := mustAddDocument(t, store, "doc-001", "test.pdf")

	_, err := store.AddTextUnit(" ", doc.ID, "Test content", make([]float32, testVectorDim), 5)
	if err == nil {
		t.Fatal("Expected AddTextUnit to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "textunit external_id is required") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if store.TextUnitCount() != 0 {
		t.Fatalf("Expected no text unit to be created, got %d", store.TextUnitCount())
	}
}

func TestAddTextUnitRequiresExistingDocument(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	_, err := store.AddTextUnit("tu-001", 999, "Test content", make([]float32, testVectorDim), 5)
	if err == nil {
		t.Fatal("Expected AddTextUnit to reject missing document_id")
	}
	if !strings.Contains(err.Error(), "textunit document_id 999 does not exist") {
		t.Fatalf("Expected document_id error, got %v", err)
	}
	if store.TextUnitCount() != 0 {
		t.Fatalf("Expected no text unit to be created, got %d", store.TextUnitCount())
	}
}

func TestAddTextUnitAllowsEmptyEmbedding(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)
	doc := mustAddDocument(t, store, "doc-001", "test.pdf")

	tu, err := store.AddTextUnit("tu-001", doc.ID, "Test content", nil, 5)
	if err != nil {
		t.Fatalf("AddTextUnit should allow empty embedding: %v", err)
	}
	if tu.ID == 0 {
		t.Fatal("Expected text unit ID")
	}
	if store.TextUnitCount() != 1 {
		t.Fatalf("Expected 1 text unit, got %d", store.TextUnitCount())
	}
	if store.GetTextUnitIndex().Count() != 0 {
		t.Fatal("Expected empty embedding text unit to remain non-indexed")
	}
}

func TestAddTextUnitRejectsInvalidEmbedding(t *testing.T) {
	tests := []struct {
		name      string
		embedding []float32
		want      string
	}{
		{
			name:      "wrong dimension",
			embedding: make([]float32, testVectorDim-1),
			want:      "textunit embedding dimension mismatch",
		},
		{
			name:      "NaN",
			embedding: append([]float32{float32(math.NaN())}, make([]float32, testVectorDim-1)...),
			want:      "textunit embedding contains non-finite value at index 0",
		},
		{
			name:      "Inf",
			embedding: append([]float32{float32(math.Inf(1))}, make([]float32, testVectorDim-1)...),
			want:      "textunit embedding contains non-finite value at index 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSessionStore("test-session", testVectorDim)
			doc := mustAddDocument(t, store, "doc-001", "test.pdf")

			_, err := store.AddTextUnit("tu-001", doc.ID, "Test content", tt.embedding, 5)
			if err == nil {
				t.Fatal("Expected AddTextUnit to reject invalid embedding")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %v", tt.want, err)
			}
			if store.TextUnitCount() != 0 {
				t.Fatalf("Expected no text unit to be created, got %d", store.TextUnitCount())
			}
			if got := store.GetTextUnitsByDocumentID(doc.ID); len(got) != 0 {
				t.Fatalf("Expected no text unit document index entries, got %d", len(got))
			}
		})
	}
}

func TestAddTextUnitDuplicate(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	embedding := make([]float32, testVectorDim)

	_, err := store.AddTextUnit("tu-001", doc.ID, "Content 1", embedding, 5)
	if err != nil {
		t.Fatalf("First AddTextUnit failed: %v", err)
	}

	// Try to add duplicate
	_, err = store.AddTextUnit("tu-001", doc.ID, "Content 2", embedding, 5)
	if err == nil {
		t.Error("Expected error when adding duplicate text unit")
	}
}

func TestGetTextUnit(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	embedding := make([]float32, testVectorDim)
	tu := mustAddTextUnit(t, store, "tu-001", doc.ID, "Test content", embedding, 5)

	// Get by ID
	retrieved, ok := store.GetTextUnit(tu.ID)
	if !ok {
		t.Error("GetTextUnit should find the text unit")
	}

	if retrieved.Content != "Test content" {
		t.Errorf("Expected content 'Test content', got '%s'", retrieved.Content)
	}

	// Get non-existent
	_, ok = store.GetTextUnit(99999)
	if ok {
		t.Error("GetTextUnit should return false for non-existent ID")
	}
}

func TestDeleteTextUnit(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	embedding := make([]float32, testVectorDim)
	tu := mustAddTextUnit(t, store, "tu-001", doc.ID, "Test content", embedding, 5)

	// Delete
	ok := store.DeleteTextUnit(tu.ID)
	if !ok {
		t.Error("DeleteTextUnit should return true")
	}

	if store.TextUnitCount() != 0 {
		t.Errorf("Expected 0 text units after delete, got %d", store.TextUnitCount())
	}

	// Delete non-existent
	ok = store.DeleteTextUnit(99999)
	if ok {
		t.Error("DeleteTextUnit should return false for non-existent ID")
	}
}

func TestDeleteTextUnitRestrictedWhenLinkedToEntity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	tu := mustAddTextUnit(t, store, "tu-001", doc.ID, "Content", embedding, 5)
	ent := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	if !store.LinkTextUnitToEntity(tu.ID, ent.ID) {
		t.Fatal("Expected link to succeed")
	}

	err := store.DeleteTextUnitChecked(tu.ID)
	if err == nil {
		t.Fatal("Expected linked text unit delete to fail")
	}
	if !strings.Contains(err.Error(), "textunit 1 is linked to entities") {
		t.Fatalf("Expected linked entities error, got %v", err)
	}
	if store.TextUnitCount() != 1 {
		t.Fatalf("Expected text unit to remain, got %d text units", store.TextUnitCount())
	}
}

// =============================================================================
// Entity Operations Tests
// =============================================================================

func TestAddEntity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	for i := range embedding {
		embedding[i] = float32(i) / float32(testVectorDim)
	}

	entity, err := store.AddEntity("ent-001", "Test Entity", "person", "A test entity", embedding)
	if err != nil {
		t.Fatalf("AddEntity failed: %v", err)
	}

	if entity.ExternalID != "ent-001" {
		t.Errorf("Expected ExternalID 'ent-001', got '%s'", entity.ExternalID)
	}

	// Title is normalized to uppercase
	if entity.Title != "TEST ENTITY" {
		t.Errorf("Expected Title 'TEST ENTITY', got '%s'", entity.Title)
	}

	if entity.Type != "person" {
		t.Errorf("Expected Type 'person', got '%s'", entity.Type)
	}

	if store.EntityCount() != 1 {
		t.Errorf("Expected 1 entity, got %d", store.EntityCount())
	}
}

func TestAddEntityRequiresExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	_, err := store.AddEntity("", "Test Entity", "person", "A test entity", make([]float32, testVectorDim))
	if err == nil {
		t.Fatal("Expected AddEntity to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "entity external_id is required") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if store.EntityCount() != 0 {
		t.Fatalf("Expected no entity to be created, got %d", store.EntityCount())
	}
}

func TestAddEntityDuplicate(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)

	_, err := store.AddEntity("ent-001", "Entity 1", "person", "Desc 1", embedding)
	if err != nil {
		t.Fatalf("First AddEntity failed: %v", err)
	}

	// Try to add duplicate
	_, err = store.AddEntity("ent-001", "Entity 2", "person", "Desc 2", embedding)
	if err == nil {
		t.Error("Expected error when adding duplicate entity")
	}
}

func TestAddEntityDuplicateNormalizedTitle(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	_, err := store.AddEntity("ent-001", "Bank Indonesia", "organization", "Desc 1", nil)
	if err != nil {
		t.Fatalf("First AddEntity failed: %v", err)
	}

	_, err = store.AddEntity("ent-002", " bank indonesia ", "organization", "Desc 2", nil)
	if err == nil {
		t.Fatal("Expected duplicate normalized title to fail")
	}
	if !strings.Contains(err.Error(), "entity with title  bank indonesia  already exists") {
		t.Fatalf("Expected duplicate title error, got %v", err)
	}
	if store.EntityCount() != 1 {
		t.Fatalf("Expected only first entity to be created, got %d", store.EntityCount())
	}
}

func TestAddEntityAllowsEmptyEmbedding(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	ent, err := store.AddEntity("ent-001", "Test Entity", "person", "A test entity", nil)
	if err != nil {
		t.Fatalf("AddEntity should allow empty embedding: %v", err)
	}
	if ent.ID == 0 {
		t.Fatal("Expected entity ID")
	}
	if store.EntityCount() != 1 {
		t.Fatalf("Expected 1 entity, got %d", store.EntityCount())
	}
	if store.GetEntityIndex().Count() != 0 {
		t.Fatal("Expected empty embedding entity to remain non-indexed")
	}
}

func TestAddEntityRejectsInvalidEmbedding(t *testing.T) {
	tests := []struct {
		name      string
		embedding []float32
		want      string
	}{
		{
			name:      "wrong dimension",
			embedding: make([]float32, testVectorDim-1),
			want:      "entity embedding dimension mismatch",
		},
		{
			name:      "NaN",
			embedding: append([]float32{float32(math.NaN())}, make([]float32, testVectorDim-1)...),
			want:      "entity embedding contains non-finite value at index 0",
		},
		{
			name:      "Inf",
			embedding: append([]float32{float32(math.Inf(1))}, make([]float32, testVectorDim-1)...),
			want:      "entity embedding contains non-finite value at index 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSessionStore("test-session", testVectorDim)

			_, err := store.AddEntity("ent-001", "Test Entity", "person", "A test entity", tt.embedding)
			if err == nil {
				t.Fatal("Expected AddEntity to reject invalid embedding")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %v", tt.want, err)
			}
			if store.EntityCount() != 0 {
				t.Fatalf("Expected no entity to be created, got %d", store.EntityCount())
			}
		})
	}
}

func TestGetEntity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	entity := mustAddEntity(t, store, "ent-001", "Test Entity", "person", "Description", embedding)

	// Get by ID
	retrieved, ok := store.GetEntity(entity.ID)
	if !ok {
		t.Error("GetEntity should find the entity")
	}

	// Title is normalized to uppercase
	if retrieved.Title != "TEST ENTITY" {
		t.Errorf("Expected Title 'TEST ENTITY', got '%s'", retrieved.Title)
	}

	// Get non-existent
	_, ok = store.GetEntity(99999)
	if ok {
		t.Error("GetEntity should return false for non-existent ID")
	}
}

func TestGetEntityByTitle(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	mustAddEntity(t, store, "ent-001", "Test Entity", "person", "Description", embedding)

	entity, ok := store.GetEntityByTitle("Test Entity")
	if !ok {
		t.Error("GetEntityByTitle should find the entity")
	}

	if entity.ExternalID != "ent-001" {
		t.Errorf("Expected ExternalID 'ent-001', got '%s'", entity.ExternalID)
	}

	// Get non-existent
	_, ok = store.GetEntityByTitle("Non-existent")
	if ok {
		t.Error("GetEntityByTitle should return false for non-existent")
	}
}

func TestUpdateEntityDescription(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	entity := mustAddEntity(t, store, "ent-001", "Test Entity", "person", "Original description", embedding)

	// Update description
	newEmbedding := make([]float32, testVectorDim)
	ok := store.UpdateEntityDescription(entity.ID, "Updated description", newEmbedding)
	if !ok {
		t.Error("UpdateEntityDescription should return true")
	}

	retrieved, _ := store.GetEntity(entity.ID)
	if retrieved.Description != "Updated description" {
		t.Errorf("Expected description 'Updated description', got '%s'", retrieved.Description)
	}

	// Update non-existent
	ok = store.UpdateEntityDescription(99999, "New description", newEmbedding)
	if ok {
		t.Error("UpdateEntityDescription should return false for non-existent ID")
	}
}

func TestDeleteEntity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	entity := mustAddEntity(t, store, "ent-001", "Test Entity", "person", "Description", embedding)

	// Delete
	ok := store.DeleteEntity(entity.ID)
	if !ok {
		t.Error("DeleteEntity should return true")
	}

	if store.EntityCount() != 0 {
		t.Errorf("Expected 0 entities after delete, got %d", store.EntityCount())
	}

	// Delete non-existent
	ok = store.DeleteEntity(99999)
	if ok {
		t.Error("DeleteEntity should return false for non-existent ID")
	}
}

func TestDeleteEntityRestrictedWhenLinkedToTextUnits(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	doc := mustAddDocument(t, store, "doc-001", "test.pdf")
	tu := mustAddTextUnit(t, store, "tu-001", doc.ID, "Content", embedding, 5)
	ent := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	if !store.LinkTextUnitToEntity(tu.ID, ent.ID) {
		t.Fatal("Expected link to succeed")
	}

	err := store.DeleteEntityChecked(ent.ID)
	if err == nil {
		t.Fatal("Expected linked entity delete to fail")
	}
	if !strings.Contains(err.Error(), "entity 1 is linked to text units") {
		t.Fatalf("Expected linked text units error, got %v", err)
	}
	if store.EntityCount() != 1 {
		t.Fatalf("Expected entity to remain, got %d entities", store.EntityCount())
	}
}

func TestDeleteEntityRestrictedWhenLinkedToRelationships(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)

	err := store.DeleteEntityChecked(e1.ID)
	if err == nil {
		t.Fatal("Expected relationship-linked entity delete to fail")
	}
	if !strings.Contains(err.Error(), "entity 1 is linked to relationships") {
		t.Fatalf("Expected linked relationships error, got %v", err)
	}
	if store.EntityCount() != 2 {
		t.Fatalf("Expected entities to remain, got %d entities", store.EntityCount())
	}
}

func TestDeleteEntityRestrictedWhenReferencedByCommunity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	ent := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	mustAddCommunity(t, store, "comm-001", "Community", "Summary", "Full", 0, []uint64{ent.ID}, nil, embedding)

	err := store.DeleteEntityChecked(ent.ID)
	if err == nil {
		t.Fatal("Expected entity referenced by community to fail")
	}
	if !strings.Contains(err.Error(), "entity 1 is referenced by communities") {
		t.Fatalf("Expected community dependency error, got %v", err)
	}
	if store.EntityCount() != 1 {
		t.Fatalf("Expected entity to remain, got %d entities", store.EntityCount())
	}
}

func TestListEntitiesPagination(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	for i := 0; i < 5; i++ {
		extID := fmt.Sprintf("ent-%d", i+1)
		title := fmt.Sprintf("Entity %d", i+1)
		if _, err := store.AddEntity(extID, title, "person", "desc", nil); err != nil {
			t.Fatalf("AddEntity failed: %v", err)
		}
	}

	entities, next := store.ListEntities(0, 2)
	if len(entities) != 2 {
		t.Fatalf("Expected 2 entities, got %d", len(entities))
	}
	if next == 0 {
		t.Fatalf("Expected non-zero next cursor")
	}
	if entities[0].ID >= entities[1].ID {
		t.Errorf("Expected ascending IDs, got %d then %d", entities[0].ID, entities[1].ID)
	}
	if next != entities[len(entities)-1].ID {
		t.Errorf("Expected next cursor %d, got %d", entities[len(entities)-1].ID, next)
	}

	entities2, next2 := store.ListEntities(next, 2)
	if len(entities2) != 2 {
		t.Fatalf("Expected 2 entities, got %d", len(entities2))
	}
	if next2 == 0 {
		t.Fatalf("Expected non-zero next cursor for page 2")
	}

	entities3, next3 := store.ListEntities(next2, 2)
	if len(entities3) != 1 {
		t.Fatalf("Expected 1 entity, got %d", len(entities3))
	}
	if next3 != 0 {
		t.Fatalf("Expected next cursor 0 at end, got %d", next3)
	}
}

// =============================================================================
// Relationship Operations Tests
// =============================================================================

func TestAddRelationship(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	rel, err := store.AddRelationship("rel-001", e1.ID, e2.ID, "KNOWS", "They know each other", 1.0)
	if err != nil {
		t.Fatalf("AddRelationship failed: %v", err)
	}

	if rel.ExternalID != "rel-001" {
		t.Errorf("Expected ExternalID 'rel-001', got '%s'", rel.ExternalID)
	}

	if rel.Type != "KNOWS" {
		t.Errorf("Expected Type 'KNOWS', got '%s'", rel.Type)
	}

	if rel.Weight != 1.0 {
		t.Errorf("Expected Weight 1.0, got %f", rel.Weight)
	}

	if store.RelationshipCount() != 1 {
		t.Errorf("Expected 1 relationship, got %d", store.RelationshipCount())
	}
}

func TestAddRelationshipAllowsMissingExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	rel, err := store.AddRelationship("", e1.ID, e2.ID, "KNOWS", "They know each other", 1.0)
	if err != nil {
		t.Fatalf("AddRelationship should allow missing external_id: %v", err)
	}
	if rel.ExternalID != "" {
		t.Fatalf("Expected empty relationship external_id, got %q", rel.ExternalID)
	}
}

func TestAddRelationshipDuplicate(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	_, err := store.AddRelationship("rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	if err != nil {
		t.Fatalf("First AddRelationship failed: %v", err)
	}

	// Try to add duplicate
	_, err = store.AddRelationship("rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	if err == nil {
		t.Error("Expected error when adding duplicate relationship")
	}
}

func TestAddRelationshipAllowsSameSourceTargetWithDifferentType(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	rel1 := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	rel2, err := store.AddRelationship("rel-002", e1.ID, e2.ID, "WORKS_WITH", "Desc", 1.0)
	if err != nil {
		t.Fatalf("Expected same source/target with different type to succeed: %v", err)
	}
	if rel1.ID == rel2.ID {
		t.Fatal("Expected relationships to have distinct IDs")
	}
	if store.RelationshipCount() != 2 {
		t.Fatalf("Expected 2 relationships, got %d", store.RelationshipCount())
	}
}

func TestAddRelationshipRejectsDuplicateSourceTargetType(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	_, err := store.AddRelationship("rel-002", e1.ID, e2.ID, "KNOWS", "Another desc", 1.0)
	if err == nil {
		t.Fatal("Expected duplicate relationship identity to fail")
	}
	if !strings.Contains(err.Error(), "relationship from 1 to 2 with type KNOWS already exists") {
		t.Fatalf("Expected duplicate relationship identity error, got %v", err)
	}
}

func TestAddRelationshipRequiresExistingEntities(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)

	_, err := store.AddRelationship("rel-001", e1.ID, 999, "KNOWS", "Desc", 1.0)
	if err == nil {
		t.Fatal("Expected missing target entity to fail")
	}
	if !strings.Contains(err.Error(), "relationship target_id 999 does not exist") {
		t.Fatalf("Expected missing target error, got %v", err)
	}

	_, err = store.AddRelationship("rel-002", 999, e1.ID, "KNOWS", "Desc", 1.0)
	if err == nil {
		t.Fatal("Expected missing source entity to fail")
	}
	if !strings.Contains(err.Error(), "relationship source_id 999 does not exist") {
		t.Fatalf("Expected missing source error, got %v", err)
	}
}

func TestGetRelationship(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	rel := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)

	// Get by ID
	retrieved, ok := store.GetRelationship(rel.ID)
	if !ok {
		t.Error("GetRelationship should find the relationship")
	}

	if retrieved.Type != "KNOWS" {
		t.Errorf("Expected Type 'KNOWS', got '%s'", retrieved.Type)
	}

	// Get non-existent
	_, ok = store.GetRelationship(99999)
	if ok {
		t.Error("GetRelationship should return false for non-existent ID")
	}
}

func TestDeleteRelationship(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	rel := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)

	// Delete
	ok := store.DeleteRelationship(rel.ID)
	if !ok {
		t.Error("DeleteRelationship should return true")
	}

	if store.RelationshipCount() != 0 {
		t.Errorf("Expected 0 relationships after delete, got %d", store.RelationshipCount())
	}

	// Delete non-existent
	ok = store.DeleteRelationship(99999)
	if ok {
		t.Error("DeleteRelationship should return false for non-existent ID")
	}
}

func TestDeleteRelationshipRestrictedWhenReferencedByCommunity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	rel := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	mustAddCommunity(t, store, "comm-001", "Community", "Summary", "Full", 0, []uint64{e1.ID, e2.ID}, []uint64{rel.ID}, embedding)

	err := store.DeleteRelationshipChecked(rel.ID)
	if err == nil {
		t.Fatal("Expected relationship referenced by community to fail")
	}
	if !strings.Contains(err.Error(), "relationship 1 is referenced by communities") {
		t.Fatalf("Expected community dependency error, got %v", err)
	}
	if store.RelationshipCount() != 1 {
		t.Fatalf("Expected relationship to remain, got %d relationships", store.RelationshipCount())
	}
}

func TestListRelationshipsPagination(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	e3 := mustAddEntity(t, store, "ent-003", "Entity 3", "person", "Desc", embedding)

	if _, err := store.AddRelationship("rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0); err != nil {
		t.Fatalf("AddRelationship failed: %v", err)
	}
	if _, err := store.AddRelationship("rel-002", e2.ID, e3.ID, "KNOWS", "Desc", 1.0); err != nil {
		t.Fatalf("AddRelationship failed: %v", err)
	}
	if _, err := store.AddRelationship("rel-003", e3.ID, e1.ID, "KNOWS", "Desc", 1.0); err != nil {
		t.Fatalf("AddRelationship failed: %v", err)
	}

	rels, next := store.ListRelationships(0, 2)
	if len(rels) != 2 {
		t.Fatalf("Expected 2 relationships, got %d", len(rels))
	}
	if next == 0 {
		t.Fatalf("Expected non-zero next cursor")
	}
	if rels[0].ID >= rels[1].ID {
		t.Errorf("Expected ascending IDs, got %d then %d", rels[0].ID, rels[1].ID)
	}
	if next != rels[len(rels)-1].ID {
		t.Errorf("Expected next cursor %d, got %d", rels[len(rels)-1].ID, next)
	}

	rels2, next2 := store.ListRelationships(next, 2)
	if len(rels2) != 1 {
		t.Fatalf("Expected 1 relationship, got %d", len(rels2))
	}
	if next2 != 0 {
		t.Fatalf("Expected next cursor 0 at end, got %d", next2)
	}
}

func TestGetOutgoingEdges(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	e3 := mustAddEntity(t, store, "ent-003", "Entity 3", "person", "Desc", embedding)

	mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)
	mustAddRelationship(t, store, "rel-002", e1.ID, e3.ID, "KNOWS", "Desc", 1.0)

	rels := store.GetOutgoingRelationships(e1.ID)
	if len(rels) != 2 {
		t.Errorf("Expected 2 outgoing relationships, got %d", len(rels))
	}
}

func TestGetIncomingEdges(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	e3 := mustAddEntity(t, store, "ent-003", "Entity 3", "person", "Desc", embedding)

	mustAddRelationship(t, store, "rel-001", e1.ID, e3.ID, "KNOWS", "Desc", 1.0)
	mustAddRelationship(t, store, "rel-002", e2.ID, e3.ID, "KNOWS", "Desc", 1.0)

	rels := store.GetIncomingRelationships(e3.ID)
	if len(rels) != 2 {
		t.Errorf("Expected 2 incoming relationships, got %d", len(rels))
	}
}

// =============================================================================
// Community Operations Tests
// =============================================================================

func TestAddCommunity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	comm, err := store.AddCommunity("comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID, e2.ID}, []uint64{}, embedding)
	if err != nil {
		t.Fatalf("AddCommunity failed: %v", err)
	}

	if comm.ExternalID != "comm-001" {
		t.Errorf("Expected ExternalID 'comm-001', got '%s'", comm.ExternalID)
	}

	if comm.Title != "Test Community" {
		t.Errorf("Expected Title 'Test Community', got '%s'", comm.Title)
	}

	if len(comm.EntityIDs) != 2 {
		t.Errorf("Expected 2 entities, got %d", len(comm.EntityIDs))
	}

	if store.CommunityCount() != 1 {
		t.Errorf("Expected 1 community, got %d", store.CommunityCount())
	}
}

func TestAddCommunityAllowsMissingExternalID(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)

	comm, err := store.AddCommunity("", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID, e2.ID}, nil, embedding)
	if err != nil {
		t.Fatalf("AddCommunity should allow missing external_id: %v", err)
	}
	if comm.ExternalID != "" {
		t.Fatalf("Expected empty community external_id, got %q", comm.ExternalID)
	}
}

func TestAddCommunityRequiresExistingReferences(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)

	_, err := store.AddCommunity("comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID, 999}, nil, embedding)
	if err == nil {
		t.Fatal("Expected missing community entity reference to fail")
	}
	if !strings.Contains(err.Error(), "community entity_id 999 does not exist") {
		t.Fatalf("Expected missing entity reference error, got %v", err)
	}

	_, err = store.AddCommunity("comm-002", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID}, []uint64{999}, embedding)
	if err == nil {
		t.Fatal("Expected missing community relationship reference to fail")
	}
	if !strings.Contains(err.Error(), "community relationship_id 999 does not exist") {
		t.Fatalf("Expected missing relationship reference error, got %v", err)
	}
	if store.CommunityCount() != 0 {
		t.Fatalf("Expected no community to be created, got %d", store.CommunityCount())
	}
}

func TestAddCommunityAllowsEmptyEmbedding(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)

	comm, err := store.AddCommunity("comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID}, nil, nil)
	if err != nil {
		t.Fatalf("AddCommunity should allow empty embedding: %v", err)
	}
	if comm.ID == 0 {
		t.Fatal("Expected community ID")
	}
	if store.GetCommunityIndex().Count() != 0 {
		t.Fatal("Expected empty embedding community to remain non-indexed")
	}
}

func TestAddCommunityRejectsInvalidEmbedding(t *testing.T) {
	tests := []struct {
		name      string
		embedding []float32
		want      string
	}{
		{
			name:      "wrong dimension",
			embedding: make([]float32, testVectorDim-1),
			want:      "community embedding dimension mismatch",
		},
		{
			name:      "NaN",
			embedding: append([]float32{float32(math.NaN())}, make([]float32, testVectorDim-1)...),
			want:      "community embedding contains non-finite value at index 0",
		},
		{
			name:      "Inf",
			embedding: append([]float32{float32(math.Inf(1))}, make([]float32, testVectorDim-1)...),
			want:      "community embedding contains non-finite value at index 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSessionStore("test-session", testVectorDim)
			entity := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", make([]float32, testVectorDim))

			_, err := store.AddCommunity("comm-001", "Test Community", "Summary", "Full content", 0, []uint64{entity.ID}, nil, tt.embedding)
			if err == nil {
				t.Fatal("Expected AddCommunity to reject invalid embedding")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Expected error containing %q, got %v", tt.want, err)
			}
			if store.CommunityCount() != 0 {
				t.Fatalf("Expected no community to be created, got %d", store.CommunityCount())
			}
		})
	}
}

func TestGetCommunityAfterValidatedReferences(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc", embedding)
	rel := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)

	comm := mustAddCommunity(t, store, "comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID, e2.ID}, []uint64{rel.ID}, embedding)
	retrieved, ok := store.GetCommunity(comm.ID)
	if !ok {
		t.Fatal("Expected community to be retrievable after validation")
	}
	if len(retrieved.EntityIDs) != 2 {
		t.Fatalf("Expected 2 community entity references, got %d", len(retrieved.EntityIDs))
	}
	if len(retrieved.RelationshipIDs) != 1 || retrieved.RelationshipIDs[0] != rel.ID {
		t.Fatalf("Expected relationship reference %d, got %v", rel.ID, retrieved.RelationshipIDs)
	}
}

func TestGetCommunity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	comm := mustAddCommunity(t, store, "comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID}, []uint64{}, embedding)

	// Get by ID
	retrieved, ok := store.GetCommunity(comm.ID)
	if !ok {
		t.Error("GetCommunity should find the community")
	}

	if retrieved.Title != "Test Community" {
		t.Errorf("Expected Title 'Test Community', got '%s'", retrieved.Title)
	}

	// Get non-existent
	_, ok = store.GetCommunity(99999)
	if ok {
		t.Error("GetCommunity should return false for non-existent ID")
	}
}

func TestDeleteCommunity(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	embedding := make([]float32, testVectorDim)
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc", embedding)
	comm := mustAddCommunity(t, store, "comm-001", "Test Community", "Summary", "Full content", 0, []uint64{e1.ID}, []uint64{}, embedding)

	// Delete
	ok := store.DeleteCommunity(comm.ID)
	if !ok {
		t.Error("DeleteCommunity should return true")
	}

	if store.CommunityCount() != 0 {
		t.Errorf("Expected 0 communities after delete, got %d", store.CommunityCount())
	}

	// Delete non-existent
	ok = store.DeleteCommunity(99999)
	if ok {
		t.Error("DeleteCommunity should return false for non-existent ID")
	}
}

// =============================================================================
// Vector Index Tests
// =============================================================================

func TestGetTextUnitIndex(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	idx := store.GetTextUnitIndex()
	if idx == nil {
		t.Fatal("GetTextUnitIndex returned nil")
	}

	// Should return the same index on subsequent calls
	idx2 := store.GetTextUnitIndex()
	if idx != idx2 {
		t.Error("GetTextUnitIndex should return the same index instance")
	}
}

func TestGetEntityIndex(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	idx := store.GetEntityIndex()
	if idx == nil {
		t.Fatal("GetEntityIndex returned nil")
	}

	// Should return the same index on subsequent calls
	idx2 := store.GetEntityIndex()
	if idx != idx2 {
		t.Error("GetEntityIndex should return the same index instance")
	}
}

func TestGetCommunityIndex(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	idx := store.GetCommunityIndex()
	if idx == nil {
		t.Fatal("GetCommunityIndex returned nil")
	}

	// Should return the same index on subsequent calls
	idx2 := store.GetCommunityIndex()
	if idx != idx2 {
		t.Error("GetCommunityIndex should return the same index instance")
	}
}

// =============================================================================
// ID Generator Tests
// =============================================================================

func TestIDGenerator(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	idGen := store.GetIDGenerator()
	if idGen == nil {
		t.Fatal("GetIDGenerator returned nil")
	}

	// IDs should be sequential
	id1 := idGen.NextDocumentID()
	id2 := idGen.NextDocumentID()

	if id2 <= id1 {
		t.Errorf("IDs should be sequential: %d, %d", id1, id2)
	}
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrentDocumentOperations(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	const numOps = 100
	done := make(chan bool, numOps)

	// Concurrent adds
	for i := 0; i < numOps; i++ {
		go func(id int) {
			extID := string(rune('A'+(id%26))) + string(rune('0'+(id/26)))
			_, err := store.AddDocument(extID, "test.pdf")
			if err != nil {
				t.Logf("Duplicate document: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all operations
	for i := 0; i < numOps; i++ {
		<-done
	}

	// Should have some documents
	if store.DocumentCount() == 0 {
		t.Error("Expected some documents after concurrent adds")
	}
}

func TestConcurrentEntityOperations(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	const numOps = 50
	done := make(chan bool, numOps)

	// Concurrent adds
	for i := 0; i < numOps; i++ {
		go func(id int) {
			extID := string(rune('A'+(id%26))) + string(rune('0'+(id/26)))
			embedding := make([]float32, testVectorDim)
			_, err := store.AddEntity(extID, "Entity "+extID, "person", "Desc", embedding)
			if err != nil {
				t.Logf("Duplicate entity: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all operations
	for i := 0; i < numOps; i++ {
		<-done
	}

	// Should have some entities
	if store.EntityCount() == 0 {
		t.Error("Expected some entities after concurrent adds")
	}
}

// =============================================================================
// Integration Tests
// =============================================================================

func TestCompleteWorkflow(t *testing.T) {
	store := NewSessionStore("test-session", testVectorDim)

	// Add documents
	doc1 := mustAddDocument(t, store, "doc-001", "test1.pdf")
	doc2 := mustAddDocument(t, store, "doc-002", "test2.pdf")

	// Add text units
	embedding := make([]float32, testVectorDim)
	tu1 := mustAddTextUnit(t, store, "tu-001", doc1.ID, "Content 1", embedding, 5)
	tu2 := mustAddTextUnit(t, store, "tu-002", doc2.ID, "Content 2", embedding, 5)

	// Add entities
	e1 := mustAddEntity(t, store, "ent-001", "Entity 1", "person", "Desc 1", embedding)
	e2 := mustAddEntity(t, store, "ent-002", "Entity 2", "person", "Desc 2", embedding)

	// Link text units to entities
	store.LinkTextUnitToEntity(tu1.ID, e1.ID)
	store.LinkTextUnitToEntity(tu2.ID, e2.ID)

	// Add relationship
	rel := mustAddRelationship(t, store, "rel-001", e1.ID, e2.ID, "KNOWS", "Desc", 1.0)

	// Add community
	comm := mustAddCommunity(t, store, "comm-001", "Community 1", "Summary", "Full content", 0, []uint64{e1.ID, e2.ID}, []uint64{rel.ID}, embedding)

	// Verify counts
	info := store.GetInfo()
	if info.DocumentCount != 2 {
		t.Errorf("Expected 2 documents, got %d", info.DocumentCount)
	}
	if info.TextUnitCount != 2 {
		t.Errorf("Expected 2 text units, got %d", info.TextUnitCount)
	}
	if info.EntityCount != 2 {
		t.Errorf("Expected 2 entities, got %d", info.EntityCount)
	}
	if info.RelationshipCount != 1 {
		t.Errorf("Expected 1 relationship, got %d", info.RelationshipCount)
	}
	if info.CommunityCount != 1 {
		t.Errorf("Expected 1 community, got %d", info.CommunityCount)
	}

	// Clean up complete workflow state without testing delete ordering here.
	_ = comm
	_ = rel
	_ = e1
	_ = e2
	_ = tu1
	_ = tu2
	_ = doc1
	_ = doc2
	store.Clear()

	// Verify empty
	info = store.GetInfo()
	if info.DocumentCount != 0 {
		t.Errorf("Expected 0 documents after cleanup, got %d", info.DocumentCount)
	}
	if info.TextUnitCount != 0 {
		t.Errorf("Expected 0 text units after cleanup, got %d", info.TextUnitCount)
	}
	if info.EntityCount != 0 {
		t.Errorf("Expected 0 entities after cleanup, got %d", info.EntityCount)
	}
}
