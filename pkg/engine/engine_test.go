// Package engine provides the query engine tests
package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gibram-io/gibram/pkg/graph"
	"github.com/gibram-io/gibram/pkg/store"
	"github.com/gibram-io/gibram/pkg/types"
)

// =============================================================================
// Test Helpers
// =============================================================================

const testVectorDim = 64

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(i%10) / 10.0
	}
	return v
}

func createTestEngine() *Engine {
	return NewEngine(testVectorDim)
}

const testSessionID = "test-session-1"

// =============================================================================
// Engine Creation Tests
// =============================================================================

func TestNewEngine(t *testing.T) {
	e := NewEngine(128)

	if e == nil {
		t.Fatal("NewEngine returned nil")
	}

	if e.vectorDim != 128 {
		t.Errorf("Expected vectorDim 128, got %d", e.vectorDim)
	}

	// Verify session-based architecture
	if e.sessions == nil {
		t.Error("sessions map is nil")
	}

	info := e.Info()
	if info.SessionStoreMode != "ephemeral" {
		t.Errorf("expected default session store mode ephemeral, got %s", info.SessionStoreMode)
	}
	if !info.SessionStoreHealthy {
		t.Error("expected default session store to be healthy")
	}
}

func TestNewEngineWithOptions_DurableModeInfo(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:       walDir,
			SyncPolicy:   "periodic",
			SyncInterval: 250 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewEngineWithOptions durable failed: %v", err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			t.Fatalf("engine close failed: %v", err)
		}
	}()

	info := e.Info()
	if info.SessionStoreMode != "durable" {
		t.Errorf("expected durable mode, got %s", info.SessionStoreMode)
	}
	if info.WALSyncPolicy != "periodic" {
		t.Errorf("expected periodic WAL policy, got %s", info.WALSyncPolicy)
	}
	if info.WALSyncIntervalMS != 250 {
		t.Errorf("expected 250ms WAL interval, got %dms", info.WALSyncIntervalMS)
	}
	if !info.SessionStoreHealthy {
		t.Error("expected durable session store to be healthy")
	}
}

// =============================================================================
// Document Operations Tests
// =============================================================================

func TestEngine_AddDocument(t *testing.T) {
	e := createTestEngine()

	doc, err := e.AddDocument(testSessionID, "ext-doc-1", "test.txt")
	if err != nil {
		t.Fatalf("AddDocument failed: %v", err)
	}

	if doc.ID == 0 {
		t.Error("Document ID should not be 0")
	}
	if doc.ExternalID != "ext-doc-1" {
		t.Errorf("Expected ExternalID 'ext-doc-1', got '%s'", doc.ExternalID)
	}
	if doc.Filename != "test.txt" {
		t.Errorf("Expected Filename 'test.txt', got '%s'", doc.Filename)
	}
	if doc.Status != types.DocStatusUploaded {
		t.Errorf("Expected status Uploaded, got %s", doc.Status)
	}
}

func TestEngine_DurableDocumentSurvivesRestart(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	if err != nil {
		t.Fatalf("create durable engine: %v", err)
	}

	doc, err := e.AddDocument(testSessionID, "durable-doc-1", "durable.txt")
	if err != nil {
		t.Fatalf("AddDocument durable failed: %v", err)
	}
	walBytes, err := os.ReadFile(filepath.Join(walDir, "session.wal"))
	if err != nil {
		t.Fatalf("read session WAL: %v", err)
	}
	if !bytes.Contains(walBytes, []byte(`"op":"add_document"`)) {
		t.Fatalf("expected add_document WAL record, got %s", string(walBytes))
	}
	if !bytes.Contains(walBytes, []byte(`"external_id":"durable-doc-1"`)) {
		t.Fatalf("expected canonical document in WAL, got %s", string(walBytes))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	recovered, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	if err != nil {
		t.Fatalf("recover durable engine: %v", err)
	}
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	got, ok := recovered.GetDocument(testSessionID, doc.ID)
	if !ok {
		t.Fatalf("expected recovered document %d", doc.ID)
	}
	if got.ExternalID != "durable-doc-1" || got.Filename != "durable.txt" {
		t.Fatalf("unexpected recovered document: %+v", got)
	}
}

func TestEngine_EphemeralDocumentWriteDoesNotCreateSessionWAL(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "ephemeral",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	if _, err := e.AddDocument(testSessionID, "ephemeral-doc-1", "ephemeral.txt"); err != nil {
		t.Fatalf("AddDocument ephemeral failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(walDir, "session.wal")); !os.IsNotExist(err) {
		t.Fatalf("expected no session WAL in ephemeral mode, stat err=%v", err)
	}
}

func TestEngine_DurableTextUnitAndEntitySurviveRestartWithVectorSearch(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	doc := mustAddDocument(t, e, testSessionID, "durable-doc-graph", "graph.txt")
	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "durable-tu-1", doc.ID, "Durable text unit", embedding, 3)
	ent := mustAddEntity(t, e, testSessionID, "durable-ent-1", "Durable Entity", "concept", "Durable entity description", embedding)
	walBytes, err := os.ReadFile(filepath.Join(walDir, "session.wal"))
	if err != nil {
		t.Fatalf("read session WAL: %v", err)
	}
	if !bytes.Contains(walBytes, []byte(`"op":"add_textunit"`)) || !bytes.Contains(walBytes, []byte(`"text_unit_embedding"`)) {
		t.Fatalf("expected textunit canonical data and embedding in WAL, got %s", string(walBytes))
	}
	if !bytes.Contains(walBytes, []byte(`"op":"add_entity"`)) || !bytes.Contains(walBytes, []byte(`"entity_embedding"`)) {
		t.Fatalf("expected entity canonical data and embedding in WAL, got %s", string(walBytes))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	gotTU, ok := recovered.GetTextUnit(testSessionID, tu.ID)
	if !ok {
		t.Fatalf("expected recovered textunit %d", tu.ID)
	}
	if gotTU.DocumentID != doc.ID || gotTU.Content != "Durable text unit" {
		t.Fatalf("unexpected recovered textunit: %+v", gotTU)
	}
	gotEnt, ok := recovered.GetEntity(testSessionID, ent.ID)
	if !ok {
		t.Fatalf("expected recovered entity %d", ent.ID)
	}
	if gotEnt.ExternalID != "durable-ent-1" {
		t.Fatalf("unexpected recovered entity: %+v", gotEnt)
	}

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity}
	spec.KHops = 0
	spec.TopK = 1

	result, err := recovered.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("query after durable replay failed: %v", err)
	}
	if result.Stats.TextUnitsSearched != 1 || result.Stats.EntitiesSearched != 1 {
		t.Fatalf("expected restored vector indexes to be searchable, got stats %+v", result.Stats)
	}
	if len(result.TextUnits) != 1 || result.TextUnits[0].TextUnit.ID != tu.ID {
		t.Fatalf("expected recovered textunit vector result %d, got %+v", tu.ID, result.TextUnits)
	}
	if len(result.Entities) != 1 || result.Entities[0].Entity.ID != ent.ID {
		t.Fatalf("expected recovered entity vector result %d, got %+v", ent.ID, result.Entities)
	}
}

func TestEngine_DurableRelationshipsAndRestrictedDeletesSurviveRestart(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	docToDelete := mustAddDocument(t, e, testSessionID, "durable-doc-delete", "delete.txt")
	docWithDependency := mustAddDocument(t, e, testSessionID, "durable-doc-dependent", "dependent.txt")
	mustAddTextUnit(t, e, testSessionID, "durable-tu-dependent", docWithDependency.ID, "dependent", nil, 1)
	source := mustAddEntity(t, e, testSessionID, "durable-source", "Durable Source", "concept", "source", nil)
	target := mustAddEntity(t, e, testSessionID, "durable-target", "Durable Target", "concept", "target", nil)
	entityToDelete := mustAddEntity(t, e, testSessionID, "durable-entity-delete", "Durable Entity Delete", "concept", "delete", nil)
	relSurvive := mustAddRelationship(t, e, testSessionID, "durable-rel-survive", source.ID, target.ID, "RELATED", "survives restart", 1.0)
	relDelete := mustAddRelationship(t, e, testSessionID, "durable-rel-delete", target.ID, entityToDelete.ID, "RELATED", "deleted before restart", 1.0)

	if err := e.DeleteEntityChecked(testSessionID, source.ID); err == nil {
		t.Fatal("expected restricted delete to reject entity with relationship")
	}
	if err := e.DeleteDocumentChecked(testSessionID, docWithDependency.ID); err == nil {
		t.Fatal("expected restricted delete to reject document with textunit")
	}
	if err := e.DeleteRelationshipChecked(testSessionID, relDelete.ID); err != nil {
		t.Fatalf("delete relationship failed: %v", err)
	}
	if err := e.DeleteEntityChecked(testSessionID, entityToDelete.ID); err != nil {
		t.Fatalf("delete now-unlinked entity failed: %v", err)
	}
	if err := e.DeleteDocumentChecked(testSessionID, docToDelete.ID); err != nil {
		t.Fatalf("delete document failed: %v", err)
	}

	walBytes, err := os.ReadFile(filepath.Join(walDir, "session.wal"))
	if err != nil {
		t.Fatalf("read session WAL: %v", err)
	}
	for _, expected := range []string{`"op":"add_relationship"`, `"op":"delete_relationship"`, `"op":"delete_entity"`, `"op":"delete_document"`} {
		if !bytes.Contains(walBytes, []byte(expected)) {
			t.Fatalf("expected %s in WAL, got %s", expected, string(walBytes))
		}
	}
	if bytes.Contains(walBytes, []byte(fmt.Sprintf(`"op":"delete_entity","session_id":"%s","id":%d`, testSessionID, source.ID))) {
		t.Fatal("restricted entity delete should not have appended a WAL record")
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	if _, ok := recovered.GetRelationship(testSessionID, relSurvive.ID); !ok {
		t.Fatalf("expected relationship %d to survive restart", relSurvive.ID)
	}
	if _, ok := recovered.GetRelationship(testSessionID, relDelete.ID); ok {
		t.Fatalf("expected relationship %d to remain deleted after restart", relDelete.ID)
	}
	if _, ok := recovered.GetEntity(testSessionID, entityToDelete.ID); ok {
		t.Fatalf("expected entity %d to remain deleted after restart", entityToDelete.ID)
	}
	if _, ok := recovered.GetDocument(testSessionID, docToDelete.ID); ok {
		t.Fatalf("expected document %d to remain deleted after restart", docToDelete.ID)
	}
	if err := recovered.DeleteEntityChecked(testSessionID, source.ID); err == nil {
		t.Fatal("expected replayed relationship to preserve restricted entity delete invariant")
	}
	if _, err := recovered.AddRelationship(testSessionID, "duplicate-rel", source.ID, target.ID, "RELATED", "duplicate", 1.0); err == nil {
		t.Fatal("expected replayed relationship identity to reject duplicate source/target/type")
	}
}

func TestEngine_DurableAtomicBulkWritesSurviveRestart(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	docIDs, err := e.MSetDocuments(testSessionID, []types.BulkDocumentInput{
		{ExternalID: "bulk-doc-1", Filename: "one.txt"},
		{ExternalID: "bulk-doc-2", Filename: "two.txt"},
	})
	if err != nil {
		t.Fatalf("MSetDocuments durable failed: %v", err)
	}
	embedding := randomVector(testVectorDim)
	tuIDs, err := e.MSetTextUnits(testSessionID, []types.BulkTextUnitInput{
		{ExternalID: "bulk-tu-1", DocumentID: docIDs[0], Content: "one", Embedding: embedding, TokenCount: 1},
		{ExternalID: "bulk-tu-2", DocumentID: docIDs[1], Content: "two", Embedding: nil, TokenCount: 1},
	})
	if err != nil {
		t.Fatalf("MSetTextUnits durable failed: %v", err)
	}
	entityIDs, err := e.MSetEntities(testSessionID, []types.BulkEntityInput{
		{ExternalID: "bulk-ent-1", Title: "Bulk Entity One", Type: "concept", Description: "one", Embedding: embedding},
		{ExternalID: "bulk-ent-2", Title: "Bulk Entity Two", Type: "concept", Description: "two", Embedding: nil},
	})
	if err != nil {
		t.Fatalf("MSetEntities durable failed: %v", err)
	}
	relIDs, err := e.MSetRelationships(testSessionID, []types.BulkRelationshipInput{
		{ExternalID: "bulk-rel-1", SourceID: entityIDs[0], TargetID: entityIDs[1], Type: "RELATED", Description: "one to two", Weight: 1},
	})
	if err != nil {
		t.Fatalf("MSetRelationships durable failed: %v", err)
	}

	walBytes, err := os.ReadFile(filepath.Join(walDir, "session.wal"))
	if err != nil {
		t.Fatalf("read session WAL: %v", err)
	}
	for _, op := range []string{`"op":"mset_documents"`, `"op":"mset_textunits"`, `"op":"mset_entities"`, `"op":"mset_relationships"`} {
		if bytes.Count(walBytes, []byte(op)) != 1 {
			t.Fatalf("expected exactly one %s batch WAL record, got WAL %s", op, string(walBytes))
		}
	}
	if bytes.Contains(walBytes, []byte(`"op":"add_textunit"`)) || bytes.Contains(walBytes, []byte(`"op":"add_entity"`)) {
		t.Fatalf("expected bulk writes to use batch WAL records only, got %s", string(walBytes))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	info, err := recovered.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession after replay failed: %v", err)
	}
	if info.DocumentCount != 2 || info.TextUnitCount != 2 || info.EntityCount != 2 || info.RelationshipCount != 1 {
		t.Fatalf("unexpected replayed counts: %+v", info)
	}
	if _, ok := recovered.GetTextUnit(testSessionID, tuIDs[0]); !ok {
		t.Fatalf("expected replayed textunit %d", tuIDs[0])
	}
	if _, ok := recovered.GetRelationship(testSessionID, relIDs[0]); !ok {
		t.Fatalf("expected replayed relationship %d", relIDs[0])
	}

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity}
	spec.KHops = 0
	result, err := recovered.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("query after bulk replay failed: %v", err)
	}
	if result.Stats.TextUnitsSearched != 1 || result.Stats.EntitiesSearched != 1 {
		t.Fatalf("expected replayed bulk embeddings to rebuild indexes, got stats %+v", result.Stats)
	}
}

func TestEngine_DurableInvalidBulkWritesDoNotAppendOrMutate(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	defer func() {
		if err := e.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()

	_, err := e.MSetDocuments(testSessionID, []types.BulkDocumentInput{
		{ExternalID: "duplicate-doc", Filename: "one.txt"},
		{ExternalID: "duplicate-doc", Filename: "two.txt"},
	})
	if err == nil {
		t.Fatal("expected duplicate document bulk to fail")
	}
	info := e.Info()
	if info.DocumentCount != 0 {
		t.Fatalf("invalid bulk should not mutate memory, got %d documents", info.DocumentCount)
	}
	walSize, err := e.durable.wal.Size()
	if err != nil {
		t.Fatalf("read session WAL size: %v", err)
	}
	if walSize != 0 {
		walBytes, _ := os.ReadFile(filepath.Join(walDir, "session.wal"))
		t.Fatalf("invalid bulk should not append WAL, got %s", string(walBytes))
	}
}

func TestEngine_DurableCommunityReplacementSurvivesRestart(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	ent1 := mustAddEntity(t, e, testSessionID, "comm-ent-1", "Community Entity One", "concept", "one", nil)
	ent2 := mustAddEntity(t, e, testSessionID, "comm-ent-2", "Community Entity Two", "concept", "two", nil)
	ent3 := mustAddEntity(t, e, testSessionID, "comm-ent-3", "Community Entity Three", "concept", "three", nil)
	mustAddRelationship(t, e, testSessionID, "comm-rel-1", ent1.ID, ent2.ID, "RELATED", "one two", 1)
	mustAddRelationship(t, e, testSessionID, "comm-rel-2", ent2.ID, ent3.ID, "RELATED", "two three", 1)

	cfg := graph.DefaultLeidenConfig()
	cfg.RandomSeed = 7
	first, err := e.ComputeCommunities(testSessionID, cfg)
	if err != nil {
		t.Fatalf("ComputeCommunities durable failed: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("expected first community replacement to create communities")
	}
	firstIDs := make(map[uint64]struct{}, len(first))
	for _, comm := range first {
		firstIDs[comm.ID] = struct{}{}
	}

	cfg.MaxLevels = 1
	cfg.MinCommunitySize = 2
	second, err := e.ComputeHierarchicalCommunities(testSessionID, cfg)
	if err != nil {
		t.Fatalf("ComputeHierarchicalCommunities durable failed: %v", err)
	}
	secondIDs := make(map[uint64]struct{}, len(second))
	for _, comm := range second {
		secondIDs[comm.ID] = struct{}{}
	}

	walBytes, err := os.ReadFile(filepath.Join(walDir, "session.wal"))
	if err != nil {
		t.Fatalf("read session WAL: %v", err)
	}
	if bytes.Count(walBytes, []byte(`"op":"replace_communities"`)) != 2 {
		t.Fatalf("expected two community replacement WAL records, got %s", string(walBytes))
	}
	if !bytes.Contains(walBytes, []byte(`"communities"`)) || !bytes.Contains(walBytes, []byte(`"community_embeddings"`)) {
		t.Fatalf("expected canonical communities and embeddings in WAL, got %s", string(walBytes))
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	info, err := recovered.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession after community replay failed: %v", err)
	}
	if info.CommunityCount != len(second) {
		t.Fatalf("expected final replacement count %d, got %d", len(second), info.CommunityCount)
	}
	for id := range firstIDs {
		if _, stillCurrent := secondIDs[id]; stillCurrent {
			continue
		}
		if _, ok := recovered.GetCommunity(testSessionID, id); ok {
			t.Fatalf("old community %d should not survive final replacement replay", id)
		}
	}
	for id := range secondIDs {
		if _, ok := recovered.GetCommunity(testSessionID, id); !ok {
			t.Fatalf("final community %d missing after replay", id)
		}
	}
}

func TestEngine_DurableStartupFailsClosedOnCorruptRecoveryInput(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	if err := os.MkdirAll(walDir, 0750); err != nil {
		t.Fatalf("create wal dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "session.wal"), []byte("{not-json}\n"), 0640); err != nil {
		t.Fatalf("write corrupt wal: %v", err)
	}

	_, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	if !errors.Is(err, ErrDurableRecovery) {
		t.Fatalf("expected durable recovery failure, got %v", err)
	}
}

func TestEngine_DurableStartupFailsClosedOnUnreplayableRecoveryInput(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	if err := os.MkdirAll(walDir, 0750); err != nil {
		t.Fatalf("create wal dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "session.wal"), []byte(`{"op":"unknown","session_id":"`+testSessionID+`"}`+"\n"), 0640); err != nil {
		t.Fatalf("write unreplayable wal: %v", err)
	}

	_, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})
	if !errors.Is(err, ErrDurableRecovery) {
		t.Fatalf("expected durable recovery failure, got %v", err)
	}
}

func TestEngine_EphemeralStartupIgnoresRecoveryStateAndStartsEmpty(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "session_wal")
	if err := os.MkdirAll(walDir, 0750); err != nil {
		t.Fatalf("create wal dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "session.wal"), []byte("{not-json}\n"), 0640); err != nil {
		t.Fatalf("write corrupt wal: %v", err)
	}

	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "ephemeral",
		Durable: DurableOptions{
			WALDir:     walDir,
			SyncPolicy: "every_write",
		},
	})

	info := e.Info()
	if info.SessionStoreMode != "ephemeral" || !info.SessionStoreHealthy {
		t.Fatalf("expected healthy ephemeral startup, got %+v", info)
	}
	if info.DocumentCount != 0 || info.SessionCount != 0 {
		t.Fatalf("expected empty ephemeral engine, got %+v", info)
	}
}

func TestEngine_DurableSnapshotRestoresCanonicalDataAndRebuildsIndexes(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})

	embedding := randomVector(testVectorDim)
	doc := mustAddDocument(t, e, testSessionID, "snap-doc-1", "snapshot.txt")
	tu := mustAddTextUnit(t, e, testSessionID, "snap-tu-1", doc.ID, "snapshot text", embedding, 2)
	ent := mustAddEntity(t, e, testSessionID, "snap-ent-1", "Snapshot Entity", "concept", "snapshot entity", embedding)
	comm := mustAddCommunity(t, e, testSessionID, "snap-comm-1", "Snapshot Community", "summary", "full", 0, []uint64{ent.ID}, nil, embedding)

	if _, err := e.CreateDurableSnapshot("", false); err != nil {
		t.Fatalf("CreateDurableSnapshot failed: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()

	if _, ok := recovered.GetTextUnit(testSessionID, tu.ID); !ok {
		t.Fatalf("expected restored textunit %d", tu.ID)
	}
	if _, ok := recovered.GetEntity(testSessionID, ent.ID); !ok {
		t.Fatalf("expected restored entity %d", ent.ID)
	}
	if _, ok := recovered.GetCommunity(testSessionID, comm.ID); !ok {
		t.Fatalf("expected restored community %d", comm.ID)
	}

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity, types.SearchTypeCommunity}
	spec.KHops = 0
	result, err := recovered.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("query after snapshot restore failed: %v", err)
	}
	if result.Stats.TextUnitsSearched != 1 || result.Stats.EntitiesSearched != 1 || result.Stats.CommunitiesSearched != 1 {
		t.Fatalf("expected indexes rebuilt from canonical embeddings, got stats %+v", result.Stats)
	}
}

func TestEngine_DurableAutoSnapshotByIntervalAndWALGrowth(t *testing.T) {
	t.Run("interval", func(t *testing.T) {
		root := t.TempDir()
		e := NewEngineWithOptionsMust(t, Options{
			VectorDim: testVectorDim,
			StoreMode: "durable",
			Durable: DurableOptions{
				WALDir:           filepath.Join(root, "wal"),
				SnapshotDir:      filepath.Join(root, "snapshots"),
				SyncPolicy:       "every_write",
				SnapshotInterval: time.Nanosecond,
			},
		})
		defer func() {
			if err := e.Close(); err != nil {
				t.Fatalf("close engine: %v", err)
			}
		}()
		mustAddDocument(t, e, testSessionID, "auto-interval-doc", "interval.txt")
		files, err := filepath.Glob(filepath.Join(root, "snapshots", "snapshot-*.json"))
		if err != nil || len(files) == 0 {
			t.Fatalf("expected interval snapshot, files=%v err=%v", files, err)
		}
	})

	t.Run("wal growth", func(t *testing.T) {
		root := t.TempDir()
		e := NewEngineWithOptionsMust(t, Options{
			VectorDim: testVectorDim,
			StoreMode: "durable",
			Durable: DurableOptions{
				WALDir:               filepath.Join(root, "wal"),
				SnapshotDir:          filepath.Join(root, "snapshots"),
				SyncPolicy:           "every_write",
				SnapshotWALSizeBytes: 1,
			},
		})
		defer func() {
			if err := e.Close(); err != nil {
				t.Fatalf("close engine: %v", err)
			}
		}()
		mustAddDocument(t, e, testSessionID, "auto-size-doc", "size.txt")
		files, err := filepath.Glob(filepath.Join(root, "snapshots", "snapshot-*.json"))
		if err != nil || len(files) == 0 {
			t.Fatalf("expected WAL-size snapshot, files=%v err=%v", files, err)
		}
	})
}

func TestEngine_DurableWALTruncationAfterSnapshotAndRecovery(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	mustAddDocument(t, e, testSessionID, "truncate-doc-before", "before.txt")
	sizeBefore, err := e.durable.wal.Size()
	if err != nil || sizeBefore == 0 {
		t.Fatalf("expected non-empty WAL before snapshot, size=%d err=%v", sizeBefore, err)
	}
	if _, err := e.CreateDurableSnapshot("", true); err != nil {
		t.Fatalf("CreateDurableSnapshot truncate failed: %v", err)
	}
	sizeAfter, err := e.durable.wal.Size()
	if err != nil || sizeAfter != 0 {
		t.Fatalf("expected truncated WAL after successful snapshot, size=%d err=%v", sizeAfter, err)
	}
	mustAddDocument(t, e, testSessionID, "truncate-doc-after", "after.txt")
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()
	info, err := recovered.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession failed: %v", err)
	}
	if info.DocumentCount != 2 {
		t.Fatalf("expected snapshot+post-snapshot WAL recovery to restore 2 docs, got %+v", info)
	}
}

func TestEngine_DurableSnapshotReplayCursorIsGenerationAware(t *testing.T) {
	testCases := []struct {
		name             string
		beforeFilename   string
		postSnapshotDocs int
	}{
		{
			name:             "new WAL smaller than previous generation offset",
			beforeFilename:   strings.Repeat("before-", 200),
			postSnapshotDocs: 1,
		},
		{
			name:             "new WAL larger than previous generation offset",
			beforeFilename:   "before.txt",
			postSnapshotDocs: 20,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			walDir := filepath.Join(root, "wal")
			snapshotDir := filepath.Join(root, "snapshots")
			e := NewEngineWithOptionsMust(t, Options{
				VectorDim: testVectorDim,
				StoreMode: "durable",
				Durable: DurableOptions{
					WALDir:      walDir,
					SnapshotDir: snapshotDir,
					SyncPolicy:  "every_write",
				},
			})
			mustAddDocument(t, e, testSessionID, "generation-before", tc.beforeFilename)
			if _, err := e.CreateDurableSnapshot("", true); err != nil {
				t.Fatalf("CreateDurableSnapshot failed: %v", err)
			}

			snapshotPaths, err := filepath.Glob(filepath.Join(snapshotDir, "snapshot-*.json"))
			if err != nil || len(snapshotPaths) != 1 {
				t.Fatalf("expected one snapshot, paths=%v err=%v", snapshotPaths, err)
			}
			snapshotBytes, err := os.ReadFile(snapshotPaths[0])
			if err != nil {
				t.Fatalf("read snapshot: %v", err)
			}
			var snapshot durableSnapshot
			if err := json.Unmarshal(snapshotBytes, &snapshot); err != nil {
				t.Fatalf("decode snapshot: %v", err)
			}
			if snapshot.Version != durableSnapshotVersion || snapshot.WALGeneration == 0 || snapshot.WALOffset == 0 {
				t.Fatalf("expected generation-aware snapshot cursor, got %+v", snapshot)
			}

			for i := 0; i < tc.postSnapshotDocs; i++ {
				mustAddDocument(
					t,
					e,
					testSessionID,
					fmt.Sprintf("generation-after-%02d", i),
					fmt.Sprintf("after-%02d-%s.txt", i, strings.Repeat("x", 20)),
				)
			}
			newWALSize, err := e.durable.wal.Size()
			if err != nil {
				t.Fatalf("read new WAL size: %v", err)
			}
			if strings.Contains(tc.name, "smaller") && newWALSize >= snapshot.WALOffset {
				t.Fatalf("test setup expected new WAL %d < old offset %d", newWALSize, snapshot.WALOffset)
			}
			if strings.Contains(tc.name, "larger") && newWALSize <= snapshot.WALOffset {
				t.Fatalf("test setup expected new WAL %d > old offset %d", newWALSize, snapshot.WALOffset)
			}
			if err := e.Close(); err != nil {
				t.Fatalf("close engine: %v", err)
			}

			recovered := NewEngineWithOptionsMust(t, Options{
				VectorDim: testVectorDim,
				StoreMode: "durable",
				Durable: DurableOptions{
					WALDir:      walDir,
					SnapshotDir: snapshotDir,
					SyncPolicy:  "every_write",
				},
			})
			defer func() {
				if err := recovered.Close(); err != nil {
					t.Fatalf("close recovered engine: %v", err)
				}
			}()
			info, err := recovered.InfoForSession(testSessionID)
			if err != nil {
				t.Fatalf("InfoForSession failed: %v", err)
			}
			expected := 1 + tc.postSnapshotDocs
			if info.DocumentCount != expected {
				t.Fatalf("expected %d recovered documents, got %+v", expected, info)
			}
		})
	}
}

func TestEngine_DurableSnapshotReplayWithinSameWALGeneration(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	mustAddDocument(t, e, testSessionID, "same-generation-before", "before.txt")
	if _, err := e.CreateDurableSnapshot("", false); err != nil {
		t.Fatalf("CreateDurableSnapshot failed: %v", err)
	}
	mustAddDocument(t, e, testSessionID, "same-generation-after", "after.txt")
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	defer recovered.Close()
	info, err := recovered.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession failed: %v", err)
	}
	if info.DocumentCount != 2 {
		t.Fatalf("expected snapshot plus same-generation WAL tail, got %+v", info)
	}
}

func TestEngine_DurableSnapshotWALGenerationMismatchFailsClosed(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	mustAddDocument(t, e, testSessionID, "mismatch-doc", "mismatch.txt")
	if _, err := e.CreateDurableSnapshot("", false); err != nil {
		t.Fatalf("CreateDurableSnapshot failed: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	walPath := filepath.Join(walDir, "session.wal")
	walBytes, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	lineEnd := bytes.IndexByte(walBytes, '\n')
	if lineEnd < 0 {
		t.Fatal("expected WAL header line")
	}
	var header walHeader
	if err := json.Unmarshal(walBytes[:lineEnd], &header); err != nil {
		t.Fatalf("decode WAL header: %v", err)
	}
	header.Generation += 2
	headerBytes, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("encode WAL header: %v", err)
	}
	tampered := append(append(headerBytes, '\n'), walBytes[lineEnd+1:]...)
	if err := os.WriteFile(walPath, tampered, 0640); err != nil {
		t.Fatalf("write tampered WAL: %v", err)
	}

	_, err = NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	if !errors.Is(err, ErrDurableRecovery) || !strings.Contains(err.Error(), "does not match current generation") {
		t.Fatalf("expected fail-closed generation mismatch, got %v", err)
	}
}

func TestEngine_LegacySnapshotWithAmbiguousWALOffsetFailsClosed(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	if err := os.MkdirAll(walDir, 0750); err != nil {
		t.Fatalf("create WAL dir: %v", err)
	}
	if err := os.MkdirAll(snapshotDir, 0750); err != nil {
		t.Fatalf("create snapshot dir: %v", err)
	}
	legacyRecord := walRecord{
		Op:        walOpAddDocument,
		SessionID: testSessionID,
		Document:  &types.Document{ID: 1, ExternalID: "legacy-doc", Filename: "legacy.txt"},
	}
	legacyWAL, err := json.Marshal(legacyRecord)
	if err != nil {
		t.Fatalf("encode legacy WAL: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "session.wal"), append(legacyWAL, '\n'), 0640); err != nil {
		t.Fatalf("write legacy WAL: %v", err)
	}
	legacySnapshot, err := json.Marshal(durableSnapshot{
		Version:   1,
		CreatedAt: time.Now().UnixNano(),
		WALSize:   1,
		Sessions:  []*store.SessionSnapshot{},
	})
	if err != nil {
		t.Fatalf("encode legacy snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "snapshot-00000000000000000001.json"), legacySnapshot, 0640); err != nil {
		t.Fatalf("write legacy snapshot: %v", err)
	}

	_, err = NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	if !errors.Is(err, ErrDurableRecovery) || !strings.Contains(err.Error(), "requires an explicit migration") {
		t.Fatalf("expected explicit legacy compatibility failure, got %v", err)
	}
}

func TestEngine_DurableCustomSnapshotPathAlsoPublishesCanonicalRecoverySnapshot(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	customPath := filepath.Join(root, "operator-backup.json")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	mustAddDocument(t, e, testSessionID, "checkpoint-doc", "checkpoint.txt")

	if _, err := e.CreateDurableSnapshot(customPath, true); err != nil {
		t.Fatalf("CreateDurableSnapshot custom path failed: %v", err)
	}
	if _, err := os.Stat(customPath); err != nil {
		t.Fatalf("expected operator snapshot at %s: %v", customPath, err)
	}
	canonical, err := filepath.Glob(filepath.Join(snapshotDir, "snapshot-*.json"))
	if err != nil {
		t.Fatalf("glob canonical snapshots: %v", err)
	}
	if len(canonical) != 1 {
		t.Fatalf("expected one canonical recovery snapshot, got %v", canonical)
	}
	if size, err := e.durable.wal.Size(); err != nil || size != 0 {
		t.Fatalf("expected WAL truncated after canonical publication, size=%d err=%v", size, err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()
	if _, ok := recovered.GetDocument(testSessionID, 1); !ok {
		t.Fatal("expected checkpoint document restored from canonical snapshot")
	}
}

func TestEngine_DurableFailedSnapshotDoesNotTruncateWAL(t *testing.T) {
	root := t.TempDir()
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      filepath.Join(root, "wal"),
			SnapshotDir: filepath.Join(root, "snapshots"),
			SyncPolicy:  "every_write",
		},
	})
	defer func() {
		if err := e.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	mustAddDocument(t, e, testSessionID, "failed-snap-doc", "failed.txt")
	sizeBefore, err := e.durable.wal.Size()
	if err != nil || sizeBefore == 0 {
		t.Fatalf("expected non-empty WAL before failed snapshot, size=%d err=%v", sizeBefore, err)
	}
	if _, err := e.CreateDurableSnapshot(root, true); err == nil {
		t.Fatal("expected snapshot path that is a directory to fail")
	}
	sizeAfter, err := e.durable.wal.Size()
	if err != nil {
		t.Fatalf("WAL size after failed snapshot: %v", err)
	}
	if sizeAfter != sizeBefore {
		t.Fatalf("failed snapshot must not truncate WAL, before=%d after=%d", sizeBefore, sizeAfter)
	}
}

func TestEngine_DurableCorruptSnapshotFailsClosed(t *testing.T) {
	root := t.TempDir()
	snapshotDir := filepath.Join(root, "snapshots")
	if err := os.MkdirAll(snapshotDir, 0750); err != nil {
		t.Fatalf("create snapshot dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "snapshot-99999999999999999999.json"), []byte("{not-json}\n"), 0640); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	_, err := NewEngineWithOptions(Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      filepath.Join(root, "wal"),
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	if !errors.Is(err, ErrDurableRecovery) {
		t.Fatalf("expected durable recovery failure for corrupt snapshot, got %v", err)
	}
}

func TestEngine_DurableRecoveryRPOAndRTORegression(t *testing.T) {
	const datasetSize = 25
	const maxRecovery = 5 * time.Second

	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	snapshotDir := filepath.Join(root, "snapshots")
	e := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	embedding := randomVector(testVectorDim)
	for i := 0; i < datasetSize; i++ {
		doc := mustAddDocument(t, e, testSessionID, fmt.Sprintf("rpo-doc-%02d", i), fmt.Sprintf("%02d.txt", i))
		mustAddTextUnit(t, e, testSessionID, fmt.Sprintf("rpo-tu-%02d", i), doc.ID, "rpo text", embedding, 2)
		mustAddEntity(t, e, testSessionID, fmt.Sprintf("rpo-ent-%02d", i), fmt.Sprintf("RPO Entity %02d", i), "concept", "rpo", embedding)
	}
	if _, err := e.CreateDurableSnapshot("", true); err != nil {
		t.Fatalf("CreateDurableSnapshot failed: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	start := time.Now()
	recovered := NewEngineWithOptionsMust(t, Options{
		VectorDim: testVectorDim,
		StoreMode: "durable",
		Durable: DurableOptions{
			WALDir:      walDir,
			SnapshotDir: snapshotDir,
			SyncPolicy:  "every_write",
		},
	})
	elapsed := time.Since(start)
	defer func() {
		if err := recovered.Close(); err != nil {
			t.Fatalf("close recovered engine: %v", err)
		}
	}()
	if elapsed > maxRecovery {
		t.Fatalf("RTO regression: recovered %d docs/textunits/entities in %s, budget %s", datasetSize, elapsed, maxRecovery)
	}
	info, err := recovered.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession failed: %v", err)
	}
	if info.DocumentCount != datasetSize || info.TextUnitCount != datasetSize || info.EntityCount != datasetSize {
		t.Fatalf("RPO regression: committed data lost after recovery, got %+v", info)
	}
	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity}
	spec.KHops = 0
	if _, err := recovered.Query(testSessionID, spec); err != nil {
		t.Fatalf("query after RPO/RTO recovery failed: %v", err)
	}
}

func NewEngineWithOptionsMust(t *testing.T, opts Options) *Engine {
	t.Helper()
	e, err := NewEngineWithOptions(opts)
	if err != nil {
		t.Fatalf("NewEngineWithOptions failed: %v", err)
	}
	return e
}

func TestEngine_AddDocumentAllowsDuplicateFilename(t *testing.T) {
	e := createTestEngine()

	doc1 := mustAddDocument(t, e, testSessionID, "ext-doc-1", "shared.txt")
	doc2 := mustAddDocument(t, e, testSessionID, "ext-doc-2", "shared.txt")

	if doc1.ID == doc2.ID {
		t.Fatal("Expected duplicate filename documents to have distinct IDs")
	}
	if doc1.Filename != doc2.Filename {
		t.Fatalf("Expected matching filenames, got %q and %q", doc1.Filename, doc2.Filename)
	}
	info, err := e.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession failed: %v", err)
	}
	if info.DocumentCount != 2 {
		t.Fatalf("Expected 2 documents, got %d", info.DocumentCount)
	}
}

func TestEngine_AddDocument_Duplicate(t *testing.T) {
	e := createTestEngine()

	_, err := e.AddDocument(testSessionID, "ext-doc-1", "test.txt")
	if err != nil {
		t.Fatalf("First AddDocument failed: %v", err)
	}

	_, err = e.AddDocument(testSessionID, "ext-doc-1", "test2.txt")
	if err == nil {
		t.Error("Duplicate AddDocument should fail")
	}
}

func TestEngine_GetDocument(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")

	retrieved, ok := e.GetDocument(testSessionID, doc.ID)
	if !ok {
		t.Error("GetDocument should return true")
	}
	if retrieved.ExternalID != "ext-doc-1" {
		t.Errorf("Expected ExternalID 'ext-doc-1', got '%s'", retrieved.ExternalID)
	}
}

func TestEngine_GetDocument_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetDocument(testSessionID, 99999)
	if ok {
		t.Error("GetDocument should return false for non-existent document")
	}
}

func TestEngine_UpdateDocumentStatus(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")

	success := e.UpdateDocumentStatus(testSessionID, doc.ID, types.DocStatusReady)
	if !success {
		t.Error("UpdateDocumentStatus should return true")
	}

	retrieved, _ := e.GetDocument(testSessionID, doc.ID)
	if retrieved.Status != types.DocStatusReady {
		t.Errorf("Expected status Ready, got %s", retrieved.Status)
	}
}

func TestEngine_UpdateDocumentStatus_NotFound(t *testing.T) {
	e := createTestEngine()

	success := e.UpdateDocumentStatus(testSessionID, 99999, types.DocStatusReady)
	if success {
		t.Error("UpdateDocumentStatus should return false for non-existent document")
	}
}

// =============================================================================
// TextUnit Operations Tests
// =============================================================================

func TestEngine_AddTextUnit(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")

	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Test content", embedding, 10)

	if tu.ID == 0 {
		t.Error("TextUnit ID should not be 0")
	}
	if tu.ExternalID != "ext-tu-1" {
		t.Errorf("Expected ExternalID 'ext-tu-1', got '%s'", tu.ExternalID)
	}
	if tu.DocumentID != doc.ID {
		t.Errorf("Expected DocumentID %d, got %d", doc.ID, tu.DocumentID)
	}
	if tu.Content != "Test content" {
		t.Errorf("Expected content 'Test content', got '%s'", tu.Content)
	}
}

func TestEngine_AddTextUnit_Duplicate(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")

	embedding := randomVector(testVectorDim)
	_, err := e.AddTextUnit(testSessionID, "ext-tu-1", doc.ID, "Content 1", embedding, 10)
	if err != nil {
		t.Fatalf("First AddTextUnit failed: %v", err)
	}

	_, err = e.AddTextUnit(testSessionID, "ext-tu-1", doc.ID, "Content 2", embedding, 10)
	if err == nil {
		t.Error("Duplicate AddTextUnit should fail")
	}
}

func TestEngine_GetTextUnit(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Test content", embedding, 10)

	retrieved, ok := e.GetTextUnit(testSessionID, tu.ID)
	if !ok {
		t.Error("GetTextUnit should return true")
	}
	if retrieved.Content != "Test content" {
		t.Errorf("Expected content 'Test content', got '%s'", retrieved.Content)
	}
}

// =============================================================================
// Entity Operations Tests
// =============================================================================

func TestEngine_AddEntity(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent, err := e.AddEntity(testSessionID, "ext-ent-1", "Bank Indonesia", "organization", "Central bank", embedding)
	if err != nil {
		t.Fatalf("AddEntity failed: %v", err)
	}

	if ent.ID == 0 {
		t.Error("Entity ID should not be 0")
	}
	if ent.ExternalID != "ext-ent-1" {
		t.Errorf("Expected ExternalID 'ext-ent-1', got '%s'", ent.ExternalID)
	}
}

func TestEngine_MSetDocumentsRequiresExternalID(t *testing.T) {
	e := createTestEngine()

	ids, err := e.MSetDocuments(testSessionID, []types.BulkDocumentInput{
		{ExternalID: "doc-valid", Filename: "valid.pdf"},
		{Filename: "missing.pdf"},
	})
	if err == nil {
		t.Fatal("Expected MSetDocuments to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "atomic bulk documents failed at index 1") ||
		!strings.Contains(err.Error(), "document external_id is required") ||
		!strings.Contains(err.Error(), "no items committed") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("Expected no ids on validation failure, got %v", ids)
	}
	if info, _ := e.InfoForSession(testSessionID); info.DocumentCount != 0 {
		t.Fatalf("Expected no documents to be created, got %d", info.DocumentCount)
	}
}

func TestEngine_MSetTextUnitsRequiresExternalID(t *testing.T) {
	e := createTestEngine()
	doc := mustAddDocument(t, e, testSessionID, "doc-for-tu", "valid.pdf")

	ids, err := e.MSetTextUnits(testSessionID, []types.BulkTextUnitInput{
		{ExternalID: "tu-valid", DocumentID: doc.ID, Content: "Valid", Embedding: randomVector(testVectorDim), TokenCount: 1},
		{DocumentID: doc.ID, Content: "Missing external id", Embedding: randomVector(testVectorDim), TokenCount: 3},
	})
	if err == nil {
		t.Fatal("Expected MSetTextUnits to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "atomic bulk textunits failed at index 1") ||
		!strings.Contains(err.Error(), "textunit external_id is required") ||
		!strings.Contains(err.Error(), "no items committed") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("Expected no ids on validation failure, got %v", ids)
	}
	if info, _ := e.InfoForSession(testSessionID); info.TextUnitCount != 0 {
		t.Fatalf("Expected no text units to be created, got %d", info.TextUnitCount)
	}
}

func TestEngine_MSetEntitiesRequiresExternalID(t *testing.T) {
	e := createTestEngine()

	ids, err := e.MSetEntities(testSessionID, []types.BulkEntityInput{
		{ExternalID: "ent-valid", Title: "Valid Entity", Type: "test", Embedding: randomVector(testVectorDim)},
		{Title: "Missing External ID", Type: "test", Embedding: randomVector(testVectorDim)},
	})
	if err == nil {
		t.Fatal("Expected MSetEntities to reject missing external_id")
	}
	if !strings.Contains(err.Error(), "atomic bulk entities failed at index 1") ||
		!strings.Contains(err.Error(), "entity external_id is required") ||
		!strings.Contains(err.Error(), "no items committed") {
		t.Fatalf("Expected external_id error, got %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("Expected no ids on validation failure, got %v", ids)
	}
	if info, _ := e.InfoForSession(testSessionID); info.EntityCount != 0 {
		t.Fatalf("Expected no entities to be created, got %d", info.EntityCount)
	}
}

func TestEngine_GetEntity(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Bank Indonesia", "organization", "Central bank", embedding)

	retrieved, ok := e.GetEntity(testSessionID, ent.ID)
	if !ok {
		t.Error("GetEntity should return true")
	}
	// Title is normalized to uppercase
	if retrieved.Title != "BANK INDONESIA" {
		t.Errorf("Expected title 'BANK INDONESIA', got '%s'", retrieved.Title)
	}
}

func TestEngine_GetEntityByTitle(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	mustAddEntity(t, e, testSessionID, "ext-ent-1", "Bank Indonesia", "organization", "Central bank", embedding)

	// Should find with different case
	retrieved, ok := e.GetEntityByTitle(testSessionID, "bank indonesia")
	if !ok {
		t.Error("GetEntityByTitle should return true")
	}
	if retrieved.Title != "BANK INDONESIA" {
		t.Errorf("Expected title 'BANK INDONESIA', got '%s'", retrieved.Title)
	}
}

func TestEngine_UpdateEntityDescription(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Bank Indonesia", "organization", "Central bank", embedding)

	newEmbedding := randomVector(testVectorDim)
	success := e.UpdateEntityDescription(testSessionID, ent.ID, "Updated description", newEmbedding)
	if !success {
		t.Error("UpdateEntityDescription should return true")
	}

	retrieved, _ := e.GetEntity(testSessionID, ent.ID)
	if retrieved.Description != "Updated description" {
		t.Errorf("Expected description 'Updated description', got '%s'", retrieved.Description)
	}
}

// =============================================================================
// Relationship Operations Tests
// =============================================================================

func TestEngine_AddRelationship(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)

	rel, err := e.AddRelationship(testSessionID, "ext-rel-1", ent1.ID, ent2.ID, "RELATED_TO", "Relationship desc", 1.0)
	if err != nil {
		t.Fatalf("AddRelationship failed: %v", err)
	}

	if rel.ID == 0 {
		t.Error("Relationship ID should not be 0")
	}
	if rel.SourceID != ent1.ID {
		t.Errorf("Expected SourceID %d, got %d", ent1.ID, rel.SourceID)
	}
	if rel.TargetID != ent2.ID {
		t.Errorf("Expected TargetID %d, got %d", ent2.ID, rel.TargetID)
	}
}

func TestEngine_GetRelationship(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)

	rel := mustAddRelationship(t, e, testSessionID, "ext-rel-1", ent1.ID, ent2.ID, "RELATED_TO", "Desc", 1.0)

	retrieved, ok := e.GetRelationship(testSessionID, rel.ID)
	if !ok {
		t.Error("GetRelationship should return true")
	}
	if retrieved.Type != "RELATED_TO" {
		t.Errorf("Expected type 'RELATED_TO', got '%s'", retrieved.Type)
	}
}

func TestEngine_GetRelationshipByEntities(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)

	mustAddRelationship(t, e, testSessionID, "ext-rel-1", ent1.ID, ent2.ID, "RELATED_TO", "Desc", 1.0)

	retrieved, ok := e.GetRelationshipByEntities(testSessionID, ent1.ID, ent2.ID)
	if !ok {
		t.Error("GetRelationshipByEntities should return true")
	}
	if retrieved.SourceID != ent1.ID || retrieved.TargetID != ent2.ID {
		t.Error("Retrieved wrong relationship")
	}
}

// =============================================================================
// Community Operations Tests
// =============================================================================

func TestEngine_AddCommunity(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)
	rel := mustAddRelationship(t, e, testSessionID, "ext-rel-1", ent1.ID, ent2.ID, "RELATED_TO", "Desc", 1.0)
	comm, err := e.AddCommunity(testSessionID, "ext-comm-1", "Test Community", "Summary", "Full content", 0, []uint64{ent1.ID, ent2.ID}, []uint64{rel.ID}, embedding)
	if err != nil {
		t.Fatalf("AddCommunity failed: %v", err)
	}

	if comm.ID == 0 {
		t.Error("Community ID should not be 0")
	}
	if comm.Title != "Test Community" {
		t.Errorf("Expected title 'Test Community', got '%s'", comm.Title)
	}
}

func TestEngine_AddCommunityValidatesReferencesAndEmbedding(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc", embedding)

	if _, err := e.AddCommunity(testSessionID, "ext-comm-1", "Community", "Summary", "Full", 0, []uint64{ent.ID, 999}, nil, embedding); err == nil {
		t.Fatal("Expected missing community entity reference to fail")
	}
	if _, err := e.AddCommunity(testSessionID, "ext-comm-2", "Community", "Summary", "Full", 0, []uint64{ent.ID}, []uint64{999}, embedding); err == nil {
		t.Fatal("Expected missing community relationship reference to fail")
	}
	if _, err := e.AddCommunity(testSessionID, "ext-comm-3", "Community", "Summary", "Full", 0, []uint64{ent.ID}, nil, make([]float32, testVectorDim-1)); err == nil {
		t.Fatal("Expected invalid community embedding to fail")
	}
}

func TestEngine_GetCommunity(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	comm := mustAddCommunity(t, e, testSessionID, "ext-comm-1", "Test Community", "Summary", "Full content", 0, []uint64{ent.ID}, nil, embedding)

	retrieved, ok := e.GetCommunity(testSessionID, comm.ID)
	if !ok {
		t.Error("GetCommunity should return true")
	}
	if retrieved.Summary != "Summary" {
		t.Errorf("Expected summary 'Summary', got '%s'", retrieved.Summary)
	}
}

// =============================================================================
// Query Pipeline Tests
// =============================================================================

func TestEngine_Query_Basic(t *testing.T) {
	e := createTestEngine()

	// Add test data
	embedding1 := randomVector(testVectorDim)
	embedding2 := randomVector(testVectorDim)

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Test content 1", embedding1, 10)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Description 1", embedding1)
	mustAddCommunity(t, e, testSessionID, "ext-comm-1", "Community 1", "Summary", "Full", 0, []uint64{ent.ID}, nil, embedding2)

	// Query
	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding1

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.QueryID == 0 {
		t.Error("QueryID should not be 0")
	}
}

func TestEngine_Query_EmptyIndex(t *testing.T) {
	e := createTestEngine()

	// Add a document to ensure session exists (even though indices are empty)
	mustAddDocument(t, e, testSessionID, "doc-1", "test.pdf")

	spec := types.DefaultQuerySpec()
	spec.QueryVector = randomVector(testVectorDim)

	_, err := e.Query(testSessionID, spec)
	if !errors.Is(err, ErrRetrievalNotReady) {
		t.Fatalf("Expected retrieval ready error on empty index, got %v", err)
	}
}

func TestEngine_Query_PartiallyIndexedSkipsEmptySeedIndexes(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	doc := mustAddDocument(t, e, testSessionID, "doc-1", "test.pdf")
	mustAddTextUnit(t, e, testSessionID, "tu-unindexed", doc.ID, "Stored without embedding", nil, 3)
	mustAddEntity(t, e, testSessionID, "ent-indexed", "Indexed Entity", "test", "Has embedding", embedding)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity, types.SearchTypeCommunity}
	spec.KHops = 0

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query should run when one requested seed index is ready: %v", err)
	}
	if result.Stats.EntitiesSearched != 1 {
		t.Fatalf("Expected entity index to be searched, got %d", result.Stats.EntitiesSearched)
	}
	if result.Stats.TextUnitsSearched != 0 {
		t.Fatalf("Expected empty textunit index to be skipped, searched %d", result.Stats.TextUnitsSearched)
	}
	if got, want := result.Stats.SkippedSeedIndexes, []string{"textunit", "community"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("Expected skipped seed indexes %v, got %v", want, got)
	}
}

func TestEngine_Query_FullyIndexedHasNoSkippedSeedIndexes(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	doc := mustAddDocument(t, e, testSessionID, "doc-1", "test.pdf")
	mustAddTextUnit(t, e, testSessionID, "tu-indexed", doc.ID, "Stored with embedding", embedding, 3)
	ent := mustAddEntity(t, e, testSessionID, "ent-indexed", "Indexed Entity", "test", "Has embedding", embedding)
	mustAddCommunity(t, e, testSessionID, "comm-indexed", "Indexed Community", "Summary", "Full", 0, []uint64{ent.ID}, nil, embedding)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit, types.SearchTypeEntity, types.SearchTypeCommunity}
	spec.KHops = 0

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query should run when all requested seed indexes are ready: %v", err)
	}
	if len(result.Stats.SkippedSeedIndexes) != 0 {
		t.Fatalf("Expected no skipped seed indexes, got %v", result.Stats.SkippedSeedIndexes)
	}
	if result.Stats.TextUnitsSearched != 1 || result.Stats.EntitiesSearched != 1 || result.Stats.CommunitiesSearched != 1 {
		t.Fatalf("Expected all seed indexes searched once, got stats %+v", result.Stats)
	}
}

func TestEngine_Query_TextUnitWithoutEmbeddingIsNotVectorSeed(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "doc-1", "test.txt")
	unindexed := mustAddTextUnit(t, e, testSessionID, "tu-unindexed", doc.ID, "Stored without embedding", nil, 3)
	indexedEmbedding := randomVector(testVectorDim)
	indexed := mustAddTextUnit(t, e, testSessionID, "tu-indexed", doc.ID, "Stored with embedding", indexedEmbedding, 3)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = indexedEmbedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeTextUnit}
	spec.TopK = 10
	spec.KHops = 0

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.Stats.TextUnitsSearched != 1 {
		t.Fatalf("Expected only indexed text unit to be searched, got %d", result.Stats.TextUnitsSearched)
	}
	if len(result.TextUnits) != 1 {
		t.Fatalf("Expected one text unit result, got %d", len(result.TextUnits))
	}
	if result.TextUnits[0].TextUnit.ID != indexed.ID {
		t.Fatalf("Expected indexed text unit result %d, got %d", indexed.ID, result.TextUnits[0].TextUnit.ID)
	}
	if result.TextUnits[0].TextUnit.ID == unindexed.ID {
		t.Fatal("Unembedded text unit should not be used as a vector-search seed")
	}
	info, err := e.InfoForSession(testSessionID)
	if err != nil {
		t.Fatalf("InfoForSession failed: %v", err)
	}
	if info.TextUnitCount != 2 {
		t.Fatalf("Expected both text units to be stored, got %d", info.TextUnitCount)
	}
}

func TestEngine_Query_UnembeddedEntityParticipatesInGraphTraversal(t *testing.T) {
	e := createTestEngine()

	seedEmbedding := randomVector(testVectorDim)
	seed := mustAddEntity(t, e, testSessionID, "ent-indexed", "Indexed Entity", "test", "Has embedding", seedEmbedding)
	unembedded := mustAddEntity(t, e, testSessionID, "ent-unembedded", "Unembedded Entity", "test", "No embedding", nil)
	mustAddRelationship(t, e, testSessionID, "rel-indexed-unembedded", seed.ID, unembedded.ID, "RELATED", "Traversal edge", 1.0)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = seedEmbedding
	spec.SearchTypes = []types.SearchType{types.SearchTypeEntity}
	spec.TopK = 1
	spec.KHops = 1
	spec.MaxEntities = 10

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result.Stats.EntitiesSearched != 1 {
		t.Fatalf("Expected only indexed entity to be vector searched, got %d", result.Stats.EntitiesSearched)
	}

	foundUnembedded := false
	for _, entityResult := range result.Entities {
		if entityResult.Entity.ID == unembedded.ID {
			foundUnembedded = true
			if entityResult.Hop != 1 {
				t.Fatalf("Expected unembedded entity at hop 1, got hop %d", entityResult.Hop)
			}
			if entityResult.Similarity != 0 {
				t.Fatalf("Expected traversal entity similarity 0, got %f", entityResult.Similarity)
			}
		}
	}
	if !foundUnembedded {
		t.Fatal("Expected unembedded entity to participate in graph traversal")
	}
}

func TestEngine_Query_WithGraphExpansion(t *testing.T) {
	e := createTestEngine()

	// Setup: entities with relationships
	embedding := randomVector(testVectorDim)

	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", randomVector(testVectorDim))
	mustAddRelationship(t, e, testSessionID, "rel-1", ent1.ID, ent2.ID, "RELATED", "Desc", 1.0)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding
	spec.KHops = 2
	spec.SearchTypes = []types.SearchType{types.SearchTypeEntity}

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query with graph expansion failed: %v", err)
	}

	// Should find entities via graph traversal
	if result.QueryID == 0 {
		t.Error("QueryID should not be 0")
	}
}

// =============================================================================
// Explain Tests
// =============================================================================

func TestEngine_Explain(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)

	spec := types.DefaultQuerySpec()
	spec.QueryVector = embedding

	result, err := e.Query(testSessionID, spec)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	explain, ok := e.Explain(result.QueryID)
	if !ok {
		t.Error("Explain should return true for valid QueryID")
	}

	if explain.QueryID != result.QueryID {
		t.Errorf("Explain QueryID mismatch: got %d, want %d", explain.QueryID, result.QueryID)
	}
}

func TestEngine_Explain_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.Explain(99999)
	if ok {
		t.Error("Explain should return false for non-existent QueryID")
	}
}

// =============================================================================
// Info Tests
// =============================================================================

func TestEngine_Info(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)

	info := e.Info()

	if info.Version == "" {
		t.Error("Version should not be empty")
	}
	if info.DocumentCount != 1 {
		t.Errorf("Expected DocumentCount 1, got %d", info.DocumentCount)
	}
	if info.EntityCount != 1 {
		t.Errorf("Expected EntityCount 1, got %d", info.EntityCount)
	}
	if info.VectorDim != testVectorDim {
		t.Errorf("Expected VectorDim %d, got %d", testVectorDim, info.VectorDim)
	}
}

// =============================================================================
// TTL Tests
// =============================================================================

// DEPRECATED v0.1.0: TTL management moved to session level
// func TestEngine_SetTTL(t *testing.T) { ... }

// DEPRECATED v0.1.0
// func TestEngine_SetTTL_NotFound(t *testing.T) { ... }

// DEPRECATED v0.1.0
// func TestEngine_SetIdleTTL(t *testing.T) { ... }

// =============================================================================
// Link Operations Tests
// =============================================================================

func TestEngine_LinkTextUnitToEntity(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Test content", embedding, 10)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc", embedding)

	success := e.LinkTextUnitToEntity(testSessionID, tu.ID, ent.ID)
	if !success {
		t.Error("LinkTextUnitToEntity should return true")
	}

	// Verify link
	retrievedTU, _ := e.GetTextUnit(testSessionID, tu.ID)
	found := false
	for _, eid := range retrievedTU.EntityIDs {
		if eid == ent.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("TextUnit should have entity linked")
	}

	// Verify reverse link
	retrievedEnt, _ := e.GetEntity(testSessionID, ent.ID)
	found = false
	for _, tuID := range retrievedEnt.TextUnitIDs {
		if tuID == tu.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("Entity should have text unit linked")
	}
}

// =============================================================================
// LRU Cache Tests
// =============================================================================

func TestQueryLogLRU_Basic(t *testing.T) {
	cache := newQueryLogLRU(10)

	log := &queryLog{spec: types.DefaultQuerySpec()}
	cache.Set(1, log)

	retrieved, ok := cache.Get(1)
	if !ok {
		t.Error("Get should return true")
	}
	if retrieved == nil {
		t.Error("Retrieved log should not be nil")
	}
}

func TestQueryLogLRU_Eviction(t *testing.T) {
	cache := newQueryLogLRU(3)

	// Add 4 items (capacity is 3)
	for i := uint64(1); i <= 4; i++ {
		cache.Set(i, &queryLog{})
	}

	// First item should be evicted
	_, ok := cache.Get(1)
	if ok {
		t.Error("Item 1 should have been evicted")
	}

	// Latest items should exist
	_, ok = cache.Get(4)
	if !ok {
		t.Error("Item 4 should exist")
	}
}

func TestQueryLogLRU_Len(t *testing.T) {
	cache := newQueryLogLRU(10)

	cache.Set(1, &queryLog{})
	cache.Set(2, &queryLog{})

	if cache.Len() != 2 {
		t.Errorf("Expected Len 2, got %d", cache.Len())
	}
}

// =============================================================================
// Concurrent Tests
// =============================================================================

func TestEngine_ConcurrentAccess(t *testing.T) {
	e := createTestEngine()

	var wg sync.WaitGroup
	const n = 20

	// Concurrent document additions
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mustAddDocument(t, e, testSessionID, "doc-"+itoa(id), "file.txt")
		}(i)
	}

	// Concurrent entity additions
	embedding := randomVector(testVectorDim)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mustAddEntity(t, e, testSessionID, "ent-"+itoa(id), "Entity "+itoa(id), "test", "Desc", embedding)
		}(i)
	}

	wg.Wait()

	info := e.Info()
	if info.DocumentCount != n {
		t.Errorf("Expected %d documents, got %d", n, info.DocumentCount)
	}
	if info.EntityCount != n {
		t.Errorf("Expected %d entities, got %d", n, info.EntityCount)
	}
}

func TestEngine_ConcurrentQueries(t *testing.T) {
	e := createTestEngine()

	// Setup data
	embedding := randomVector(testVectorDim)
	for i := 0; i < 10; i++ {
		mustAddEntity(t, e, testSessionID, "ent-"+itoa(i), "Entity "+itoa(i), "test", "Desc", embedding)
	}

	var wg sync.WaitGroup
	const n = 20
	errCh := make(chan error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			spec := types.DefaultQuerySpec()
			spec.QueryVector = embedding
			if _, err := e.Query(testSessionID, spec); err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("Concurrent query error: %v", err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte(i%10) + '0'
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// =============================================================================
// Additional Coverage Tests
// =============================================================================

func TestEngine_GetTextUnit_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetTextUnit(testSessionID, 99999)
	if ok {
		t.Error("GetTextUnit should return false for non-existent")
	}
}

func TestEngine_GetEntity_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetEntity(testSessionID, 99999)
	if ok {
		t.Error("GetEntity should return false for non-existent")
	}
}

func TestEngine_GetEntityByTitle_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetEntityByTitle(testSessionID, "Non Existent Entity")
	if ok {
		t.Error("GetEntityByTitle should return false for non-existent")
	}
}

func TestEngine_UpdateEntityDescription_NotFound(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	success := e.UpdateEntityDescription(testSessionID, 99999, "New desc", embedding)
	if success {
		t.Error("UpdateEntityDescription should return false for non-existent")
	}
}

func TestEngine_GetRelationship_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetRelationship(testSessionID, 99999)
	if ok {
		t.Error("GetRelationship should return false for non-existent")
	}
}

func TestEngine_GetRelationshipByEntities_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetRelationshipByEntities(testSessionID, 99999, 99998)
	if ok {
		t.Error("GetRelationshipByEntities should return false for non-existent")
	}
}

func TestEngine_GetCommunity_NotFound(t *testing.T) {
	e := createTestEngine()

	_, ok := e.GetCommunity(testSessionID, 99999)
	if ok {
		t.Error("GetCommunity should return false for non-existent")
	}
}

func TestEngine_AddRelationship_Duplicate(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)

	_, err := e.AddRelationship(testSessionID, "ext-rel-1", ent1.ID, ent2.ID, "RELATED_TO", "Desc", 1.0)
	if err != nil {
		t.Fatalf("First AddRelationship failed: %v", err)
	}

	_, err = e.AddRelationship(testSessionID, "ext-rel-2", ent1.ID, ent2.ID, "ANOTHER_TYPE", "Desc", 1.0)
	if err != nil {
		t.Fatalf("Same source/target with different type should succeed: %v", err)
	}

	_, err = e.AddRelationship(testSessionID, "ext-rel-3", ent1.ID, ent2.ID, "RELATED_TO", "Desc", 1.0)
	if err == nil {
		t.Error("Duplicate relationship identity should fail")
	}
}

func TestEngine_MSetRelationshipsAtomicInvalidMiddle(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent1 := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc 1", embedding)
	ent2 := mustAddEntity(t, e, testSessionID, "ext-ent-2", "Entity 2", "test", "Desc 2", embedding)

	ids, err := e.MSetRelationships(testSessionID, []types.BulkRelationshipInput{
		{ExternalID: "rel-valid", SourceID: ent1.ID, TargetID: ent2.ID, Type: "KNOWS", Description: "Valid", Weight: 1},
		{ExternalID: "rel-invalid", SourceID: ent1.ID, TargetID: 999, Type: "KNOWS", Description: "Invalid", Weight: 1},
	})
	if err == nil {
		t.Fatal("Expected MSetRelationships to reject missing target")
	}
	if !strings.Contains(err.Error(), "atomic bulk relationships failed at index 1") ||
		!strings.Contains(err.Error(), "relationship target_id 999 does not exist") ||
		!strings.Contains(err.Error(), "no items committed") {
		t.Fatalf("Expected atomic relationship error, got %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("Expected no ids on validation failure, got %v", ids)
	}
	if info, _ := e.InfoForSession(testSessionID); info.RelationshipCount != 0 {
		t.Fatalf("Expected no relationships to be created, got %d", info.RelationshipCount)
	}
}

func TestEngine_AddCommunity_Duplicate(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity 1", "test", "Desc", embedding)
	_, err := e.AddCommunity(testSessionID, "ext-comm-1", "Community 1", "Summary", "Full", 0, []uint64{ent.ID}, nil, embedding)
	if err != nil {
		t.Fatalf("First AddCommunity failed: %v", err)
	}

	_, err = e.AddCommunity(testSessionID, "ext-comm-1", "Community 2", "Summary", "Full", 0, []uint64{ent.ID}, nil, embedding)
	if err == nil {
		t.Error("Duplicate community should fail")
	}
}

func TestEngine_LinkTextUnitToEntity_NotFound(t *testing.T) {
	e := createTestEngine()

	success := e.LinkTextUnitToEntity(testSessionID, 99999, 99998)
	if success {
		t.Error("LinkTextUnitToEntity should return false for non-existent")
	}
}

// DEPRECATED v0.1.0: TTL management removed
/*
func TestEngine_SetTTL_AllTypes(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Content", embedding, 10)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity", "test", "Desc", embedding)
	comm, _ := e.AddCommunity("ext-comm-1", "Comm", "Sum", "Full", 0, []uint64{}, []uint64{}, embedding, 0, 0)

	tests := []struct {
		itemType types.ItemType
		id       uint64
	}{
		{types.ItemTypeDocument, doc.ID},
		{types.ItemTypeTextUnit, tu.ID},
		{types.ItemTypeEntity, ent.ID},
		{types.ItemTypeCommunity, comm.ID},
	}

	for _, tt := range tests {
		success := e.SetTTL(tt.itemType, tt.id, 3600)
		if !success {
			t.Errorf("SetTTL failed for %s", tt.itemType)
		}

		ttl := e.GetTTL(tt.itemType, tt.id)
		if ttl <= 0 {
			t.Errorf("GetTTL should be positive for %s", tt.itemType)
		}
	}
}
*/

// DEPRECATED v0.1.0
/*
func TestEngine_SetIdleTTL_AllTypes(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	embedding := randomVector(testVectorDim)
	tu := mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Content", embedding, 10)
	ent := mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity", "test", "Desc", embedding)
	comm, _ := e.AddCommunity("ext-comm-1", "Comm", "Sum", "Full", 0, []uint64{}, []uint64{}, embedding, 0, 0)

	tests := []struct {
		itemType types.ItemType
		id       uint64
	}{
		{types.ItemTypeDocument, doc.ID},
		{types.ItemTypeTextUnit, tu.ID},
		{types.ItemTypeEntity, ent.ID},
		{types.ItemTypeCommunity, comm.ID},
	}

	for _, tt := range tests {
		success := e.SetIdleTTL(tt.itemType, tt.id, 600)
		if !success {
			t.Errorf("SetIdleTTL failed for %s", tt.itemType)
		}
	}
}
*/

// DEPRECATED v0.1.0
/*
func TestEngine_GetTTL_InvalidType(t *testing.T) {
	e := createTestEngine()

	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	e.SetTTL(types.ItemTypeDocument, doc.ID, 3600)

	// Test with unknown item type - returns negative value
	ttl := e.GetTTL("unknown", doc.ID)
	if ttl >= 0 {
		t.Errorf("GetTTL with invalid type should return negative value, got %d", ttl)
	}
}
*/

func TestEngine_Query_AllSearchTypes(t *testing.T) {
	e := createTestEngine()

	embedding := randomVector(testVectorDim)
	doc := mustAddDocument(t, e, testSessionID, "ext-doc-1", "test.txt")
	mustAddTextUnit(t, e, testSessionID, "ext-tu-1", doc.ID, "Content", embedding, 10)
	mustAddEntity(t, e, testSessionID, "ext-ent-1", "Entity", "test", "Desc", embedding)
	mustAddCommunity(t, e, testSessionID, "ext-comm-1", "Comm", "Summary", "Full", 0, []uint64{}, []uint64{}, embedding)

	// Test each search type
	searchTypes := []types.SearchType{
		types.SearchTypeTextUnit,
		types.SearchTypeEntity,
		types.SearchTypeCommunity,
	}

	for _, st := range searchTypes {
		spec := types.DefaultQuerySpec()
		spec.QueryVector = embedding
		spec.SearchTypes = []types.SearchType{st}

		result, err := e.Query(testSessionID, spec)
		if err != nil {
			t.Errorf("Query with search type %s failed: %v", st, err)
		}
		if result.QueryID == 0 {
			t.Errorf("Query with search type %s returned invalid QueryID", st)
		}
	}
}

func TestEngine_ListEntitiesPagination(t *testing.T) {
	e := createTestEngine()

	for i := 0; i < 5; i++ {
		extID := fmt.Sprintf("ent-%d", i+1)
		title := fmt.Sprintf("Entity %d", i+1)
		mustAddEntity(t, e, testSessionID, extID, title, "person", "desc", nil)
	}

	entities, next := e.ListEntities(testSessionID, 0, 2)
	if len(entities) != 2 {
		t.Fatalf("Expected 2 entities, got %d", len(entities))
	}
	if next == 0 {
		t.Fatalf("Expected non-zero next cursor")
	}
	if entities[0].ID >= entities[1].ID {
		t.Errorf("Expected ascending IDs, got %d then %d", entities[0].ID, entities[1].ID)
	}

	entities2, next2 := e.ListEntities(testSessionID, next, 2)
	if len(entities2) != 2 {
		t.Fatalf("Expected 2 entities, got %d", len(entities2))
	}
	if next2 == 0 {
		t.Fatalf("Expected non-zero next cursor for page 2")
	}

	entities3, next3 := e.ListEntities(testSessionID, next2, 2)
	if len(entities3) != 1 {
		t.Fatalf("Expected 1 entity, got %d", len(entities3))
	}
	if next3 != 0 {
		t.Fatalf("Expected next cursor 0 at end, got %d", next3)
	}
}

func TestEngine_ListRelationshipsPagination(t *testing.T) {
	e := createTestEngine()

	e1 := mustAddEntity(t, e, testSessionID, "ent-001", "Entity 1", "person", "desc", nil)
	e2 := mustAddEntity(t, e, testSessionID, "ent-002", "Entity 2", "person", "desc", nil)
	e3 := mustAddEntity(t, e, testSessionID, "ent-003", "Entity 3", "person", "desc", nil)

	mustAddRelationship(t, e, testSessionID, "rel-001", e1.ID, e2.ID, "KNOWS", "desc", 1.0)
	mustAddRelationship(t, e, testSessionID, "rel-002", e2.ID, e3.ID, "KNOWS", "desc", 1.0)
	mustAddRelationship(t, e, testSessionID, "rel-003", e3.ID, e1.ID, "KNOWS", "desc", 1.0)

	rels, next := e.ListRelationships(testSessionID, 0, 2)
	if len(rels) != 2 {
		t.Fatalf("Expected 2 relationships, got %d", len(rels))
	}
	if next == 0 {
		t.Fatalf("Expected non-zero next cursor")
	}
	if rels[0].ID >= rels[1].ID {
		t.Errorf("Expected ascending IDs, got %d then %d", rels[0].ID, rels[1].ID)
	}

	rels2, next2 := e.ListRelationships(testSessionID, next, 2)
	if len(rels2) != 1 {
		t.Fatalf("Expected 1 relationship, got %d", len(rels2))
	}
	if next2 != 0 {
		t.Fatalf("Expected next cursor 0 at end, got %d", next2)
	}
}

func TestQueryLogLRU_Update(t *testing.T) {
	cache := newQueryLogLRU(3)

	cache.Set(1, &queryLog{})
	cache.Set(2, &queryLog{})
	cache.Set(3, &queryLog{})

	// Add new item, should evict oldest item (item 1)
	cache.Set(4, &queryLog{})

	_, ok := cache.Get(1)
	if ok {
		t.Error("Item 1 should have been evicted (oldest)")
	}

	// Items 2, 3, 4 should exist
	_, ok = cache.Get(4)
	if !ok {
		t.Error("Item 4 should exist")
	}
}
