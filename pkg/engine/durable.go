package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gibram-io/gibram/pkg/store"
	"github.com/gibram-io/gibram/pkg/types"
)

const (
	durableSnapshotVersion  = 2
	durableWALHeaderVersion = 1
	durableWALHeaderFormat  = "gibram-session-wal"

	walOpAddDocument        = "add_document"
	walOpAddTextUnit        = "add_textunit"
	walOpAddEntity          = "add_entity"
	walOpAddRelationship    = "add_relationship"
	walOpMSetDocuments      = "mset_documents"
	walOpMSetTextUnits      = "mset_textunits"
	walOpMSetEntities       = "mset_entities"
	walOpMSetRelationships  = "mset_relationships"
	walOpReplaceCommunities = "replace_communities"
	walOpDeleteDocument     = "delete_document"
	walOpDeleteTextUnit     = "delete_textunit"
	walOpDeleteEntity       = "delete_entity"
	walOpDeleteRelationship = "delete_relationship"
)

type durableSessionStore struct {
	mu                   sync.Mutex
	wal                  *sessionWAL
	snapshotDir          string
	snapshotInterval     time.Duration
	snapshotWALSizeBytes int64
	lastSnapshotAt       time.Time
	lastSnapshotWALSize  int64
	recoveryDuration     time.Duration
	replayWALStartOffset int64
	unhealthy            bool
	markUnhealthy        func()
}

type durableSnapshot struct {
	Version       int                      `json:"version"`
	CreatedAt     int64                    `json:"created_at"`
	WALGeneration uint64                   `json:"wal_generation,omitempty"`
	WALOffset     int64                    `json:"wal_offset,omitempty"`
	WALSize       int64                    `json:"wal_size,omitempty"`
	Sessions      []*store.SessionSnapshot `json:"sessions"`
}

type walHeader struct {
	Format     string `json:"format"`
	Version    int    `json:"version"`
	Generation uint64 `json:"generation"`
}

type walRecord struct {
	Op                  string                `json:"op"`
	SessionID           string                `json:"session_id"`
	Document            *types.Document       `json:"document,omitempty"`
	Documents           []*types.Document     `json:"documents,omitempty"`
	TextUnit            *types.TextUnit       `json:"text_unit,omitempty"`
	TextUnitEmbedding   []float32             `json:"text_unit_embedding,omitempty"`
	TextUnits           []*types.TextUnit     `json:"text_units,omitempty"`
	TextUnitEmbeddings  [][]float32           `json:"text_unit_embeddings,omitempty"`
	Entity              *types.Entity         `json:"entity,omitempty"`
	EntityEmbedding     []float32             `json:"entity_embedding,omitempty"`
	Entities            []*types.Entity       `json:"entities,omitempty"`
	EntityEmbeddings    [][]float32           `json:"entity_embeddings,omitempty"`
	Relationship        *types.Relationship   `json:"relationship,omitempty"`
	Relationships       []*types.Relationship `json:"relationships,omitempty"`
	Communities         []*types.Community    `json:"communities,omitempty"`
	CommunityEmbeddings [][]float32           `json:"community_embeddings,omitempty"`
	ID                  uint64                `json:"id,omitempty"`
}

type sessionWAL struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	policy     string
	generation uint64
	dataStart  int64
	currentLSN int64
	flushedLSN int64
	stopCh     chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
}

func openDurableSessionStore(opts DurableOptions) (*durableSessionStore, error) {
	wal, err := openSessionWAL(opts.WALDir, opts.SyncPolicy, opts.SyncInterval)
	if err != nil {
		return nil, err
	}
	if opts.SnapshotDir == "" {
		opts.SnapshotDir = filepath.Join(filepath.Dir(opts.WALDir), "session_snapshots")
	}
	if err := os.MkdirAll(opts.SnapshotDir, 0750); err != nil {
		if closeErr := wal.Close(); closeErr != nil {
			return nil, fmt.Errorf("create durable snapshot dir: %v (close failed: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("create durable snapshot dir: %w", err)
	}
	return &durableSessionStore{
		wal:                  wal,
		snapshotDir:          opts.SnapshotDir,
		snapshotInterval:     opts.SnapshotInterval,
		snapshotWALSizeBytes: opts.SnapshotWALSizeBytes,
	}, nil
}

func openSessionWAL(walDir, policy string, interval time.Duration) (*sessionWAL, error) {
	if walDir == "" {
		return nil, fmt.Errorf("durable session store requires wal dir")
	}
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, fmt.Errorf("create durable session WAL dir: %w", err)
	}
	path := filepath.Join(walDir, "session.wal")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, fmt.Errorf("open durable session WAL: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return nil, fmt.Errorf("stat durable session WAL: %v (close failed: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("stat durable session WAL: %w", err)
	}
	generation, dataStart, err := readOrInitializeWALHeader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("read durable session WAL header: %w", err)
	}
	info, err = file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat initialized durable session WAL: %w", err)
	}
	if _, err := file.Seek(0, 2); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek durable session WAL: %w", err)
	}
	dataSize := info.Size() - dataStart
	if dataSize < 0 {
		_ = file.Close()
		return nil, fmt.Errorf("durable session WAL header exceeds file size")
	}
	w := &sessionWAL{
		file:       file,
		path:       path,
		policy:     policy,
		generation: generation,
		dataStart:  dataStart,
		currentLSN: dataSize,
		flushedLSN: dataSize,
		stopCh:     make(chan struct{}),
	}
	if policy == "periodic" {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = w.Sync()
				case <-w.stopCh:
					return
				}
			}
		}()
	}
	return w, nil
}

func readOrInitializeWALHeader(file *os.File) (uint64, int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size() == 0 {
		headerBytes, err := marshalWALHeader(1)
		if err != nil {
			return 0, 0, err
		}
		if _, err := file.Write(headerBytes); err != nil {
			return 0, 0, err
		}
		if err := file.Sync(); err != nil {
			return 0, 0, err
		}
		return 1, int64(len(headerBytes)), nil
	}
	if _, err := file.Seek(0, 0); err != nil {
		return 0, 0, err
	}
	line, err := bufio.NewReader(file).ReadBytes('\n')
	if err != nil {
		return 0, 0, nil
	}
	var header walHeader
	if err := json.Unmarshal(line, &header); err != nil || header.Format == "" {
		return 0, 0, nil
	}
	if header.Format != durableWALHeaderFormat {
		return 0, 0, fmt.Errorf("unsupported WAL header format %q", header.Format)
	}
	if header.Version != durableWALHeaderVersion {
		return 0, 0, fmt.Errorf("unsupported WAL header version %d", header.Version)
	}
	if header.Generation == 0 {
		return 0, 0, fmt.Errorf("WAL generation must be greater than zero")
	}
	return header.Generation, int64(len(line)), nil
}

func marshalWALHeader(generation uint64) ([]byte, error) {
	data, err := json.Marshal(walHeader{
		Format:     durableWALHeaderFormat,
		Version:    durableWALHeaderVersion,
		Generation: generation,
	})
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (w *sessionWAL) Append(record walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal durable WAL record: %w", err)
	}
	n, err := w.file.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("append durable WAL record: %w", err)
	}
	w.currentLSN += int64(n)
	if w.policy == "every_write" {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync durable WAL record: %w", err)
		}
		w.flushedLSN = w.currentLSN
	}
	return nil
}

func (w *sessionWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.flushedLSN = w.currentLSN
	return nil
}

func (w *sessionWAL) Close() error {
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		if closeErr := w.file.Close(); closeErr != nil {
			return fmt.Errorf("sync durable WAL failed: %v (close failed: %v)", err, closeErr)
		}
		return err
	}
	return w.file.Close()
}

func (w *sessionWAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size() - w.dataStart
	if size < 0 {
		return 0, fmt.Errorf("durable WAL size is smaller than its header")
	}
	return size, nil
}

func (w *sessionWAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	nextGeneration := w.generation + 1
	headerBytes, err := marshalWALHeader(nextGeneration)
	if err != nil {
		return err
	}
	tmp := w.path + ".next"
	next, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	if _, err := next.Write(headerBytes); err != nil {
		_ = next.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := next.Sync(); err != nil {
		_ = next.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := next.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	dir, err := os.Open(filepath.Dir(w.path))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	if err := dir.Close(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	reopened, err := os.OpenFile(w.path, os.O_APPEND|os.O_RDWR, 0640)
	if err != nil {
		return err
	}
	w.file = reopened
	w.generation = nextGeneration
	w.dataStart = int64(len(headerBytes))
	w.currentLSN = 0
	w.flushedLSN = 0
	return nil
}

func (w *sessionWAL) Status() (currentLSN, flushedLSN, sizeBytes int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	size := info.Size() - w.dataStart
	if size < 0 {
		return 0, 0, 0, fmt.Errorf("durable WAL size is smaller than its header")
	}
	return w.currentLSN, w.flushedLSN, size, nil
}

func (w *sessionWAL) Position() (generation uint64, offset int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	info, err := w.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size() - w.dataStart
	if size < 0 {
		return 0, 0, fmt.Errorf("durable WAL size is smaller than its header")
	}
	return w.generation, size, nil
}

func (w *sessionWAL) ReadAll() ([]walRecord, error) {
	return w.ReadFrom(0)
}

func (w *sessionWAL) ReadFrom(offset int64) ([]walRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size() - w.dataStart
	if size < 0 {
		return nil, fmt.Errorf("durable WAL size is smaller than its header")
	}
	if offset > size {
		return nil, fmt.Errorf("durable WAL replay offset %d exceeds generation %d size %d", offset, w.generation, size)
	}
	if offset > 0 {
		previous := []byte{0}
		if _, err := w.file.ReadAt(previous, w.dataStart+offset-1); err != nil {
			return nil, err
		}
		if previous[0] != '\n' {
			return nil, fmt.Errorf("durable WAL replay offset %d is not a record boundary", offset)
		}
	}
	if _, err := w.file.Seek(w.dataStart+offset, 0); err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(w.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	var records []walRecord
	for scanner.Scan() {
		var record walRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode durable WAL record %d: %w", len(records)+1, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read durable WAL: %w", err)
	}
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, err
	}
	return records, nil
}

func (e *Engine) recoverDurableSessions() error {
	records, err := e.durable.wal.ReadFrom(e.durable.replayWALStartOffset)
	if err != nil {
		e.markUnhealthy()
		return err
	}
	for i, record := range records {
		if err := e.applyWALRecord(record); err != nil {
			e.markUnhealthy()
			return fmt.Errorf("record %d: %w", i+1, err)
		}
	}
	return nil
}

func (e *Engine) restoreLatestDurableSnapshot() error {
	path, err := e.latestDurableSnapshotPath()
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var snapshot durableSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode durable snapshot %s: %w", path, err)
	}
	replayOffset, err := e.resolveSnapshotReplayOffset(snapshot)
	if err != nil {
		return err
	}
	if snapshot.Version != 1 && snapshot.Version != durableSnapshotVersion {
		return fmt.Errorf("unsupported durable snapshot version %d", snapshot.Version)
	}

	e.mu.Lock()
	e.sessions = make(map[string]*store.SessionStore)
	e.mu.Unlock()
	for _, sessSnapshot := range snapshot.Sessions {
		if sessSnapshot == nil || sessSnapshot.SessionID == "" {
			return fmt.Errorf("snapshot contains invalid session")
		}
		sess := store.NewSessionStore(sessSnapshot.SessionID, e.vectorDim)
		if err := sess.RestoreFromSnapshot(sessSnapshot); err != nil {
			return err
		}
		e.applyResourceLimits(sess)
		e.mu.Lock()
		e.sessions[sessSnapshot.SessionID] = sess
		e.mu.Unlock()
	}
	e.durable.replayWALStartOffset = replayOffset
	e.durable.lastSnapshotWALSize = replayOffset
	e.durable.lastSnapshotAt = time.Unix(0, snapshot.CreatedAt)
	return nil
}

func (e *Engine) resolveSnapshotReplayOffset(snapshot durableSnapshot) (int64, error) {
	currentGeneration, currentSize, err := e.durable.wal.Position()
	if err != nil {
		return 0, err
	}
	if snapshot.Version == 1 {
		if snapshot.WALSize == 0 || currentSize == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf(
			"legacy durable snapshot version 1 has ambiguous WAL offset %d with non-empty WAL; restore requires an explicit migration",
			snapshot.WALSize,
		)
	}
	if snapshot.Version != durableSnapshotVersion {
		return 0, fmt.Errorf("unsupported durable snapshot version %d", snapshot.Version)
	}
	switch {
	case currentGeneration == snapshot.WALGeneration:
		if snapshot.WALOffset > currentSize {
			return 0, fmt.Errorf(
				"durable snapshot WAL offset %d exceeds generation %d size %d",
				snapshot.WALOffset,
				currentGeneration,
				currentSize,
			)
		}
		return snapshot.WALOffset, nil
	case currentGeneration == snapshot.WALGeneration+1:
		return 0, nil
	default:
		return 0, fmt.Errorf(
			"durable snapshot WAL generation %d does not match current generation %d",
			snapshot.WALGeneration,
			currentGeneration,
		)
	}
}

func (e *Engine) latestDurableSnapshotPath() (string, error) {
	files, err := filepath.Glob(filepath.Join(e.durable.snapshotDir, "snapshot-*.json"))
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", nil
	}
	sort.Strings(files)
	return files[len(files)-1], nil
}

func (e *Engine) CreateDurableSnapshot(path string, truncateWAL bool) (string, error) {
	if e.durable == nil {
		return "", fmt.Errorf("durable session store is not enabled")
	}
	e.durable.mu.Lock()
	defer e.durable.mu.Unlock()
	if e.durable.unhealthy {
		return "", fmt.Errorf("durable session store is unhealthy")
	}
	return e.createDurableSnapshotLocked(path, truncateWAL)
}

func (e *Engine) maybeAutoSnapshot() {
	if e.durable == nil {
		return
	}
	intervalDue := false
	if e.durable.snapshotInterval > 0 {
		if e.durable.lastSnapshotAt.IsZero() || time.Since(e.durable.lastSnapshotAt) >= e.durable.snapshotInterval {
			intervalDue = true
		}
	}
	sizeDue := false
	if e.durable.snapshotWALSizeBytes > 0 {
		size, err := e.durable.wal.Size()
		if err == nil && size-e.durable.lastSnapshotWALSize >= e.durable.snapshotWALSizeBytes {
			sizeDue = true
		}
	}
	if !intervalDue && !sizeDue {
		return
	}
	if _, err := e.CreateDurableSnapshot("", true); err != nil {
		e.markUnhealthy()
	}
}

func (e *Engine) createDurableSnapshotLocked(path string, truncateWAL bool) (string, error) {
	if err := e.durable.wal.Sync(); err != nil {
		return "", err
	}
	walGeneration, walOffset, err := e.durable.wal.Position()
	if err != nil {
		return "", err
	}
	canonicalPath := filepath.Join(e.durable.snapshotDir, fmt.Sprintf("snapshot-%020d.json", time.Now().UnixNano()))
	requestedPath := path
	if requestedPath == "" {
		requestedPath = canonicalPath
	}

	e.mu.RLock()
	snapshots := make([]*store.SessionSnapshot, 0, len(e.sessions))
	for _, sess := range e.sessions {
		if !sess.IsExpired() {
			snapshots = append(snapshots, sess.Snapshot())
		}
	}
	e.mu.RUnlock()

	snapshot := durableSnapshot{
		Version:       durableSnapshotVersion,
		CreatedAt:     time.Now().UnixNano(),
		WALGeneration: walGeneration,
		WALOffset:     walOffset,
		Sessions:      snapshots,
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return "", err
	}
	if requestedPath != canonicalPath {
		if err := writeDurableSnapshotAtomically(requestedPath, data); err != nil {
			return "", err
		}
	}
	if err := writeDurableSnapshotAtomically(canonicalPath, data); err != nil {
		return "", err
	}
	e.durable.lastSnapshotAt = time.Unix(0, snapshot.CreatedAt)
	if truncateWAL {
		if err := e.durable.wal.Truncate(); err != nil {
			return "", err
		}
		e.durable.lastSnapshotWALSize = 0
		e.durable.replayWALStartOffset = 0
	} else {
		e.durable.lastSnapshotWALSize = walOffset
		e.durable.replayWALStartOffset = walOffset
	}
	return requestedPath, nil
}

func writeDurableSnapshotAtomically(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (e *Engine) applyWALRecord(record walRecord) error {
	if record.SessionID == "" {
		return fmt.Errorf("session_id is required")
	}
	sess, err := e.getOrCreateSession(record.SessionID)
	if err != nil {
		return err
	}
	switch record.Op {
	case walOpAddDocument:
		return sess.ApplyDocument(record.Document)
	case walOpAddTextUnit:
		return sess.ApplyTextUnit(record.TextUnit, record.TextUnitEmbedding)
	case walOpAddEntity:
		return sess.ApplyEntity(record.Entity, record.EntityEmbedding)
	case walOpAddRelationship:
		return sess.ApplyRelationship(record.Relationship)
	case walOpMSetDocuments:
		return sess.ApplyDocumentsBatch(record.Documents)
	case walOpMSetTextUnits:
		return sess.ApplyTextUnitsBatch(record.TextUnits, record.TextUnitEmbeddings)
	case walOpMSetEntities:
		return sess.ApplyEntitiesBatch(record.Entities, record.EntityEmbeddings)
	case walOpMSetRelationships:
		return sess.ApplyRelationshipsBatch(record.Relationships)
	case walOpReplaceCommunities:
		return sess.ReplaceCommunities(record.Communities, record.CommunityEmbeddings)
	case walOpDeleteDocument:
		return sess.DeleteDocumentChecked(record.ID)
	case walOpDeleteTextUnit:
		return sess.DeleteTextUnitChecked(record.ID)
	case walOpDeleteEntity:
		return sess.DeleteEntityChecked(record.ID)
	case walOpDeleteRelationship:
		return sess.DeleteRelationshipChecked(record.ID)
	default:
		return fmt.Errorf("unknown durable WAL op %q", record.Op)
	}
}

func (e *Engine) markUnhealthy() {
	e.healthy.Store(false)
	if e.durable != nil {
		e.durable.unhealthy = true
	}
}

func (e *Engine) Close() error {
	if e.durable != nil {
		return e.durable.wal.Close()
	}
	return nil
}

func (e *Engine) durableSession() *durableSessionStore {
	if e.storeMode == "durable" {
		return e.durable
	}
	return nil
}

func (d *durableSessionStore) withMutation(fn func() error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.unhealthy {
		return fmt.Errorf("durable session store is unhealthy")
	}
	return fn()
}

func (d *durableSessionStore) append(record walRecord) error {
	if err := d.wal.Append(record); err != nil {
		d.unhealthy = true
		if d.markUnhealthy != nil {
			d.markUnhealthy()
		}
		return err
	}
	return nil
}

func (d *durableSessionStore) failAfterWAL(err error) error {
	d.unhealthy = true
	if d.markUnhealthy != nil {
		d.markUnhealthy()
	}
	return err
}

func durableAddDocument(sess *store.SessionStore, sessionID string, d *durableSessionStore, extID, filename string) (*types.Document, error) {
	var doc *types.Document
	err := d.withMutation(func() error {
		prepared, err := sess.PrepareDocument(extID, filename)
		if err != nil {
			return err
		}
		if err := d.append(walRecord{Op: walOpAddDocument, SessionID: sessionID, Document: prepared}); err != nil {
			return err
		}
		if err := sess.ApplyDocument(prepared); err != nil {
			return d.failAfterWAL(err)
		}
		doc = prepared
		return nil
	})
	return doc, err
}

func durableAddTextUnit(sess *store.SessionStore, sessionID string, d *durableSessionStore, extID string, docID uint64, content string, embedding []float32, tokenCount int) (*types.TextUnit, error) {
	var tu *types.TextUnit
	err := d.withMutation(func() error {
		prepared, preparedEmbedding, err := sess.PrepareTextUnit(extID, docID, content, embedding, tokenCount)
		if err != nil {
			return err
		}
		record := walRecord{Op: walOpAddTextUnit, SessionID: sessionID, TextUnit: prepared, TextUnitEmbedding: preparedEmbedding}
		if err := d.append(record); err != nil {
			return err
		}
		if err := sess.ApplyTextUnit(prepared, preparedEmbedding); err != nil {
			return d.failAfterWAL(err)
		}
		tu = prepared
		return nil
	})
	return tu, err
}

func durableAddEntity(sess *store.SessionStore, sessionID string, d *durableSessionStore, extID, title, entType, description string, embedding []float32) (*types.Entity, error) {
	var ent *types.Entity
	err := d.withMutation(func() error {
		prepared, preparedEmbedding, err := sess.PrepareEntity(extID, title, entType, description, embedding)
		if err != nil {
			return err
		}
		record := walRecord{Op: walOpAddEntity, SessionID: sessionID, Entity: prepared, EntityEmbedding: preparedEmbedding}
		if err := d.append(record); err != nil {
			return err
		}
		if err := sess.ApplyEntity(prepared, preparedEmbedding); err != nil {
			return d.failAfterWAL(err)
		}
		ent = prepared
		return nil
	})
	return ent, err
}

func durableAddRelationship(sess *store.SessionStore, sessionID string, d *durableSessionStore, extID string, sourceID, targetID uint64, relType, description string, weight float32) (*types.Relationship, error) {
	var rel *types.Relationship
	err := d.withMutation(func() error {
		prepared, err := sess.PrepareRelationship(extID, sourceID, targetID, relType, description, weight)
		if err != nil {
			return err
		}
		if err := d.append(walRecord{Op: walOpAddRelationship, SessionID: sessionID, Relationship: prepared}); err != nil {
			return err
		}
		if err := sess.ApplyRelationship(prepared); err != nil {
			return d.failAfterWAL(err)
		}
		rel = prepared
		return nil
	})
	return rel, err
}

func durableDelete(sess *store.SessionStore, sessionID string, d *durableSessionStore, op string, id uint64, validate func(uint64) error, apply func(uint64) error) error {
	return d.withMutation(func() error {
		if err := validate(id); err != nil {
			return err
		}
		if err := d.append(walRecord{Op: op, SessionID: sessionID, ID: id}); err != nil {
			return err
		}
		if err := apply(id); err != nil {
			return d.failAfterWAL(err)
		}
		return nil
	})
}

func durableMSetDocuments(sess *store.SessionStore, sessionID string, d *durableSessionStore, inputs []types.BulkDocumentInput) ([]uint64, error) {
	var ids []uint64
	err := d.withMutation(func() error {
		docs, err := sess.PrepareDocumentsBatch(inputs)
		if err != nil {
			return err
		}
		if err := d.append(walRecord{Op: walOpMSetDocuments, SessionID: sessionID, Documents: docs}); err != nil {
			return err
		}
		if err := sess.ApplyDocumentsBatch(docs); err != nil {
			return d.failAfterWAL(err)
		}
		ids = make([]uint64, 0, len(docs))
		for _, doc := range docs {
			ids = append(ids, doc.ID)
		}
		return nil
	})
	return ids, err
}

func durableMSetTextUnits(sess *store.SessionStore, sessionID string, d *durableSessionStore, inputs []types.BulkTextUnitInput) ([]uint64, error) {
	var ids []uint64
	err := d.withMutation(func() error {
		textUnits, embeddings, err := sess.PrepareTextUnitsBatch(inputs)
		if err != nil {
			return err
		}
		record := walRecord{Op: walOpMSetTextUnits, SessionID: sessionID, TextUnits: textUnits, TextUnitEmbeddings: embeddings}
		if err := d.append(record); err != nil {
			return err
		}
		if err := sess.ApplyTextUnitsBatch(textUnits, embeddings); err != nil {
			return d.failAfterWAL(err)
		}
		ids = make([]uint64, 0, len(textUnits))
		for _, tu := range textUnits {
			ids = append(ids, tu.ID)
		}
		return nil
	})
	return ids, err
}

func durableMSetEntities(sess *store.SessionStore, sessionID string, d *durableSessionStore, inputs []types.BulkEntityInput) ([]uint64, error) {
	var ids []uint64
	err := d.withMutation(func() error {
		entities, embeddings, err := sess.PrepareEntitiesBatch(inputs)
		if err != nil {
			return err
		}
		record := walRecord{Op: walOpMSetEntities, SessionID: sessionID, Entities: entities, EntityEmbeddings: embeddings}
		if err := d.append(record); err != nil {
			return err
		}
		if err := sess.ApplyEntitiesBatch(entities, embeddings); err != nil {
			return d.failAfterWAL(err)
		}
		ids = make([]uint64, 0, len(entities))
		for _, ent := range entities {
			ids = append(ids, ent.ID)
		}
		return nil
	})
	return ids, err
}

func durableMSetRelationships(sess *store.SessionStore, sessionID string, d *durableSessionStore, inputs []types.BulkRelationshipInput) ([]uint64, error) {
	var ids []uint64
	err := d.withMutation(func() error {
		relationships, err := sess.PrepareRelationshipsBatch(inputs)
		if err != nil {
			return err
		}
		if err := d.append(walRecord{Op: walOpMSetRelationships, SessionID: sessionID, Relationships: relationships}); err != nil {
			return err
		}
		if err := sess.ApplyRelationshipsBatch(relationships); err != nil {
			return d.failAfterWAL(err)
		}
		ids = make([]uint64, 0, len(relationships))
		for _, rel := range relationships {
			ids = append(ids, rel.ID)
		}
		return nil
	})
	return ids, err
}

func durableReplaceCommunities(sess *store.SessionStore, sessionID string, d *durableSessionStore, communities []*types.Community, embeddings [][]float32) error {
	return d.withMutation(func() error {
		if err := d.append(walRecord{
			Op:                  walOpReplaceCommunities,
			SessionID:           sessionID,
			Communities:         communities,
			CommunityEmbeddings: embeddings,
		}); err != nil {
			return err
		}
		if err := sess.ReplaceCommunities(communities, embeddings); err != nil {
			return d.failAfterWAL(err)
		}
		return nil
	})
}
