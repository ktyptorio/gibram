# Durable Session Store Operations

Durable Session Store is an optional production mode for the session-based GraphRAG store. The default mode remains ephemeral for exploration and fast RAG experiments. Production deployments that need restart/crash recovery must explicitly set `session_store.mode: durable`.

## Setup

Minimum durable configuration:

```yaml
server:
  data_dir: "/var/lib/gibram/data"

session_store:
  mode: "durable"
  wal_dir: "/var/lib/gibram/data/session_wal"
  wal_sync_policy: "every_write"
  wal_sync_interval: 1s
  snapshot_dir: "/var/lib/gibram/data/session_snapshots"
  snapshot_interval: 5m
  snapshot_wal_size_bytes: 67108864
```

Directory rules:

- `server.data_dir` is the trust boundary for admin persistence commands.
- `wal_dir` stores the session write-ahead log.
- `snapshot_dir` stores durable snapshots.
- Keep `wal_dir` and `snapshot_dir` on persistent storage, not container scratch space.
- Do not share one WAL directory between multiple GibRAM server processes.

WAL sync policy:

- `every_write`: best durability. A committed durable write is synced before success is returned.
- `periodic`: lower write latency, but acknowledged writes since the last sync can be lost on process or host failure.
- `never`: for development only; not a production durability setting.

Snapshot policy:

- `snapshot_interval` enables automatic snapshots by elapsed time.
- `snapshot_wal_size_bytes` enables automatic snapshots by WAL growth.
- Set at least one automatic trigger in production so WAL replay stays bounded.
- Manual snapshots through `SAVE` and `BGSAVE` remain available.
- `WAL_CHECKPOINT` waits for durable snapshot publication before returning success.
- Manual snapshot paths are operator copies. Every successful durable snapshot also publishes an authoritative `snapshot-*.json` recovery artifact in `snapshot_dir` before WAL truncation.

## Backup And Restore

Manual snapshot commands:

- `SAVE`: blocking snapshot.
- `BGSAVE`: background snapshot.
- `LASTSAVE`: returns the last successful save time and path.

Durable snapshots store canonical session data plus embeddings. Vector indexes are derived state and are rebuilt during restore. This keeps restore deterministic, but large sessions may take longer to become ready.

Startup recovery order:

1. Load the latest durable snapshot, if present.
2. Match the snapshot's WAL generation with the current WAL generation.
3. Replay from the snapshot offset when both generations match, or from offset zero when the WAL is the next generation created by snapshot truncation.
4. Reject generation mismatches and ambiguous legacy snapshot offsets instead of guessing a replay position.
5. Rebuild vector indexes from canonical embeddings.
6. Start serving only after recovery succeeds.

WAL truncation:

- Automatic snapshots, manual durable snapshots, and `WAL_CHECKPOINT` use the same authoritative snapshot publication contract.
- WAL truncation occurs only after the authoritative snapshot has been fully written, synced, atomically renamed, and made discoverable in `snapshot_dir`.
- WAL truncation atomically replaces the current WAL with an empty WAL carrying the next generation identifier.
- Do not delete or truncate WAL manually unless you have a known-good snapshot and accept the data-loss boundary.

Restore path safety:

- `SAVE`, `BGSAVE`, and `BGRESTORE` paths are validated under `server.data_dir`.
- Path traversal and symlink escapes are rejected.
- Place operator-managed restore artifacts under `server.data_dir` before invoking `BGRESTORE`.

## Failure Modes

Durable mode fails closed. If snapshot loading or WAL replay fails during startup, the server refuses to start instead of serving empty or partial session state.

Common failure cases:

- Corrupt snapshot JSON.
- Corrupt WAL record.
- Snapshot/WAL generation mismatch or an ambiguous version 1 snapshot offset.
- Missing or unreadable WAL/snapshot directories.
- Incompatible vector dimension between restored data and server config.
- Disk full or permission errors during WAL append or snapshot write.
- Internal apply failure after WAL append.

Remediation:

- Preserve the broken `data_dir` before attempting repair.
- Inspect health logs for the failing artifact path and error.
- Restore from the latest verified backup snapshot under `server.data_dir`.
- If a WAL tail is corrupt and a snapshot is acceptable as the recovery point, move the corrupt WAL aside and restart from the snapshot.
- If an internal apply failure marks the engine unhealthy, stop accepting traffic and restart only after the artifact or code issue is resolved.

## RPO And RTO

RPO is the maximum amount of accepted data the operator is willing to lose after a failure. RTO is the maximum time the operator expects the service to take before it can serve again.

Expected durable-mode targets:

- `wal_sync_policy: every_write`: RPO is intended to be near zero for writes acknowledged after WAL sync.
- `wal_sync_policy: periodic`: RPO is up to `wal_sync_interval`, plus storage and OS behavior.
- RTO depends on latest snapshot size, WAL bytes since snapshot, embedding volume, and vector-index rebuild time.
- The initial target is single-node crash/restart recovery with RPO <= 1 second and RTO <= 30 seconds when automatic snapshots keep WAL replay bounded.

Limitations:

- Durable mode is not multi-node replication.
- It does not protect against losing the underlying disk or persistent volume.
- It does not make derived vector indexes the durability source of truth.
- It does not turn ephemeral mode into a production durable database unless durable mode is explicitly enabled.

## Operational Checks

Use `HEALTH` to inspect:

- `durable_state`
- `wal_current_lsn`
- `wal_flushed_lsn`
- `wal_flush_lag_bytes`
- `wal_size_bytes`
- `snapshot_status`
- `snapshot_count`
- `wal_bytes_since_snapshot`
- `recovery_duration_ms`
- `resource_pressure`
- `retrieval_ready`
- `empty_seed_indexes`

Healthy durable production posture:

- `durable_state=serving`
- `wal_flush_lag_bytes=0` for `every_write`, or bounded for `periodic`
- `snapshot_count` increases over time
- `wal_bytes_since_snapshot` remains below the configured WAL-size trigger
- `resource_pressure=ok`
- `retrieval_ready=true` after data with embeddings has been loaded
