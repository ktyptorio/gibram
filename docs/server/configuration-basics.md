# Configuration Basics (v0.3.0)

Configure GibRAM server for production: TLS, authentication, persistence, and resource limits.

## Configuration Methods

### 1. Config File (Recommended)

Create `config.yaml`:

```yaml
server:
  addr: ":6161"
  data_dir: "/var/lib/gibram/data"
  vector_dim: 1536

logging:
  level: "info"
  format: "json"
  output: "stdout"
```

Run with config:

```bash
gibram-server --config config.yaml
```

### 2. CLI Flags

Override config or use without config file:

```bash
gibram-server \
  --addr :6161 \
  --data /var/lib/gibram/data \
  --dim 1536 \
  --log-level info
```

### 3. Precedence

CLI flags > Config file > Defaults

## Core Settings

### Server

```yaml
server:
  addr: ":6161"              # Bind address (default: :6161)
  data_dir: "./data"         # Data directory (default: ./data)
  vector_dim: 1536           # Vector dimension (default: 1536)
```

**⚠️ CRITICAL**: `vector_dim` must match SDK embedding dimensions.

**Common Values**:
- `1536` - OpenAI text-embedding-3-small (default)
- `768` - Sentence transformers, some open models
- `3072` - OpenAI text-embedding-3-large

**Once set, cannot be changed** without data loss (re-indexing required).

### Logging

```yaml
logging:
  level: "info"              # debug | info | warn | error
  format: "text"             # text | json
  output: "stdout"           # stdout | file
  file: ""                   # Log file path (if output=file)
```

**Production Recommendation**: Use `format: json` for structured logging (better for log aggregators).

## Security

### TLS (Production Required)

**Development (Auto-Cert)**:

```yaml
tls:
  auto_cert: true            # Generate self-signed cert
```

**Production (Custom Certificates)**:

```yaml
tls:
  cert_file: "/etc/gibram/certs/server.crt"
  key_file: "/etc/gibram/certs/server.key"
  auto_cert: false           # Disable auto-cert
```

**Generate Certificates**:

```bash
# Self-signed (testing)
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout server.key \
  -out server.crt \
  -days 365 \
  -subj "/CN=localhost"

# Let's Encrypt (production)
certbot certonly --standalone -d yourdomain.com
```

**⚠️ IMPORTANT**: 
- Without TLS, traffic is **unencrypted**
- Client must skip verification for self-signed certs
- Production should use CA-signed certificates

### Authentication

**API Key Authentication**:

```yaml
auth:
  keys:
    - id: "admin"
      key: "your-secure-admin-key-here"
      permissions: ["admin"]
    
    - id: "app-service"
      key: "your-secure-app-key-here"
      permissions: ["write"]
    
    - id: "query-service"
      key: "your-secure-query-key-here"
      permissions: ["read"]
```

**Permission Levels**:
- `admin` - Full access (backup, sessions, all operations)
- `write` - Read + write data (entities, relationships, queries)
- `read` - Read-only (queries, get operations)

**Using API Key (Python SDK)**:

```python
from gibram import GibRAMIndexer

indexer = GibRAMIndexer(
    session_id="my-project",
    api_key="your-secure-app-key-here"  # Not yet supported in Python SDK
)
```

**Using API Key (Go Client)**:

```go
config := client.DefaultPoolConfig()
config.APIKey = "your-secure-app-key-here"

c, err := client.NewClientWithConfig("localhost:6161", "session-id", config)
```

**⚠️ SECURITY NOTE**: Store keys in environment variables or secrets manager, not in config files committed to git.

### Rate Limiting

```yaml
security:
  max_frame_size: 67108864   # 64MB frame size limit
  rate_limit: 1000           # Requests per second
  rate_burst: 100            # Burst allowance
  idle_timeout: 300s         # Idle connection timeout
  unauth_timeout: 10s        # Timeout for unauthenticated connections
  max_conns_per_ip: 50       # Max connections per IP
```

**Adjust for Load**:
- High traffic: Increase `rate_limit` and `max_conns_per_ip`
- Low resources: Decrease to prevent DoS
- Long operations: Increase `idle_timeout`

## Persistence (Optional)

**By Default**: GibRAM is ephemeral (in-memory only). Data is lost on restart.

**Production Durable Mode**:

```yaml
session_store:
  mode: "durable"
  wal_dir: "/var/lib/gibram/data/session_wal"
  wal_sync_policy: "every_write"
  wal_sync_interval: 1s
  snapshot_dir: "/var/lib/gibram/data/session_snapshots"
  snapshot_interval: 5m
  snapshot_wal_size_bytes: 67108864
```

Admin commands:

- `SAVE` - Create snapshot (blocking)
- `BGSAVE` - Create snapshot (background)
- `LASTSAVE` - Get last save timestamp
- `BGRESTORE` - Restore from a snapshot path under `server.data_dir`

Automatic snapshots can be triggered by `snapshot_interval` or `snapshot_wal_size_bytes`. See [Durable Session Store Operations](durable-session-store.md) for WAL sync policy, backup/restore, failure modes, RPO/RTO expectations, and remediation steps.

## Session Management

**Session Cleanup Interval**:

```bash
gibram-server --session-cleanup-interval 60s
```

Default: 60 seconds (check for expired sessions every minute)

**Session TTL** (set via protocol or SDK):

Currently configured per-session via protocol commands. SDK support coming in future versions.

## Resource Limits

### Memory

**Docker Memory Limit**:

```yaml
# docker-compose.yml
services:
  gibram:
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 512M
```

**Monitoring**: Server tracks memory and logs warnings at 80%, 99%, 100%.

### Vector Dimension Impact

Higher dimensions = more memory per vector:
- 1536 dims: ~6KB per vector (float32)
- 3072 dims: ~12KB per vector

**Estimate**: 1M entities at 1536 dims ≈ 6GB RAM

## Example Configurations

### Development

```yaml
server:
  addr: ":6161"
  data_dir: "./data"
  vector_dim: 1536

tls:
  auto_cert: true

logging:
  level: "debug"
  format: "text"
```

Run:
```bash
gibram-server --insecure  # Disable TLS & auth for dev
```

### Production

```yaml
server:
  addr: ":6161"
  data_dir: "/var/lib/gibram/data"
  vector_dim: 1536

tls:
  cert_file: "/etc/gibram/certs/server.crt"
  key_file: "/etc/gibram/certs/server.key"

auth:
  keys:
    - id: "production-app"
      key_hash: "$2a$12$replace_with_bcrypt_hash_for_production_app_key"
      permissions: ["write"]

session_store:
  mode: "durable"
  wal_dir: "/var/lib/gibram/data/session_wal"
  wal_sync_policy: "every_write"
  snapshot_dir: "/var/lib/gibram/data/session_snapshots"
  snapshot_interval: 5m
  snapshot_wal_size_bytes: 67108864

security:
  max_frame_size: 4194304
  max_content_bytes: 1048576
  max_memory_bytes: 4294967296
  max_session_documents: 1000000
  max_session_entities: 1000000
  max_session_relationships: 5000000
  rate_limit: 5000
  max_conns_per_ip: 100

logging:
  level: "info"
  format: "json"
  output: "file"
  file: "/var/log/gibram/gibram.log"
```

### Docker Production

```yaml
# docker-compose.yml
version: '3.8'

services:
  gibram:
    image: gibramio/gibram:latest
    ports:
      - "6161:6161"
    
    volumes:
      - ./config.yaml:/etc/gibram/config.yaml:ro
      - ./certs:/etc/gibram/certs:ro
      - gibram-data:/var/lib/gibram/data
    
    environment:
      - GIBRAM_API_KEY=${GIBRAM_API_KEY}
    
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G

volumes:
  gibram-data:
```

## Validation

**Test Configuration**:

```bash
# Dry-run (validates config)
gibram-server --config config.yaml --help

# Check server starts
gibram-server --config config.yaml

# Verify logs
tail -f /var/log/gibram/gibram.log
```

**Check Settings**:

```bash
# Use CLI
gibram-cli -h localhost:6161

gibram> INFO
# Shows vector_dim, session count, etc.
```

## Troubleshooting

### Server Won't Start

**Symptom**: "Failed to load config"

**Check**:
1. YAML syntax valid: `yamllint config.yaml`
2. File permissions: `ls -la config.yaml`
3. Paths exist: `data_dir`, cert files

### TLS Handshake Failed

**Symptom**: Client "tls: handshake failure"

**Causes**:
- Self-signed cert without skip-verify
- Cert expired
- Cert hostname mismatch

**Fix (Development)**:
```python
# Python SDK (no skip-verify option yet)
# Use --insecure mode on server instead
```

### Authentication Failed

**Symptom**: "unauthorized" error

**Causes**:
- Wrong API key
- Key not in server config
- Insufficient permissions

**Fix**: Verify key matches config, check permission level

### Dimension Mismatch

**Symptom**: Runtime error when adding entities/chunks

**Cause**: Server `vector_dim` ≠ client embedding dimension

**Fix**: Restart server with correct `--dim` value (requires re-indexing)

## Next Steps

- **[Troubleshooting Guide](troubleshooting.md)** - Detailed error solutions
- **[SDK Configuration](../sdks/python/quickstart.md)** - Client-side setup
