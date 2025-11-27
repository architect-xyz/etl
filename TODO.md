# Make the same forked changes from pg_replicate onto this repo

Review ../pg_replicate/FORK_CHANGES.diff as a reference.  Read AGENTS.md as CLAUDE.md for direction as well.

## Theme 4: Postgres Connection Keepalives

### Changes in pg_replicate
Added TCP-level keepalive configuration to `ReplicationClient`:
```rust
.keepalives(true)
.keepalives_idle(std::time::Duration::from_secs(30))
.keepalives_interval(std::time::Duration::from_secs(30))
.keepalives_retries(3)
```

### Status in etl
**NOT IMPLEMENTED** - etl's `PgReplicationClient` (in `etl/src/replication/client.rs`) does not configure TCP keepalives. However, etl uses application-level Standby Status Update messages every ~10 seconds to maintain connection health.

### Recommendation
**OPTIONAL** - TCP keepalives provide network-level connection health checking which can detect dead connections faster than application-level heartbeats alone. This is especially useful for connections through NAT gateways or load balancers that may drop idle connections.

### Implementation Approach
If needed, modify `PgReplicationClient::connect_no_tls()` and `connect_tls()` to add:
```rust
config.keepalives(true);
config.keepalives_idle(Duration::from_secs(30));
config.keepalives_interval(Duration::from_secs(30));
config.keepalives_retries(3);
```

Note: These settings should probably be configurable via `PgConnectionConfig`.
