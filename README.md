# Ezu

Ezu is a live spatial engine written in Rust.

- Indexes moving entities.
- Answers radius, bbox, polygon and k-nearest queries.
- Emits geofence enter, exit and dwell events on a resumable stream.
- Keeps recent tracks.

Ezu doesn't make governance decisions: a caller in front of it enforces policy. It persists through a WAL plus snapshots, exchanges bulk data as GeoArrow over Arrow Flight, and runs as a single binary with no runtime network calls.

Status: early. See [VISION.md](VISION.md) for the roadmap and GitHub Issues for planned work.

License: Apache-2.0.

## API

The contract is the versioned protobuf in `proto/ezu/v1/ezu.proto`: upsert,
delete, get, query (bbox, radius, polygon contains/intersects, nearest) and
status. One listener serves it over gRPC and as HTTP/JSON:
`POST /v1/entities`, `GET|DELETE /v1/entities/{collection}/{id}`,
`POST /v1/query`, `GET /v1/status`, `GET /v1/fences`,
`PUT|GET|DELETE /v1/fences/{id}` and `GET /v1/events`. Log shipping to
followers and promotion are the separate gRPC service `Replication` (see
Follower replicas and Promotion). JSON bodies are the same messages in
snake_case; a oneof is an object naming its variant.

Every request carries `authorization: Bearer <token>`. The namespace comes
from the authenticated identity, never from the request, and Ezu makes no
other authorization decision. Authentication is one of:

- JWT verified against a JWKS file given at startup (`EZU_JWKS_FILE`,
  `EZU_JWT_AUDIENCE`, optional `EZU_JWT_ISSUER`), with the namespace taken
  from a tenant claim (`EZU_TENANT_CLAIM`, default `tenant`);
- a static token (`EZU_STATIC_TOKEN_FILE`, `EZU_STATIC_NAMESPACE`) for
  standalone or development use, compared in constant time.

`EZU_TLS_CERT` and `EZU_TLS_KEY` serve over TLS; `EZU_TLS_CLIENT_CA` adds
mutual TLS. The server needs `EZU_DATA_DIR` and listens on `EZU_LISTEN`
(default `127.0.0.1:7600`); it refuses to start without authentication.

## Operations

- `GET /healthz` answers 200 while the engine accepts writes and 503 after a
  durability failure (reopen to recover); for a follower, see Follower
  replicas, and for a leader with a sync standby, Sync standby.
- `DurableEngine::promote(dir)` raises the data directory's epoch, kept in
  `wal/EPOCH` and recorded in the log. An engine still holding the directory
  at an older epoch refuses every later write and reports unhealthy, even
  while idle. An owner of a release without epochs cannot be fenced, so
  the promotion is refused while one runs. Once the old process is gone,
  the next open continues the log and event ids without a gap. Logs from
  before epochs still open. A directory that was never promoted keeps writing WAL
  format version 2, so an older release can still open it; after the first
  promotion, segments are version 3 and older releases refuse the directory.
  Rolling back past a promotion is not supported: an older release that
  recovers from a rebuild source skips reading segments, so it would not see
  that refusal. Roll back only directories that were never promoted.
- `GET /metrics` serves Prometheus text:
  - entity count, logged writes and fence events;
  - query latency as a histogram;
  - WAL bytes and records since the last snapshot (the WAL lag);
  - snapshot age, sealed namespaces and an `ezu_up` gauge;
  - on a follower, the leader's last LSN, the applied LSN, the lag
    between them in records, and whether it is connected
    (`ezu_replication_*`);
  - on a leader with a sync standby, the LSN it acknowledged and whether
    its lease is held (`ezu_sync_*`).

  Every series is aggregated over namespaces, so neither endpoint takes a
  token or reveals tenant data.
- `ezu diagnostics` prints a JSON support bundle for `EZU_DATA_DIR`. It holds
  WAL and snapshot counts, sizes, LSNs and format versions, and the names
  (not values) of the `EZU_*` settings present. It never includes secrets or
  namespace names, and it runs next to a live server.
- `make static` builds a fully static Linux binary. The `Dockerfile` copies
  it into an otherwise empty image, so there is no base image to pin. Deploy
  the image by digest, never by a `latest` tag. Signed `v*` tags and GitHub
  Releases are described in [RELEASING.md](RELEASING.md). Ezu makes no
  network calls of its own; CI starts the static binary with no network but
  loopback and checks it serves.
- `cargo deny` in CI refuses copyleft licenses in the default build (see
  `deny.toml`).
- `ezu-bench` measures throughput, query and fence latency, memory and
  recovery; see [docs/benchmarks.md](docs/benchmarks.md) for how it runs in
  CI and the latest results.

## Follower replicas

A follower serves reads from a copy of its leader's state, so reads go on
when the leader's node is lost, and can be promoted to a leader (see
Promotion).

- The leader serves its committed log over the `Replication.Follow` gRPC
  stream on its API listener. The stream carries every namespace, so it
  takes a token of its own, `EZU_REPLICATION_TOKEN_FILE`, and is refused
  without one. Only records the leader has synced are shipped, never an
  unfinished batch.
- `EZU_FOLLOW=<leader URL>` (with the same token file) runs the server as
  a follower. Over `https`, `EZU_FOLLOW_CA` verifies the leader, and
  `EZU_TLS_CERT` and `EZU_TLS_KEY` are the client certificate the leader's
  mutual TLS asks for.
- A follower logs what it is shipped in its own `EZU_DATA_DIR`, byte for
  byte at the leader's LSNs, and applies it as recovery replays a log. Its
  applied LSN is its own last LSN, so a restart or a dropped stream
  resumes right after it, and a resent record changes nothing. Queries and
  fence events read the same, with the same event ids.
- Each time it connects, the follower sends the CRC32 and epoch of its
  last record, and the leader compares them with its own record at that
  LSN before shipping more. A follower that followed another history is
  refused (`DATA_LOSS`) and stops. To keep that record checkable, a
  snapshot keeps the segment holding the LSN it covers, so the WAL holds
  up to one segment more than before.
- A follower marks its data directory with a `FOLLOWER` file, written
  before it applies anything, and the server refuses to open a marked
  directory as a leader: it would write past its leader's log. Promotion
  clears it. Releases from before followers ignore the file and would open
  the directory as a leader.
- Every write (API, Flight) is refused with `FAILED_PRECONDITION`. Expiry
  removals come from the leader's log.
- It records the epoch of the records it applies (`wal/EPOCH`, and a
  promotion frame in its log). A record of an older epoch after a newer
  one, a gap in LSNs, or a leader that no longer holds the next record
  (a snapshot dropped it) stops the follower for good. It keeps serving
  what it applied, `GET /healthz` answers 503 (the reason goes to the server
  log), and it needs a rebuild. While it applies, `/healthz` answers 200 with the applied
  LSN, the leader's and the lag.
- The leader drops its log at each snapshot. A follower down for longer
  than that, or new against a leader whose log no longer starts at LSN 1,
  stops and needs a re-seed: stop it, empty its data directory, copy the
  leader's newest `snapshots/<lsn>/` set into `snapshots/` there, and
  start it again. It follows from that LSN. Holding no record of its own
  at that LSN, it sends no fingerprint on its first connect: the copy is
  trusted as it is, and takes the leader's lineage (below) with its first
  epoch.
- On each connect the leader checks the follower's last record (its
  checksum and epoch) against its own at that LSN, and the follower's
  lineage, and refuses a mismatch (`DATA_LOSS`). The lineage
  (`wal/LINEAGE`) lists where each epoch started: its epoch, the LSN its
  records follow, and the node that started it. A follower takes its
  leader's starts with its epochs, so its lineage must be a prefix of the
  leader's holding every start before its applied LSN. Two logs that end
  in the same record but were promoted differently are refused, so a
  follower can switch to another leader: see Promotion. Epochs raised by a
  release without lineage have no start; a directory-local
  `DurableEngine::promote` records one from the next open.
- Encrypted namespaces are not replicated: a follower takes no keys, and a
  sealed record, shipped or in a seeded snapshot, stops it.
- Followers do not ship on: every replica follows the leader directly. A
  refused replication token stops the follower instead of retrying.

## Sync standby

Replication to followers is asynchronous: a leader acknowledges a write once
its own disk has it. A sync standby makes acknowledged writes outlive the
leader's node; promoting it (see Promotion) is the failover.

- A follower with `EZU_NODE_ID` follows over `Replication.Sync` and
  acknowledges `{lsn, epoch}` once a batch is appended and synced in its own
  log, and again on every heartbeat. Acknowledgements are cumulative.
- A leader with `EZU_SYNC_STANDBY=<node id>` (it needs
  `EZU_REPLICATION_TOKEN_FILE`) acknowledges a write only once that node
  acknowledged the write's LSN at the leader's epoch. Acknowledgements from
  any other node or epoch are not counted. Writes wait after the writer
  lock is released, so concurrent writes share one round trip. The API
  waits asynchronously, holding no blocking thread; an embedding host does
  the same with `DurableEngine::deferring` and `acknowledged`, or calls the
  engine directly and blocks. Even a write
  that logs nothing (a stale upsert) waits for the log it answered from.
- Past `EZU_SYNC_TIMEOUT` (milliseconds, default 5000) a write fails with
  `UNAVAILABLE`. It is logged on the leader and may still reach the
  standby, so its outcome is unknown; upserts and deletes are safe to
  retry.
- Lease: with no acknowledgement for 10 seconds, the leader refuses reads
  and writes (`UNAVAILABLE`) and `GET /healthz` answers 503. This bounds
  stale reads by a leader that lost its standby. A leader that starts
  serves nothing until its standby's first acknowledgement, and recovers
  on its own when acknowledgements resume. Leader reads can still show
  writes not yet acknowledged; fence events cannot. The lease runs from
  the leader's own clock: every batch and heartbeat on the sync stream
  carries the time the leader read it, the standby echoes the newest in
  each acknowledgement, and the lease lasts 10 seconds from that time, not
  from when the acknowledgement arrived. An acknowledgement held up in
  transit therefore never keeps the leader serving longer than 10 seconds
  after it sent what that acknowledgement echoes. (An unstamped one, from
  a standby of an earlier release, renews it from arrival; see below.)
- Event gating: `ListEvents` and `Subscribe` serve only events of writes
  the standby acknowledged. A resume point past the leader's last LSN fails
  with `OUT_OF_RANGE` (HTTP 410): the reader saw another history. A
  resume point handed out never passes the acknowledged LSN, and events
  not yet acknowledged are kept past `events_retained` until they are. A
  leader does not record what was acknowledged, so after a restart it
  keeps every event it recovers until the standby acknowledges again and
  the next snapshot trims them.
- A snapshot keeps the log the standby has not acknowledged, so it can
  still be shipped; the next snapshot after the acknowledgement drops it.
- Every other follower is shipped only what the standby acknowledged, so
  none holds a write the standby lacks, and each can follow the standby
  once it is promoted. Their lag includes the standby's round trip. A
  heartbeat announcing an epoch that starts past what such a follower holds
  is taken only once its records reach that start.
- Each batch on the sync stream tells the follower whether it is the
  leader's sync standby; it keeps that in `FOLLOWER` for a promotion.
- Fencing across nodes needs no clock. An old leader cannot acknowledge a
  write without its standby, and the standby follows one leader at one
  epoch: a heartbeat carries the leader's epoch, which the standby takes;
  a leader at a lower epoch than the standby's, or its records, are
  refused, and the standby stops (fail closed, as for a follower; its log
  is intact, restart it against the right leader).
- Losing either node pauses writes until an operator acts: continuing
  without the standby means removing `EZU_SYNC_STANDBY`, which is async
  mode; losing the leader means promoting the standby.
- Without `EZU_SYNC_STANDBY` nothing changes. To roll out, upgrade the
  leader first, then the standby with `EZU_NODE_ID`, then set
  `EZU_SYNC_STANDBY` on the leader. A standby with `EZU_NODE_ID` against an
  older leader gets `UNIMPLEMENTED` and keeps retrying, logged; a follower
  without it never acknowledges, so a leader waiting for it refuses reads
  and writes. A standby from before promotion echoes no stamp; its
  acknowledgements renew the lease from when they arrive, as before, and
  `ezu_sync_unstamped` reports it. It keeps working until it is upgraded,
  so the two need not be upgraded together; it cannot be promoted until
  then. From the first stamped acknowledgement on, the leader refuses
  unstamped ones for the rest of its life (`ezu_sync_unstamped_refused`
  counts them) and its lease follows the newest stamp alone. Only the
  standby's newest sync stream counts: a newer one ends the older
  (`ABORTED`), and what still arrives on that is ignored.
- Residual during that upgrade: while the standby is not upgraded, an
  acknowledgement renews the lease from its arrival, so the bound on stale
  reads by a leader that lost its standby rests on timing. Writes are
  always fenced by acknowledgements. Upgrade standbys before relying on
  promotion.

## Promotion

`Replication.Promote` (gRPC, with the replication token) turns a follower
into a leader in place. Promotion is manual: nothing promotes on its own.

- It needs `EZU_NODE_ID`, which the lineage records. Without `force`, only
  the leader's sync standby is promoted (`FAILED_PRECONDITION` otherwise):
  it holds every acknowledged write.
- `force` is the operator's assertion that the old leader and its standby
  are gone. Ezu cannot check it. Writes the standby acknowledged that the
  forced follower lacks are lost. A forced promotion while the old leader
  and its standby are alive gives two leaders, both taking writes: the
  lineage refuses either one rejoining the other, but it does not prevent
  the split.
- The follower stops following, so it acknowledges nothing more, and waits
  10.5 seconds (the lease plus a twentieth) since its last acknowledgement,
  or since it started when it sent none since: a standby that restarted
  may have acknowledged just before. The old leader's lease ran from a
  time before that acknowledgement left, so by then the old leader refuses
  reads and writes, however late the acknowledgement arrived. This assumes
  the two clocks run at rates no more than 5% apart; they need not agree on
  the time. A follower that failed for any reason other than its leader
  refusing it (durability lost) is not promoted, also when the failure
  happens during the wait: restart it first. Without `force`, the standby
  must also have let out a stamped acknowledgement since it started, so
  its leader counts no unstamped one from an earlier process of it any
  more; until then the promotion is refused. Then it writes the intent into
  `FOLLOWER`, raises the epoch past the highest it followed, adds its start
  to the lineage, begins a segment with the promotion frame, and deletes
  `FOLLOWER`. It serves writes and ships its log at once. LSNs, and so
  event ids, continue after the last record it applied: a subscriber
  resumes on it from its last id without a gap.
- Asked again (also after a restart), it answers the same epoch. A crash
  in between is completed by starting the node as a leader (without
  `EZU_FOLLOW`); started as a follower, it refuses the directory.
- A promoted node runs without a sync standby, in async mode. To have one
  again, restart it with `EZU_SYNC_STANDBY` once another follower follows
  it with `EZU_NODE_ID`.
- Other followers switch by setting `EZU_FOLLOW` to the new leader and
  restarting. Their lineage passes, as they never held a write the standby
  lacked. A node that holds records past the promotion point (the old
  leader, or a follower of it without a sync standby) is refused
  (`DATA_LOSS`) and needs a re-seed. So is a node promoted elsewhere at the
  same point: two promotions never share a lineage.
- In a partition the old leader acknowledges nothing once its standby
  stops following, and serves nothing after the lease.
- Upgrade every node before the first promotion: a follower from before
  lineage sends none and is refused by a leader that has one.

## Bulk interchange (Arrow Flight)

The API listener also serves Arrow Flight, with the same bearer-token
authentication and namespace rule.

- `DoPut` upserts each Arrow batch and answers it with JSON outcome counts.
- `DoGet` takes a ticket holding a JSON query (the `POST /v1/query` body)
  and streams every entity it selects; a world bounding box exports a whole
  collection.

Batches have the columns `collection`, `id`, `geometry`, `ts_ms`, `speed`,
`heading`, `ttl_ms` and `fields`. `geometry` is WKB carrying the GeoArrow
extension type `geoarrow.wkb` (GeoArrow spec 0.2, CRS `OGC:CRS84`), so Arrow
query engines read exports directly. `fields` is a JSON object; integers are
written without a fraction and floats with one, so values round-trip.

### GeoParquet layout

An export written to Parquet is a GeoParquet 1.1 file when it keeps the
columns above unchanged, one row per entity, and carries this file metadata
under the key `geo`:

```json
{
  "version": "1.1.0",
  "primary_column": "geometry",
  "columns": {
    "geometry": {"encoding": "WKB", "geometry_types": []}
  }
}
```

The `crs` member is left out on purpose: absent, it means longitude and
latitude on WGS 84 (`OGC:CRS84`), whereas `null` would mean unknown. Such a
file loads back through `DoPut` as it is.

## Geofences

A fence is a circle or polygon with a selector (collections, field
filters). Every logged entity write is tested against the fences whose
bounds it touches, through an R-tree over fence bounds, and raises `enter`,
`exit` (also on delete and expiry), `dwell` once per stay after `dwell_ms`,
and, for fences that ask, `inside` capped at `inside_per_second` per fence.
An event's id is the LSN of the write that raised it plus its index in that
write. Memberships are logged state and part of snapshots, so a restart
neither re-enters nor resets dwell timers, and replay raises the same events
with the same ids.

The newest `events_retained` events of each namespace are kept in memory
and in snapshots. `Subscribe` (a gRPC server stream) sends them from a
last-seen id, then live, optionally for chosen fences; `ListEvents` (also
`GET /v1/events`) reads them in pages. Delivery is at least once and the
event id is the idempotency key: a reader resumes from its last id across
disconnects and Ezu restarts without gaps. A resume point older than the
newest dropped event fails with `OUT_OF_RANGE` (HTTP 410) instead of
silently skipping events. On a leader, a resume point past its last LSN
fails the same way: it was read from another history. A follower may be
behind its reader and serves such a resume point once it catches up.

## Offline map packages

`POST /v1/packages` with `{"region": [west, south, east, north],
"min_zoom": z0, "max_zoom": z1, "label_filter": {...}}` renders the
namespace's entities and fences in that region as vector tiles in a
PMTiles archive, for devices without a network.

- The archive is the response body. Its manifest is in the
  `x-ezu-package-manifest` header: region, zooms, creation time, the LSN
  the data is at least as new as, size and SHA-256.
- The label filter applies as for queries, and governed mode requires it.
- Zooms go up to 16, with at most 100,000 tiles per package.
- Basemaps are a separate input.

## Labels and governed mode

Entities and fences carry up to 16 opaque labels: 1 to 64 bytes each, with
no control characters, commas or surrounding whitespace. Ezu never interprets them.

A read may carry a label filter, the set of labels the caller may read.
It then sees only entities, fences and events that carry no other label;
an event carries the labels of both its entity and its fence. Filtering
happens inside the index, so pages and limits stay exact. The filter is
`label_filter` in request bodies, gRPC messages and Flight tickets, and
the `x-ezu-readable-labels` header (comma-separated) on HTTP reads without
a body.

With `EZU_GOVERNED=1`, every read (get, query, export, fences, events)
without a filter is refused. Which labels a caller may read is the
caller's decision, made in front of Ezu.

Labels are written to the log and snapshots only where present. Data
without labels stays readable by older binaries; labeled data makes them
refuse to start rather than drop the labels.

## Encryption at rest

An embedding host passes `DurabilityConfig::keys`: a key provider (the
`sekai-keys` `KeyProvider` contract) and an unwrap grant per namespace. Each
granted namespace gets a data key that is minted by Ezu, wrapped by the
provider and stored only wrapped under `keys/`. Its WAL records and snapshot
data are sealed with AES-256-GCM, bound to their LSN, namespace and key id.
Namespace names and key ids stay readable as metadata.

This protects confidentiality: without its key, an encrypted namespace's
data cannot be read from disk, and sealed content that was altered fails to
open. It does not protect against someone who can write the data directory
replacing data with forged plaintext or rolling it back; that needs an
authenticated chain over the whole store and is not provided.

Snapshots are per-namespace sets (`snapshots/<lsn>/`, format version 2);
version 1 single-file snapshots still load. `refresh_keys` rechecks every
key with the provider (the server does so with each expiry sweep). A revoked
key or withdrawn grant seals that namespace alone: it refuses reads and
writes, its data leaves memory and is carried, still sealed, through later
snapshots. It opens again on the next restart that can unwrap its key.
`rotate_key` starts a new key at once; the next snapshot after a write
re-seals older data under it and retires the old key. Granting a key to a
namespace with plaintext data seals that data the same way.
