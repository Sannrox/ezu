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
`PUT|GET|DELETE /v1/fences/{id}` and `GET /v1/events`. JSON bodies are the same messages in
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
  durability failure (reopen to recover).
- `GET /metrics` serves Prometheus text:
  - entity count, logged writes and fence events;
  - query latency as a histogram;
  - WAL bytes and records since the last snapshot (the WAL lag);
  - snapshot age, sealed namespaces and an `ezu_up` gauge.

  Every series is aggregated over namespaces, so neither endpoint takes a
  token or reveals tenant data.
- `ezu diagnostics` prints a JSON support bundle for `EZU_DATA_DIR`. It holds
  WAL and snapshot counts, sizes, LSNs and format versions, and the names
  (not values) of the `EZU_*` settings present. It never includes secrets or
  namespace names, and it runs next to a live server.
- `make static` builds a fully static Linux binary. The `Dockerfile` copies
  it into an otherwise empty image, so there is no base image to pin. Ezu
  makes no network calls of its own; CI starts the static binary with no
  network but loopback and checks it serves.
- `cargo deny` in CI refuses copyleft licenses in the default build (see
  `deny.toml`).

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
silently skipping events.

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
