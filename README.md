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
`POST /v1/query`, `GET /v1/status`. JSON bodies are the same messages in
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
