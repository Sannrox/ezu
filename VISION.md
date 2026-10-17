# Ezu vision

Ezu (絵図, map) is a standalone, governance-unaware live spatial engine.

## Vision
- A live index of changing entities (points, shapes, tracks) per namespace.
- Spatial queries: radius, bbox, polygon contains/intersects, k-nearest.
- Geofencing with enter, exit and dwell events over a resumable stream.
- Recent track windows per entity.
- Batch analytics and long history belong to other systems. Ezu exports GeoArrow and GeoParquet for them.
- Authorization, markings and business actions are enforced by the caller in front of Ezu. Ezu only maps an authenticated identity to a namespace.
- Single binary, WAL plus snapshots, no network calls at runtime, so it is suitable for air-gapped sites.

## Roadmap
1. **v0.1 MVP:** points and polygons; radius, bbox, polygon and kNN queries; static fences with enter, exit and dwell; resumable event stream; namespaces and quotas; WAL plus snapshot recovery; gRPC and HTTP; JWT/JWKS auth hook; Arrow Flight bulk in/out; metrics and diagnostics; benchmarks at 10^6 and 10^7.
2. **v0.2:** track windows, roaming fences, follower read replicas, live vector tiles, GeoParquet snapshots to object storage.
3. **Later:** Raft HA, sharding, optional robust-geometry and reprojection features.
