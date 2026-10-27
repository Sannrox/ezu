//! Ezu: live spatial engine. An index over moving entities, spatial queries,
//! geofencing events and recent tracks.

pub mod api;
pub mod auth;
mod codec;
pub mod diagnostics;
pub mod durable;
pub mod entity;
pub mod fence;
pub mod flight;
pub mod follow;
pub mod geometry;
pub mod index;
pub mod keys;
mod metrics;
pub mod query;
mod snapshot;
pub mod tiles;
mod wal;

pub use durable::{
    DurabilityConfig, DurableEngine, DurableError, EpochStart, Pending, RebuildSource,
    ReplicaError, SyncStandby,
};
pub use entity::{Entity, EntityError, FieldValue, Identity, Namespace};
pub use geometry::{BoundingBox, Geometry, GeometryError, Position};
pub use index::{Engine, IndexError, Quotas, Upsert};
pub use query::{Filter, Query, QueryPage, QueryRequest, Relation};
