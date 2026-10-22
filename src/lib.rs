//! Ezu: live spatial engine. An index over moving entities, spatial queries,
//! geofencing events and recent tracks.

pub mod entity;
pub mod geometry;
pub mod index;

pub use entity::{Entity, EntityError, FieldValue, Identity, Namespace};
pub use geometry::{BoundingBox, Geometry, GeometryError, Position};
pub use index::{Engine, IndexError, Quotas, Upsert};
