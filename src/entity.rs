//! The entities Ezu indexes and the identity that scopes them.

use crate::geometry::{Geometry, GeometryError};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Most typed filter fields one entity carries.
pub const MAX_FIELDS: usize = 8;
const MAX_NAME_BYTES: usize = 64;
const MAX_TEXT_BYTES: usize = 64;
/// Most labels one entity or fence carries.
pub const MAX_LABELS: usize = 16;

/// Opaque marking labels. Ezu never interprets them: it stores them, and a
/// caller that names the labels it may read sees only what carries no
/// other label.
pub type Labels = BTreeSet<String>;

/// Whether something carrying `labels` is visible to a reader allowed
/// `readable`; `None` means the reader gave no filter and sees everything.
pub fn visible(labels: &Labels, readable: Option<&Labels>) -> bool {
    readable.is_none_or(|readable| labels.is_subset(readable))
}

/// A label: 1-64 bytes, no control characters, no commas and no leading or
/// trailing whitespace, so a comma-separated HTTP header lists labels
/// losslessly.
pub(crate) fn valid_labels(labels: &Labels) -> bool {
    labels.len() <= MAX_LABELS
        && labels.iter().all(|label| {
            !label.is_empty()
                && label.len() <= MAX_NAME_BYTES
                && label.trim() == label
                && !label.chars().any(|c| c.is_control() || c == ',')
        })
}

/// The identity an authentication hook established for a caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub subject: String,
    namespace: String,
}

impl Identity {
    /// Built by the authentication hook only, from a verified credential.
    pub fn authenticated(subject: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            namespace: namespace.into(),
        }
    }
}

/// A tenant namespace. It is only ever taken from an [`Identity`]; requests
/// carry no namespace of their own, so a body cannot name another tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Namespace(String);

impl Namespace {
    pub fn of(identity: &Identity) -> Self {
        Self(identity.namespace.clone())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A namespace read back from Ezu's own storage, which only ever held
    /// namespaces taken from identities.
    pub(crate) fn stored(name: String) -> Self {
        Self(name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

/// One live entity in a collection. Its key within a namespace is
/// `(collection, id)`.
#[derive(Debug, Clone, PartialEq)]
pub struct Entity {
    pub collection: String,
    pub id: String,
    pub geometry: Geometry,
    /// Observation time, Unix milliseconds. A write older than the stored
    /// entity is ignored.
    pub ts_ms: i64,
    /// Metres per second.
    pub speed: Option<f64>,
    /// Degrees clockwise from north, in `[0, 360)`.
    pub heading: Option<f64>,
    /// The entity expires `ttl_ms` after `ts_ms`; `None` never expires.
    pub ttl_ms: Option<i64>,
    pub fields: BTreeMap<String, FieldValue>,
    /// Opaque marking labels, for callers that filter by them.
    pub labels: Labels,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntityError {
    Geometry(GeometryError),
    Name,
    Speed,
    Heading,
    Ttl,
    TooManyFields,
    Field,
    Labels,
}

impl fmt::Display for EntityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Geometry(error) => write!(f, "invalid geometry: {error}"),
            Self::Name => write!(
                f,
                "collection and id must be 1-{MAX_NAME_BYTES} bytes without control characters"
            ),
            Self::Speed => f.write_str("speed must be finite and not negative"),
            Self::Heading => f.write_str("heading must be in [0, 360)"),
            Self::Ttl => f.write_str("ttl must be positive"),
            Self::TooManyFields => write!(f, "an entity carries at most {MAX_FIELDS} fields"),
            Self::Labels => write!(
                f,
                "at most {MAX_LABELS} labels of 1-{MAX_NAME_BYTES} bytes, without control \
                 characters, commas or surrounding whitespace"
            ),
            Self::Field => write!(
                f,
                "field names are 1-{MAX_NAME_BYTES} bytes; text values at most \
                 {MAX_TEXT_BYTES} bytes; floats finite"
            ),
        }
    }
}

/// Collection, id and field names: 1-64 bytes, no control characters.
pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_BYTES && !name.chars().any(char::is_control)
}

impl Entity {
    pub fn validate(&self) -> Result<(), EntityError> {
        if !valid_name(&self.collection) || !valid_name(&self.id) {
            return Err(EntityError::Name);
        }
        self.geometry.validate().map_err(EntityError::Geometry)?;
        if self.speed.is_some_and(|s| !s.is_finite() || s < 0.0) {
            return Err(EntityError::Speed);
        }
        if self
            .heading
            .is_some_and(|h| !h.is_finite() || !(0.0..360.0).contains(&h))
        {
            return Err(EntityError::Heading);
        }
        if self.ttl_ms.is_some_and(|ttl| ttl <= 0) {
            return Err(EntityError::Ttl);
        }
        if self.fields.len() > MAX_FIELDS {
            return Err(EntityError::TooManyFields);
        }
        let field_ok = |(name, value): (&String, &FieldValue)| {
            valid_name(name)
                && match value {
                    FieldValue::Text(text) => text.len() <= MAX_TEXT_BYTES,
                    FieldValue::Float(x) => x.is_finite(),
                    FieldValue::Bool(_) | FieldValue::Int(_) => true,
                }
        };
        if !self.fields.iter().all(field_ok) {
            return Err(EntityError::Field);
        }
        if !valid_labels(&self.labels) {
            return Err(EntityError::Labels);
        }
        Ok(())
    }

    /// Whether the entity has expired by `now_ms`.
    pub fn expired(&self, now_ms: i64) -> bool {
        self.ttl_ms
            .is_some_and(|ttl| self.ts_ms.saturating_add(ttl) <= now_ms)
    }
}
