//! Byte encoding shared by the WAL and snapshots: geometries as little-endian
//! WKB, and the rest of an entity as length-prefixed fields. Decoding never
//! trusts its input: every length is checked against what is left.

use crate::entity::{Entity, FieldValue};
use crate::geometry::{Geometry, Position};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecError(pub &'static str);

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "corrupt record: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

const WKB_POINT: u32 = 1;
const WKB_LINE: u32 = 2;
const WKB_POLYGON: u32 = 3;
/// Most positions or rings one decoded geometry may claim, so a corrupt
/// count cannot ask for an enormous allocation.
const MAX_COUNT: u32 = 1 << 24;

#[derive(Default)]
pub struct Writer(pub Vec<u8>);

impl Writer {
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn f64(&mut self, v: f64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.0.extend_from_slice(v);
    }
    pub fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
}

pub struct Reader<'a>(pub &'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.0.len() < n {
            return Err(CodecError("truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CodecError("truncated"))
    }
    pub fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.array()?))
    }
    pub fn f64(&mut self) -> Result<f64, CodecError> {
        Ok(f64::from_le_bytes(self.array()?))
    }
    pub fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let len = self.u32()? as usize;
        self.take(len)
    }
    pub fn str(&mut self) -> Result<String, CodecError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| CodecError("bad utf-8"))
    }
    pub fn done(&self) -> Result<(), CodecError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(CodecError("trailing bytes"))
        }
    }
}

pub fn geometry_to_wkb(geometry: &Geometry) -> Vec<u8> {
    let mut out = Writer::default();
    out.u8(1);
    let positions = |out: &mut Writer, positions: &[Position]| {
        out.u32(positions.len() as u32);
        for p in positions {
            out.f64(p.lon);
            out.f64(p.lat);
        }
    };
    match geometry {
        Geometry::Point(p) => {
            out.u32(WKB_POINT);
            out.f64(p.lon);
            out.f64(p.lat);
        }
        Geometry::Line(line) => {
            out.u32(WKB_LINE);
            positions(&mut out, line);
        }
        Geometry::Polygon(rings) => {
            out.u32(WKB_POLYGON);
            out.u32(rings.len() as u32);
            for ring in rings {
                positions(&mut out, ring);
            }
        }
    }
    out.0
}

pub fn geometry_from_wkb(bytes: &[u8]) -> Result<Geometry, CodecError> {
    let mut input = Reader(bytes);
    if input.u8()? != 1 {
        return Err(CodecError("wkb is not little-endian"));
    }
    let count = |input: &mut Reader<'_>| -> Result<u32, CodecError> {
        let n = input.u32()?;
        (n <= MAX_COUNT).then_some(n).ok_or(CodecError("wkb count"))
    };
    let positions = |input: &mut Reader<'_>| -> Result<Vec<Position>, CodecError> {
        let n = count(input)?;
        (0..n)
            .map(|_| Ok(Position::new(input.f64()?, input.f64()?)))
            .collect()
    };
    let geometry = match input.u32()? {
        WKB_POINT => Geometry::Point(Position::new(input.f64()?, input.f64()?)),
        WKB_LINE => Geometry::Line(positions(&mut input)?),
        WKB_POLYGON => {
            let n = count(&mut input)?;
            Geometry::Polygon(
                (0..n)
                    .map(|_| positions(&mut input))
                    .collect::<Result<_, _>>()?,
            )
        }
        _ => return Err(CodecError("wkb geometry type")),
    };
    input.done()?;
    Ok(geometry)
}

pub fn encode_fields(fields: &BTreeMap<String, FieldValue>) -> Vec<u8> {
    let mut out = Writer::default();
    out.u32(fields.len() as u32);
    for (name, value) in fields {
        out.str(name);
        match value {
            FieldValue::Bool(v) => {
                out.u8(0);
                out.u8(u8::from(*v));
            }
            FieldValue::Int(v) => {
                out.u8(1);
                out.i64(*v);
            }
            FieldValue::Float(v) => {
                out.u8(2);
                out.f64(*v);
            }
            FieldValue::Text(v) => {
                out.u8(3);
                out.str(v);
            }
        }
    }
    out.0
}

pub fn decode_fields(bytes: &[u8]) -> Result<BTreeMap<String, FieldValue>, CodecError> {
    let mut input = Reader(bytes);
    let n = input.u32()?;
    if n > 64 {
        return Err(CodecError("field count"));
    }
    let mut fields = BTreeMap::new();
    for _ in 0..n {
        let name = input.str()?;
        let value = match input.u8()? {
            0 => FieldValue::Bool(input.u8()? != 0),
            1 => FieldValue::Int(input.i64()?),
            2 => FieldValue::Float(input.f64()?),
            3 => FieldValue::Text(input.str()?),
            _ => return Err(CodecError("field type")),
        };
        fields.insert(name, value);
    }
    input.done()?;
    Ok(fields)
}

fn optional_f64(out: &mut Writer, v: Option<f64>) {
    match v {
        Some(v) => {
            out.u8(1);
            out.f64(v);
        }
        None => out.u8(0),
    }
}

fn read_optional_f64(input: &mut Reader<'_>) -> Result<Option<f64>, CodecError> {
    match input.u8()? {
        0 => Ok(None),
        1 => Ok(Some(input.f64()?)),
        _ => Err(CodecError("option tag")),
    }
}

pub fn encode_entity(out: &mut Writer, entity: &Entity) {
    out.str(&entity.collection);
    out.str(&entity.id);
    out.bytes(&geometry_to_wkb(&entity.geometry));
    out.i64(entity.ts_ms);
    optional_f64(out, entity.speed);
    optional_f64(out, entity.heading);
    match entity.ttl_ms {
        Some(ttl) => {
            out.u8(1);
            out.i64(ttl);
        }
        None => out.u8(0),
    }
    out.bytes(&encode_fields(&entity.fields));
}

pub fn decode_entity(input: &mut Reader<'_>) -> Result<Entity, CodecError> {
    Ok(Entity {
        collection: input.str()?,
        id: input.str()?,
        geometry: geometry_from_wkb(input.bytes()?)?,
        ts_ms: input.i64()?,
        speed: read_optional_f64(input)?,
        heading: read_optional_f64(input)?,
        ttl_ms: match input.u8()? {
            0 => None,
            1 => Some(input.i64()?),
            _ => return Err(CodecError("option tag")),
        },
        fields: decode_fields(input.bytes()?)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entities_round_trip_and_corrupt_bytes_are_refused() {
        let entity = Entity {
            collection: "c".into(),
            id: "e".into(),
            geometry: Geometry::Polygon(vec![vec![
                Position::new(0.0, 0.0),
                Position::new(1.0, 0.0),
                Position::new(1.0, 1.0),
                Position::new(0.0, 0.0),
            ]]),
            ts_ms: 42,
            speed: Some(1.5),
            heading: None,
            ttl_ms: Some(9),
            fields: BTreeMap::from([
                ("a".into(), FieldValue::Text("x".into())),
                ("b".into(), FieldValue::Int(-3)),
                ("c".into(), FieldValue::Bool(true)),
                ("d".into(), FieldValue::Float(0.25)),
            ]),
        };
        let mut out = Writer::default();
        encode_entity(&mut out, &entity);
        let mut input = Reader(&out.0);
        assert_eq!(decode_entity(&mut input).unwrap(), entity);
        input.done().unwrap();
        for cut in 0..out.0.len() {
            assert!(
                decode_entity(&mut Reader(&out.0[..cut])).is_err(),
                "cut {cut}"
            );
        }
    }
}
