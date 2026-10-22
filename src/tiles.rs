//! Offline map packages: the entity and fence layers of one namespace and
//! region, rendered as vector tiles (format 2.1) into a PMTiles v3 archive,
//! for field devices without a network.
//!
//! A package holds two layers, `entities` and `fences`, for every tile of
//! the region from `min_zoom` to `max_zoom`. Reads apply the caller's label
//! filter, as queries do, so nothing outside it enters the package. The
//! manifest names the region, zoom range, creation time, the LSN the data is
//! at least as new as, the archive's size and its SHA-256 digest. Basemaps are
//! a separate input and not rendered here.

use crate::durable::{DurableEngine, DurableError};
use crate::entity::{Entity, FieldValue, Labels, Namespace};
use crate::fence::{Fence, Shape};
use crate::geometry::{BoundingBox, Geometry, Position, encloses_pole, polar_north, short_step};
use crate::index::IndexError;
use crate::query::{EARTH_RADIUS_M, MAX_LIMIT};
use pmtiles::{PmTilesWriter, TileCoord, TileId, TileType};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::f64::consts::PI;

/// Tile coordinates span `0..EXTENT` on each axis.
const EXTENT: u32 = 4096;
/// Highest zoom a package renders.
pub const MAX_ZOOM: u8 = 16;
/// Most tiles one package holds; a larger request is refused.
pub const MAX_TILES: u64 = 100_000;
/// Most feature placements (one feature in one tile) a package renders; a
/// larger one is refused before it takes the memory.
pub const MAX_PLACEMENTS: u64 = 5_000_000;
/// Most source vertices (entities and fences) a package reads.
pub const MAX_INPUT_VERTICES: u64 = 5_000_000;
/// Most memory the entities a package reads may take, roughly.
pub const MAX_SOURCE_BYTES: u64 = 512 << 20;
/// Most vertices encoded across all placements, which bounds the work.
pub const MAX_VERTEX_WORK: u64 = 50_000_000;
/// Largest archive a package may grow to.
pub const MAX_ARCHIVE_BYTES: usize = 256 << 20;
/// Web Mercator's latitude limit.
const MAX_LAT: f64 = 85.051_128_78;

/// What to package.
#[derive(Debug, Clone, PartialEq)]
pub struct PackageRequest {
    /// `[west, south, east, north]`; `west < east`, within the Mercator range.
    pub region: [f64; 4],
    pub min_zoom: u8,
    pub max_zoom: u8,
    /// The labels the reader may see; `None` packages everything.
    pub readable: Option<Labels>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    pub region: [f64; 4],
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub created_ms: i64,
    /// Every write up to this LSN is in the package (later ones may be too).
    pub lsn: u64,
    pub size: u64,
    /// Hex SHA-256 of the archive.
    pub sha256: String,
}

/// The vector tile messages, by field number.
mod mvt {
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Tile {
        #[prost(message, repeated, tag = "3")]
        pub layers: Vec<Layer>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Layer {
        #[prost(uint32, required, tag = "15")]
        pub version: u32,
        #[prost(string, required, tag = "1")]
        pub name: String,
        #[prost(message, repeated, tag = "2")]
        pub features: Vec<Feature>,
        #[prost(string, repeated, tag = "3")]
        pub keys: Vec<String>,
        #[prost(message, repeated, tag = "4")]
        pub values: Vec<Value>,
        #[prost(uint32, optional, tag = "5")]
        pub extent: Option<u32>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Feature {
        #[prost(uint64, optional, tag = "1")]
        pub id: Option<u64>,
        #[prost(uint32, repeated, packed = "true", tag = "2")]
        pub tags: Vec<u32>,
        /// 1 point, 2 line, 3 polygon.
        #[prost(int32, optional, tag = "3")]
        pub r#type: Option<i32>,
        #[prost(uint32, repeated, packed = "true", tag = "4")]
        pub geometry: Vec<u32>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Value {
        #[prost(string, optional, tag = "1")]
        pub string_value: Option<String>,
        #[prost(double, optional, tag = "3")]
        pub double_value: Option<f64>,
        #[prost(sint64, optional, tag = "6")]
        pub sint_value: Option<i64>,
        #[prost(bool, optional, tag = "7")]
        pub bool_value: Option<bool>,
    }
}

pub use mvt::{Feature as TileFeature, Layer as TileLayer, Tile as VectorTile};

/// A position in world pixels at zoom `z`: `0..2^z * EXTENT` on each axis.
fn world(p: Position, z: u8) -> (f64, f64) {
    let scale = f64::from(1u32 << z) * f64::from(EXTENT);
    let lat = p.lat.clamp(-MAX_LAT, MAX_LAT).to_radians();
    let x = (p.lon + 180.0) / 360.0 * scale;
    let y = (1.0 - (lat.tan() + 1.0 / lat.cos()).ln() / PI) / 2.0 * scale;
    (x, y)
}

/// The tiles a world-pixel box touches at zoom `z`, clamped to the grid.
fn tile_range(min: (f64, f64), max: (f64, f64), z: u8) -> (u32, u32, u32, u32) {
    let last = (1u32 << z) - 1;
    let tile = |v: f64| ((v / f64::from(EXTENT)).floor().max(0.0) as u32).min(last);
    (tile(min.0), tile(min.1), tile(max.0), tile(max.1))
}

/// A feature's shape: rings or lines of positions, by kind.
enum Drawn {
    Point(Position),
    Line(Vec<Position>),
    Polygon(Vec<Vec<Position>>),
    /// Everywhere except inside these rings: a circle larger than a
    /// hemisphere, whose boundary surrounds the cap it leaves out.
    Cover(Vec<Vec<Position>>),
}

/// World copies a drawn shape may be encoded in: its continuous longitude
/// span in turns, plus the neighbours either side.
fn copies(drawn: &Drawn) -> u64 {
    let span = |path: &[Position]| {
        let lons = unwrapped(path, path.first().map_or(0.0, |p| p.lon));
        let (min, max) = lons.iter().fold((f64::MAX, f64::MIN), |m, p| {
            (m.0.min(p.lon), m.1.max(p.lon))
        });
        if lons.is_empty() { 0.0 } else { max - min }
    };
    let widest = match drawn {
        Drawn::Point(_) => 0.0,
        Drawn::Line(line) => span(line),
        Drawn::Polygon(rings) | Drawn::Cover(rings) => {
            rings.iter().map(|ring| span(ring)).fold(0.0, f64::max)
        }
    };
    (widest / 360.0).ceil() as u64 + 3
}

/// Bytes a feature's properties take in a tile, roughly.
fn property_bytes(properties: &[(String, mvt::Value)]) -> u64 {
    properties
        .iter()
        .map(|(key, value)| {
            key.len() as u64 + value.string_value.as_ref().map_or(8, |v| v.len() as u64) + 8
        })
        .sum()
}

/// Vertices a drawn shape encodes per world copy.
fn vertices(drawn: &Drawn) -> u64 {
    match drawn {
        Drawn::Point(_) => 1,
        Drawn::Line(line) => line.len() as u64,
        Drawn::Polygon(rings) | Drawn::Cover(rings) => {
            rings.iter().map(|ring| ring.len() as u64 + 2).sum::<u64>() + 4
        }
    }
}

fn drawn(geometry: &Geometry) -> Drawn {
    match geometry {
        Geometry::Point(p) => Drawn::Point(*p),
        Geometry::Line(line) => Drawn::Line(line.clone()),
        Geometry::Polygon(rings) => Drawn::Polygon(rings.clone()),
    }
}

/// A circle fence as drawn: a 64-sided polygon, or beyond a hemisphere
/// everything but the cap around its antipode.
fn circle_drawn(center: Position, meters: f64) -> Drawn {
    let half = PI / 2.0 * EARTH_RADIUS_M;
    if meters <= half {
        return Drawn::Polygon(circle(center, meters));
    }
    let antipode = Position::new((center.lon + 360.0).rem_euclid(360.0) - 180.0, -center.lat);
    Drawn::Cover(circle(antipode, PI * EARTH_RADIUS_M - meters))
}

/// A circle as a 64-sided polygon.
fn circle(center: Position, meters: f64) -> Vec<Vec<Position>> {
    let angular = meters / EARTH_RADIUS_M;
    let (lat, lon) = (center.lat.to_radians(), center.lon.to_radians());
    let mut ring: Vec<Position> = (0..64)
        .map(|i| {
            let bearing = f64::from(i) / 64.0 * 2.0 * PI;
            let lat2 =
                (lat.sin() * angular.cos() + lat.cos() * angular.sin() * bearing.cos()).asin();
            let lon2 = lon
                + (bearing.sin() * angular.sin() * lat.cos())
                    .atan2(angular.cos() - lat.sin() * lat2.sin());
            Position::new(
                (lon2.to_degrees() + 540.0).rem_euclid(360.0) - 180.0,
                lat2.to_degrees(),
            )
        })
        .collect();
    ring.push(ring[0]);
    vec![ring]
}

fn zigzag(v: i64) -> u32 {
    ((v << 1) ^ (v >> 63)) as u32
}

fn command(id: u32, count: usize) -> u32 {
    (id & 7) | ((count as u32) << 3)
}

/// `path` with longitudes made continuous: the first near `around`, each
/// next one the short way from the one before, as the geometry model reads
/// edges. Across the antimeridian this leaves [-180, 180], so the tile on
/// either side draws the same crossing.
fn unwrapped(path: &[Position], around: f64) -> Vec<Position> {
    let mut out: Vec<Position> = Vec::with_capacity(path.len());
    for (i, p) in path.iter().enumerate() {
        // Each step is taken between the original longitudes, so a path that
        // winds round more than once keeps winding.
        let lon = match out.last() {
            Some(previous) => previous.lon + short_step(path[i - 1].lon, p.lon),
            None => around + short_step(around, p.lon),
        };
        out.push(Position::new(lon, p.lat));
    }
    out
}

/// A position in world pixels at zoom `z`, without wrapping the longitude.
fn world_unwrapped(p: Position, z: u8) -> (f64, f64) {
    let (_, y) = world(Position::new(0.0, p.lat), z);
    let scale = f64::from(1u32 << z) * f64::from(EXTENT);
    ((p.lon + 180.0) / 360.0 * scale, y)
}

/// How far past the tile edge geometry is kept, in tile units.
const MARGIN: i64 = 64;

/// Command stream of one feature: paths with cursor-relative coordinates.
/// The format holds 32-bit signed coordinates and deltas; a feature that
/// needs more (only a degenerate ring winding round the world many times)
/// is marked `overflow` and left out rather than written corrupted.
#[derive(Default)]
struct Commands {
    out: Vec<u32>,
    cursor: (i64, i64),
    overflow: bool,
}

impl Commands {
    fn delta(&mut self, point: (i64, i64)) {
        let fits = |v: i64| i32::try_from(v).is_ok();
        let (dx, dy) = (point.0 - self.cursor.0, point.1 - self.cursor.1);
        if !(fits(dx) && fits(dy) && fits(point.0) && fits(point.1)) {
            self.overflow = true;
        }
        self.out.extend([zigzag(dx), zigzag(dy)]);
        self.cursor = point;
    }

    /// The commands, or none for a feature the format cannot hold.
    fn finish(self) -> Vec<u32> {
        if self.overflow { Vec::new() } else { self.out }
    }

    /// One MoveTo for all `points`, as a (multi)point is written.
    fn points(&mut self, points: &[(i64, i64)]) {
        if points.is_empty() {
            return;
        }
        self.out.push(command(1, points.len()));
        for point in points {
            self.delta(*point);
        }
    }

    fn path(&mut self, points: &[(i64, i64)], close: bool) {
        let Some((first, rest)) = points.split_first() else {
            return;
        };
        self.out.push(command(1, 1));
        self.delta(*first);
        if !rest.is_empty() {
            self.out.push(command(2, rest.len()));
            for point in rest {
                self.delta(*point);
            }
        }
        if close {
            self.out.push(command(7, 1));
        }
    }
}

/// Whether points come within the tile and its margin.
fn in_reach(points: &[(i64, i64)]) -> bool {
    let (low, high) = (-MARGIN, i64::from(EXTENT) + MARGIN);
    let (mut min, mut max) = ((i64::MAX, i64::MAX), (i64::MIN, i64::MIN));
    for p in points {
        min = (min.0.min(p.0), min.1.min(p.1));
        max = (max.0.max(p.0), max.1.max(p.1));
    }
    !points.is_empty() && min.0 <= high && max.0 >= low && min.1 <= high && max.1 >= low
}

/// Twice the signed area of a ring, in tile units (wide enough for rings a
/// world across).
fn polygon_area(points: &[(i64, i64)]) -> i128 {
    points
        .iter()
        .zip(points.iter().cycle().skip(1))
        .map(|(a, b)| i128::from(a.0) * i128::from(b.1) - i128::from(b.0) * i128::from(a.1))
        .sum()
}

/// A ring clipped to the square `low..=high` on both axes
/// (Sutherland-Hodgman against each edge in turn).
fn clip(points: &[(i64, i64)], low: i64, high: i64) -> Vec<(i64, i64)> {
    type Edge = (fn((i64, i64), i64) -> bool, bool, i64);
    let edges: [Edge; 4] = [
        (|p, v| p.0 >= v, true, low),
        (|p, v| p.0 <= v, true, high),
        (|p, v| p.1 >= v, false, low),
        (|p, v| p.1 <= v, false, high),
    ];
    let mut ring = points.to_vec();
    for (inside, vertical, value) in edges {
        let input = std::mem::take(&mut ring);
        let Some(&last) = input.last() else {
            break;
        };
        let cross = |a: (i64, i64), b: (i64, i64)| {
            let (a0, b0) = (a.0 as f64, b.0 as f64);
            let (a1, b1) = (a.1 as f64, b.1 as f64);
            let v = value as f64;
            if vertical {
                let t = (v - a0) / (b0 - a0);
                (value, (a1 + t * (b1 - a1)).round() as i64)
            } else {
                let t = (v - a1) / (b1 - a1);
                ((a0 + t * (b0 - a0)).round() as i64, value)
            }
        };
        let mut previous = last;
        for point in input {
            match (inside(point, value), inside(previous, value)) {
                (true, true) => ring.push(point),
                (true, false) => ring.extend([cross(previous, point), point]),
                (false, true) => ring.push(cross(previous, point)),
                (false, false) => {}
            }
            previous = point;
        }
    }
    ring
}

/// The pieces of a line inside the square `low..=high` (each segment
/// clipped on its own, Liang-Barsky), so no coordinate leaves the tile and
/// its margin.
fn clip_line(points: &[(i64, i64)], low: i64, high: i64) -> Vec<Vec<(i64, i64)>> {
    let (lo, hi) = (low as f64, high as f64);
    let mut pieces: Vec<Vec<(i64, i64)>> = Vec::new();
    let mut open = false;
    for pair in points.windows(2) {
        let (a, b) = (
            (pair[0].0 as f64, pair[0].1 as f64),
            (pair[1].0 as f64, pair[1].1 as f64),
        );
        let (dx, dy) = (b.0 - a.0, b.1 - a.1);
        let (mut t0, mut t1) = (0.0f64, 1.0f64);
        let mut keep = true;
        for (p, q) in [
            (-dx, a.0 - lo),
            (dx, hi - a.0),
            (-dy, a.1 - lo),
            (dy, hi - a.1),
        ] {
            if p == 0.0 {
                keep &= q >= 0.0;
            } else {
                let r = q / p;
                if p < 0.0 {
                    t0 = t0.max(r);
                } else {
                    t1 = t1.min(r);
                }
            }
        }
        if !keep || t0 > t1 {
            open = false;
            continue;
        }
        let at = |t: f64| ((a.0 + t * dx).round() as i64, (a.1 + t * dy).round() as i64);
        let (start, end) = (at(t0), at(t1));
        if !open || t0 > 0.0 {
            pieces.push(vec![start]);
        }
        if let Some(piece) = pieces.last_mut() {
            piece.push(end);
        }
        // The next segment continues this piece only if this one reached
        // its end unclipped.
        open = t1 >= 1.0;
    }
    pieces
        .into_iter()
        .map(|mut piece| {
            piece.dedup();
            piece
        })
        .filter(|piece| piece.len() >= 2)
        .collect()
}

/// A ring ready to write: deduplicated, open (ClosePath closes it) and
/// oriented as `exterior` or hole; `None` when it rounds to no area.
fn ring(points: Vec<(i64, i64)>, exterior: bool) -> Option<Vec<(i64, i64)>> {
    let mut points = points;
    points.dedup();
    if points.len() > 1 && points.first() == points.last() {
        points.pop();
    }
    if points.len() < 3 {
        return None;
    }
    // In tile space (y down) an exterior ring has positive area, holes
    // negative; a ring that rounds to no area is invalid.
    let area = polygon_area(&points);
    if area == 0 {
        return None;
    }
    if exterior != (area > 0) {
        points.reverse();
    }
    Some(points)
}

/// The geometry commands of `drawn` in tile `(x, y)` at zoom `z`. Paths are
/// made continuous around the tile, and drawn in each world copy (shifted
/// by a full turn) that reaches it, so a crossing of the antimeridian shows
/// on both sides even where one tile spans more than one copy.
fn encode(drawn: &Drawn, z: u8, x: u32, y: u32) -> (i32, Vec<u32>) {
    let origin = (f64::from(x * EXTENT), f64::from(y * EXTENT));
    let center = (f64::from(x) + 0.5) / f64::from(1u32 << z) * 360.0 - 180.0;
    let local = |p: &Position, shift: f64| {
        let (wx, wy) = world_unwrapped(Position::new(p.lon + shift, p.lat), z);
        (
            (wx - origin.0).round() as i64,
            (wy - origin.1).round() as i64,
        )
    };
    // A path in tile units. Edges are straight in longitude and latitude,
    // which Mercator bends, so an edge is bisected until its drawn chord is
    // within a few units of the true path. A piece's curve stays within its
    // endpoints' box (both coordinates move monotonically), so pieces away
    // from the tile are never refined: the work follows what is drawn.
    let traced = |path: &[Position], shift: f64| -> Vec<(i64, i64)> {
        const TOLERANCE: f64 = 4.0;
        const DEPTH: u32 = 24;
        let (low, high) = (-(MARGIN as f64), f64::from(EXTENT) + MARGIN as f64);
        let at = |p: Position| {
            let (wx, wy) = world_unwrapped(Position::new(p.lon + shift, p.lat), z);
            (wx - origin.0, wy - origin.1)
        };
        fn refine(
            from: (Position, (f64, f64)),
            to: (Position, (f64, f64)),
            depth: u32,
            at: &dyn Fn(Position) -> (f64, f64),
            bounds: (f64, f64),
            out: &mut Vec<(i64, i64)>,
        ) {
            let (a, b) = (from.1, to.1);
            let near = a.0.min(b.0) <= bounds.1
                && a.0.max(b.0) >= bounds.0
                && a.1.min(b.1) <= bounds.1
                && a.1.max(b.1) >= bounds.0;
            if near && depth > 0 {
                let point = |t: f64| {
                    Position::new(
                        from.0.lon + (to.0.lon - from.0.lon) * t,
                        from.0.lat + (to.0.lat - from.0.lat) * t,
                    )
                };
                // The quarter points too: across the equator the bend is
                // symmetric and the midpoint alone sits on the chord.
                let off = [0.25, 0.5, 0.75].into_iter().any(|t| {
                    let true_at = at(point(t));
                    let chord = (a.0 + (b.0 - a.0) * t, a.1 + (b.1 - a.1) * t);
                    (true_at.0 - chord.0).abs().max((true_at.1 - chord.1).abs()) > TOLERANCE
                });
                let middle = point(0.5);
                let m = at(middle);
                if off {
                    refine(from, (middle, m), depth - 1, at, bounds, out);
                    refine((middle, m), to, depth - 1, at, bounds, out);
                    return;
                }
            }
            out.push((b.0.round() as i64, b.1.round() as i64));
        }
        let mut out: Vec<(i64, i64)> = Vec::with_capacity(path.len());
        let mut previous: Option<(Position, (f64, f64))> = None;
        for p in path {
            let here = (*p, at(*p));
            match previous {
                Some(from) => refine(from, here, DEPTH, &at, (low, high), &mut out),
                None => out.push((here.1.0.round() as i64, here.1.1.round() as i64)),
            }
            previous = Some(here);
        }
        out
    };
    // A polygon ring as a continuous path; one around a pole is closed
    // along the map's polar edge so its interior stays in.
    let closed = |ring: &Vec<Position>, around: f64| {
        let mut path = unwrapped(ring, around);
        if encloses_pole(std::slice::from_ref(ring))
            && let (Some(first), Some(last)) = (path.first().copied(), path.last().copied())
        {
            let pole = if polar_north(std::slice::from_ref(ring)) {
                90.0
            } else {
                -90.0
            };
            path.push(Position::new(last.lon, pole));
            path.push(Position::new(first.lon, pole));
        }
        path
    };
    const SHIFTS: [f64; 3] = [-360.0, 0.0, 360.0];
    // The full turns that bring some part of continuous `rings` next to
    // the tile: a path that winds round reaches it in more than one.
    let shifts = |rings: &[Vec<Position>]| {
        let lons = rings.iter().flatten().map(|p| p.lon);
        let (min, max) = lons.fold((f64::MAX, f64::MIN), |m, lon| (m.0.min(lon), m.1.max(lon)));
        let first = ((center - 180.0 - max) / 360.0).floor() as i64;
        let last = ((center + 180.0 - min) / 360.0).ceil() as i64;
        (first..=last)
            .map(|k| k as f64 * 360.0)
            .collect::<Vec<f64>>()
    };
    let mut commands = Commands::default();
    match drawn {
        Drawn::Point(p) => {
            let at = unwrapped(std::slice::from_ref(p), center)[0];
            let points: Vec<_> = SHIFTS
                .iter()
                .map(|shift| local(&at, *shift))
                .filter(|point| in_reach(std::slice::from_ref(point)))
                .collect();
            commands.points(&points);
            (1, commands.finish())
        }
        Drawn::Line(line) => {
            let line = unwrapped(line, center);
            let (low, high) = (-MARGIN, i64::from(EXTENT) + MARGIN);
            for shift in shifts(std::slice::from_ref(&line)) {
                let points = traced(&line, shift);
                for piece in clip_line(&points, low, high) {
                    commands.path(&piece, false);
                }
            }
            (2, commands.finish())
        }
        Drawn::Polygon(rings) => {
            // Holes are unwrapped around the middle of their exterior's
            // longitude span, as queries read them, so they stay inside it
            // in every copy.
            let exterior = rings.first().map(|ring| closed(ring, center));
            let around = exterior.as_ref().map_or(center, |ring| {
                let (min, max) = ring.iter().fold((f64::MAX, f64::MIN), |m, p| {
                    (m.0.min(p.lon), m.1.max(p.lon))
                });
                (min + max) / 2.0
            });
            let rings: Vec<Vec<Position>> = exterior
                .into_iter()
                .chain(rings.iter().skip(1).map(|ring| closed(ring, around)))
                .collect();
            for shift in shifts(&rings) {
                // Whole, so readers clip it with its topology intact. A ring
                // spans at most a world, so its coordinates fit the format.
                let mut copy = rings.iter().map(|ring| traced(ring, shift));
                // Without its exterior a copy has nothing to draw.
                let Some(exterior) = copy.next().and_then(|points| ring(points, true)) else {
                    continue;
                };
                if !in_reach(&exterior) {
                    continue;
                }
                commands.path(&exterior, true);
                for hole in copy.filter_map(|points| ring(points, false)) {
                    commands.path(&hole, true);
                }
            }
            (3, commands.finish())
        }
        Drawn::Cover(holes) => {
            // The exterior is a square a world wide either side of the tile,
            // so every nearby copy of the left-out cap, drawn whole, is a
            // hole strictly inside it.
            let world_width = i64::from(1u32 << z) * i64::from(EXTENT);
            let (outer_low, outer_high) = (-world_width - MARGIN, world_width + MARGIN);
            let (low, high) = (-MARGIN, i64::from(EXTENT) + MARGIN);
            let tile = vec![(low, low), (high, low), (high, high), (low, high)];
            let full = polygon_area(&tile).abs();
            let mut cut = Vec::new();
            for hole in holes.iter().map(|ring| closed(ring, center)) {
                for shift in shifts(std::slice::from_ref(&hole)) {
                    let points = traced(&hole, shift);
                    if !in_reach(&points) {
                        continue;
                    }
                    // The tile lies wholly in the cap: nothing to draw.
                    if polygon_area(&clip(&points, low, high)).abs() >= full {
                        return (3, Vec::new());
                    }
                    if points.iter().all(|p| {
                        outer_low < p.0 && p.0 < outer_high && outer_low < p.1 && p.1 < outer_high
                    }) {
                        cut.extend(ring(points, false));
                    }
                }
            }
            let outer = vec![
                (outer_low, outer_low),
                (outer_high, outer_low),
                (outer_high, outer_high),
                (outer_low, outer_high),
            ];
            if let Some(exterior) = ring(outer, true) {
                commands.path(&exterior, true);
                for hole in &cut {
                    commands.path(hole, true);
                }
            }
            (3, commands.finish())
        }
    }
}

/// One layer being built: features with interned keys and values.
#[derive(Default)]
struct LayerBuilder {
    features: Vec<mvt::Feature>,
    keys: Vec<String>,
    values: Vec<mvt::Value>,
    key_index: HashMap<String, u32>,
    value_index: HashMap<ValueKey, u32>,
}

/// A value as a hashable key (floats by their bits).
#[derive(PartialEq, Eq, Hash)]
enum ValueKey {
    Text(String),
    Int(i64),
    Float(u64),
    Bool(bool),
}

impl ValueKey {
    fn of(value: &mvt::Value) -> Self {
        match value {
            mvt::Value {
                string_value: Some(v),
                ..
            } => Self::Text(v.clone()),
            mvt::Value {
                sint_value: Some(v),
                ..
            } => Self::Int(*v),
            mvt::Value {
                double_value: Some(v),
                ..
            } => Self::Float(v.to_bits()),
            mvt::Value { bool_value, .. } => Self::Bool(bool_value.unwrap_or_default()),
        }
    }
}

impl LayerBuilder {
    fn key(&mut self, key: &str) -> u32 {
        if let Some(i) = self.key_index.get(key) {
            return *i;
        }
        let i = self.keys.len() as u32;
        self.keys.push(key.to_string());
        self.key_index.insert(key.to_string(), i);
        i
    }

    fn value(&mut self, value: mvt::Value) -> u32 {
        let key = ValueKey::of(&value);
        if let Some(i) = self.value_index.get(&key) {
            return *i;
        }
        let i = self.values.len() as u32;
        self.values.push(value);
        self.value_index.insert(key, i);
        i
    }

    fn add(&mut self, kind: i32, geometry: Vec<u32>, properties: &[(String, mvt::Value)]) {
        if geometry.is_empty() {
            return;
        }
        let mut tags = Vec::new();
        for (key, value) in properties {
            let key = self.key(key);
            let value = self.value(value.clone());
            tags.extend([key, value]);
        }
        self.features.push(mvt::Feature {
            id: None,
            tags,
            r#type: Some(kind),
            geometry,
        });
    }

    fn finish(self, name: &str) -> Option<mvt::Layer> {
        (!self.features.is_empty()).then(|| mvt::Layer {
            version: 2,
            name: name.to_string(),
            features: self.features,
            keys: self.keys,
            values: self.values,
            extent: Some(EXTENT),
        })
    }
}

fn text(value: &str) -> mvt::Value {
    mvt::Value {
        string_value: Some(value.to_string()),
        ..Default::default()
    }
}

fn label_list(labels: &Labels) -> mvt::Value {
    // Labels hold no commas, so the list reads back losslessly.
    text(&labels.iter().cloned().collect::<Vec<_>>().join(","))
}

fn entity_properties(entity: &Entity) -> Vec<(String, mvt::Value)> {
    let mut properties = vec![
        ("collection".to_string(), text(&entity.collection)),
        ("id".to_string(), text(&entity.id)),
        (
            "ts_ms".to_string(),
            mvt::Value {
                sint_value: Some(entity.ts_ms),
                ..Default::default()
            },
        ),
    ];
    if !entity.labels.is_empty() {
        properties.push(("labels".to_string(), label_list(&entity.labels)));
    }
    for (name, value) in &entity.fields {
        let value = match value {
            FieldValue::Bool(v) => mvt::Value {
                bool_value: Some(*v),
                ..Default::default()
            },
            FieldValue::Int(v) => mvt::Value {
                sint_value: Some(*v),
                ..Default::default()
            },
            FieldValue::Float(v) => mvt::Value {
                double_value: Some(*v),
                ..Default::default()
            },
            FieldValue::Text(v) => text(v),
        };
        properties.push((format!("field.{name}"), value));
    }
    properties
}

fn fence_properties(fence: &Fence) -> Vec<(String, mvt::Value)> {
    let mut properties = vec![("id".to_string(), text(&fence.id))];
    if !fence.labels.is_empty() {
        properties.push(("labels".to_string(), label_list(&fence.labels)));
    }
    properties
}

fn invalid(reason: &'static str) -> DurableError {
    DurableError::Index(IndexError::InvalidQuery(reason))
}

/// Every entity of `ns` in `region` that `readable` may see, visiting each
/// collection once and refused once the entities pass the source budget,
/// before more are cloned.
fn entities(
    engine: &DurableEngine,
    ns: &Namespace,
    region: [f64; 4],
    readable: Option<&Labels>,
    now_ms: i64,
) -> Result<Vec<Entity>, DurableError> {
    let mut found = Vec::new();
    let (mut vertices, mut bytes) = (0u64, 0u64);
    for collection in engine.collections(ns)? {
        let complete =
            engine.visit_region(ns, &collection, region, readable, now_ms, &mut |entity| {
                vertices += entity.geometry.vertex_count() as u64;
                // What the entity takes in memory, roughly.
                bytes += 128
                    + entity.geometry.vertex_count() as u64 * 16
                    + entity
                        .fields
                        .iter()
                        .map(|(name, value)| {
                            name.len() as u64
                                + 32
                                + match value {
                                    FieldValue::Text(text) => text.len() as u64,
                                    _ => 8,
                                }
                        })
                        .sum::<u64>()
                    + entity
                        .labels
                        .iter()
                        .map(|label| label.len() as u64 + 24)
                        .sum::<u64>();
                if vertices > MAX_INPUT_VERTICES || bytes > MAX_SOURCE_BYTES {
                    return false;
                }
                found.push(entity.clone());
                true
            })?;
        if !complete {
            return Err(invalid(
                "the package holds too much geometry; narrow the region or zooms",
            ));
        }
    }
    Ok(found)
}

/// Render the package for `request` in `ns`: the PMTiles archive and its
/// manifest.
pub fn package(
    engine: &DurableEngine,
    ns: &Namespace,
    request: &PackageRequest,
    now_ms: i64,
) -> Result<(Vec<u8>, Manifest), DurableError> {
    let [west, south, east, north] = request.region;
    if !(west.is_finite() && east.is_finite() && south.is_finite() && north.is_finite())
        || !(-180.0..=180.0).contains(&west)
        || !(-180.0..=180.0).contains(&east)
        || west >= east
        || south >= north
        || south < -MAX_LAT
        || north > MAX_LAT
    {
        return Err(invalid(
            "a package region is west < east and south < north within the Mercator range",
        ));
    }
    if request.min_zoom > request.max_zoom || request.max_zoom > MAX_ZOOM {
        return Err(invalid(
            "package zooms run from min_zoom to max_zoom, at most 16",
        ));
    }
    let tiles: u64 = (request.min_zoom..=request.max_zoom)
        .map(|z| {
            let (x0, y0, x1, y1) = tile_range(
                world(Position::new(west, north), z),
                world(Position::new(east, south), z),
                z,
            );
            u64::from(x1 - x0 + 1) * u64::from(y1 - y0 + 1)
        })
        .sum();
    if tiles > MAX_TILES {
        return Err(invalid("the package would exceed 100000 tiles"));
    }
    // Read the LSN first: everything up to it is in what follows.
    let lsn = engine.last_lsn()?;
    let readable = request.readable.as_ref();
    let entities = entities(engine, ns, request.region, readable, now_ms)?;
    let area = BoundingBox {
        min_lon: west,
        min_lat: south,
        max_lon: east,
        max_lat: north,
    };
    // Only fences reaching the region are kept, counted against the same
    // input budget as entities while the pages come in.
    let mut fences = Vec::new();
    let mut source = entities
        .iter()
        .map(|entity| entity.geometry.vertex_count() as u64)
        .sum::<u64>();
    let mut after: Option<String> = None;
    loop {
        let page = engine.list_fences(ns, after.as_deref(), readable, MAX_LIMIT)?;
        let Some(last) = page.last() else {
            break;
        };
        after = Some(last.id.clone());
        for fence in page {
            if !fence.boxes().iter().any(|bbox| bbox.intersects(&area)) {
                continue;
            }
            source += match &fence.shape {
                Shape::Circle { .. } => 65,
                Shape::Polygon(rings) => rings.iter().map(|ring| ring.len() as u64).sum(),
            };
            if source > MAX_INPUT_VERTICES {
                return Err(invalid(
                    "the package holds too much geometry; narrow the region or zooms",
                ));
            }
            fences.push(fence);
        }
    }
    let fences: Vec<(Drawn, &Fence)> = fences
        .iter()
        .map(|fence| {
            let shape = match &fence.shape {
                Shape::Circle { center, meters } => circle_drawn(*center, *meters),
                Shape::Polygon(rings) => Drawn::Polygon(rings.clone()),
            };
            (shape, fence)
        })
        .collect();
    let entities: Vec<(Drawn, &Entity)> = entities
        .iter()
        .map(|entity| (drawn(&entity.geometry), entity))
        .collect();

    // Per tile, the entity and fence features that touch it.
    type Hits = (Vec<usize>, Vec<usize>);
    let mut grid: BTreeMap<(u8, u32, u32), Hits> = BTreeMap::new();
    let (mut placements, mut work, mut bytes) = (0u64, 0u64, 0u64);
    for z in request.min_zoom..=request.max_zoom {
        let bounds = tile_range(
            world(Position::new(west, north), z),
            world(Position::new(east, south), z),
            z,
        );
        // The bounding boxes are split at the antimeridian already, so a
        // crossing marks the tiles on both sides.
        let mut mark =
            |boxes: Vec<BoundingBox>, shape: &Drawn, properties: u64, index: usize, fence: bool| {
                let mut tiles = std::collections::BTreeSet::new();
                for bbox in boxes {
                    let (x0, y0, x1, y1) = tile_range(
                        world(Position::new(bbox.min_lon, bbox.max_lat), z),
                        world(Position::new(bbox.max_lon, bbox.min_lat), z),
                        z,
                    );
                    for x in x0.max(bounds.0)..=x1.min(bounds.2) {
                        for y in y0.max(bounds.1)..=y1.min(bounds.3) {
                            tiles.insert((x, y));
                        }
                    }
                }
                let placed = tiles.len() as u64;
                placements += placed;
                let drawn = vertices(shape) * copies(shape);
                work += placed * drawn;
                // What the tiles will hold, estimated before any is built:
                // two varints a vertex and the properties of every placement.
                bytes += placed * (drawn * 10 + properties);
                if placements > MAX_PLACEMENTS
                    || work > MAX_VERTEX_WORK
                    || bytes > MAX_ARCHIVE_BYTES as u64
                {
                    return Err(invalid(
                        "the package holds too much geometry; narrow the region or zooms",
                    ));
                }
                for (x, y) in tiles {
                    let hits = grid.entry((z, x, y)).or_default();
                    match fence {
                        false => hits.0.push(index),
                        true => hits.1.push(index),
                    }
                }
                Ok(())
            };
        for (index, (shape, entity)) in entities.iter().enumerate() {
            let properties = property_bytes(&entity_properties(entity));
            mark(entity.geometry.boxes(), shape, properties, index, false)?;
        }
        for (index, (shape, fence)) in fences.iter().enumerate() {
            let properties = property_bytes(&fence_properties(fence));
            mark(fence.boxes(), shape, properties, index, true)?;
        }
    }

    let mut ordered: Vec<(u64, TileCoord, Hits)> = grid
        .into_iter()
        .map(|((z, x, y), hits)| {
            let coord = TileCoord::new(z, x, y).map_err(|error| invalid_tile(&error))?;
            Ok((TileId::from(coord).value(), coord, hits))
        })
        .collect::<Result<_, DurableError>>()?;
    ordered.sort_by_key(|(id, _, _)| *id);

    let metadata = serde_json::json!({
        "name": "ezu",
        "format": "pbf",
        "vector_layers": [
            {"id": "entities", "fields": {"collection": "String", "id": "String", "ts_ms": "Number", "labels": "String"}},
            {"id": "fences", "fields": {"id": "String", "labels": "String"}},
        ],
    });
    let mut archive = std::io::Cursor::new(Vec::new());
    let mut writer = PmTilesWriter::new(TileType::Mvt)
        .min_zoom(request.min_zoom)
        .max_zoom(request.max_zoom)
        .bounds(west, south, east, north)
        .center_zoom(request.min_zoom)
        .center((west + east) / 2.0, (south + north) / 2.0)
        .metadata(metadata.to_string())
        .create(&mut archive)
        .map_err(|error| invalid_tile(&error))?;
    let mut written = 0usize;
    for (_, coord, (entity_hits, fence_hits)) in ordered {
        let (z, x, y) = (coord.z(), coord.x(), coord.y());
        let mut layers = Vec::new();
        let mut layer = LayerBuilder::default();
        for index in entity_hits {
            let (shape, entity) = &entities[index];
            let (kind, geometry) = encode(shape, z, x, y);
            layer.add(kind, geometry, &entity_properties(entity));
        }
        layers.extend(layer.finish("entities"));
        let mut layer = LayerBuilder::default();
        for index in fence_hits {
            let (shape, fence) = &fences[index];
            let (kind, geometry) = encode(shape, z, x, y);
            layer.add(kind, geometry, &fence_properties(fence));
        }
        layers.extend(layer.finish("fences"));
        if layers.is_empty() {
            continue;
        }
        let tile = mvt::Tile { layers }.encode_to_vec();
        written += tile.len();
        if written > MAX_ARCHIVE_BYTES {
            return Err(invalid("the package would exceed 256 MiB"));
        }
        writer
            .add_tile(coord, &tile)
            .map_err(|error| invalid_tile(&error))?;
    }
    writer.finalize().map_err(|error| invalid_tile(&error))?;
    let bytes = archive.into_inner();
    let manifest = Manifest {
        region: request.region,
        min_zoom: request.min_zoom,
        max_zoom: request.max_zoom,
        created_ms: now_ms,
        lsn,
        size: bytes.len() as u64,
        sha256: Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    };
    Ok((bytes, manifest))
}

fn invalid_tile(error: &dyn std::fmt::Display) -> DurableError {
    DurableError::Package(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn world_pixels_and_tile_ranges_follow_web_mercator() {
        let (x, y) = world(Position::new(0.0, 0.0), 0);
        assert!((x - 2048.0).abs() < 1e-9 && (y - 2048.0).abs() < 1e-9);
        let (x, y) = world(Position::new(-180.0, MAX_LAT), 1);
        assert!(x.abs() < 1e-9 && y.abs() < 1e-3);
        assert_eq!(
            tile_range(
                world(Position::new(-10.0, 10.0), 2),
                world(Position::new(10.0, -10.0), 2),
                2
            ),
            (1, 1, 2, 2)
        );
    }

    #[test]
    fn polygons_close_and_orient_exteriors_positively() {
        let square = Drawn::Polygon(vec![vec![
            Position::new(0.0, 0.0),
            Position::new(0.0, 1.0),
            Position::new(1.0, 1.0),
            Position::new(1.0, 0.0),
            Position::new(0.0, 0.0),
        ]]);
        let (kind, commands) = encode(&square, 8, 128, 127);
        assert_eq!(kind, 3);
        // MoveTo 1, LineTo, ClosePath.
        assert_eq!(commands[0], command(1, 1));
        // LineTo the rest; long edges near the tile are subdivided.
        assert_eq!(commands[3] & 7, 2);
        assert!(commands[3] >> 3 >= 3);
        assert_eq!(*commands.last().unwrap(), command(7, 1));
        assert_eq!(zigzag(-1), 1);
        assert_eq!(zigzag(1), 2);
        // A sliver that rounds to no area at this zoom draws nothing.
        let sliver = Drawn::Polygon(vec![vec![
            Position::new(0.0, 0.0),
            Position::new(0.000_001, 0.0),
            Position::new(0.000_002, 0.0),
            Position::new(0.0, 0.000_001),
            Position::new(0.0, 0.0),
        ]]);
        assert!(encode(&sliver, 2, 2, 1).1.is_empty());
    }

    #[test]
    fn polar_rings_and_huge_circles_keep_their_interiors() {
        // A ring at 80° north around the pole fills up to the map's edge.
        let polar = Drawn::Polygon(vec![vec![
            Position::new(-135.0, 80.0),
            Position::new(-45.0, 80.0),
            Position::new(45.0, 80.0),
            Position::new(135.0, 80.0),
            Position::new(-135.0, 80.0),
        ]]);
        let (kind, commands) = encode(&polar, 2, 1, 0);
        assert_eq!(kind, 3);
        assert!(!commands.is_empty());
        // A circle larger than a hemisphere covers the tile at its center
        // whole, and near its antipode leaves the cap out as a hole.
        let huge = circle_drawn(Position::new(0.0, 0.0), 15_000_000.0);
        let closes = |commands: &[u32]| commands.iter().filter(|c| **c == command(7, 1)).count();
        assert_eq!(closes(&encode(&huge, 3, 4, 4).1), 1);
        assert_eq!(closes(&encode(&huge, 3, 7, 3).1), 2);
    }

    /// The rings of polygon commands, in absolute tile coordinates.
    fn rings(commands: &[u32]) -> Vec<Vec<(i64, i64)>> {
        let unzig = |v: u32| i64::from(v >> 1) ^ -i64::from(v & 1);
        let (mut at, mut out, mut i) = ((0i64, 0i64), Vec::new(), 0);
        while i < commands.len() {
            let (id, count) = (commands[i] & 7, (commands[i] >> 3) as usize);
            i += 1;
            match id {
                1 | 2 => {
                    if id == 1 {
                        out.push(Vec::new());
                    }
                    for _ in 0..count {
                        at = (at.0 + unzig(commands[i]), at.1 + unzig(commands[i + 1]));
                        out.last_mut().unwrap().push(at);
                        i += 2;
                    }
                }
                _ => {}
            }
        }
        out
    }

    #[test]
    fn holes_stay_inside_their_exterior_across_the_seam_and_caps_are_clipped() {
        let crossing = Drawn::Polygon(vec![
            vec![
                Position::new(170.0, -10.0),
                Position::new(-170.0, -10.0),
                Position::new(-170.0, 10.0),
                Position::new(170.0, 10.0),
                Position::new(170.0, -10.0),
            ],
            vec![
                Position::new(-175.0, -5.0),
                Position::new(-175.0, 5.0),
                Position::new(175.0, 5.0),
                Position::new(175.0, -5.0),
                Position::new(-175.0, -5.0),
            ],
        ]);
        for x in [0, 3] {
            let drawn = rings(&encode(&crossing, 2, x, 1).1);
            let (exterior, hole) = (&drawn[0], &drawn[1]);
            let span = |ring: &Vec<(i64, i64)>| {
                (
                    ring.iter().map(|p| p.0).min().unwrap(),
                    ring.iter().map(|p| p.0).max().unwrap(),
                )
            };
            let (outer, inner) = (span(exterior), span(hole));
            // Clipped to the tile margin, both may touch its edge; the hole
            // never reaches past the exterior and is narrower.
            assert!(
                outer.0 <= inner.0 && inner.1 <= outer.1 && inner.1 - inner.0 < outer.1 - outer.0,
                "tile {x}: {outer:?} {inner:?}"
            );
        }
        // Near the antipode of a huge circle, the cap is a hole strictly
        // inside the exterior.
        let huge = circle_drawn(Position::new(0.0, 0.0), 15_000_000.0);
        let drawn = rings(&encode(&huge, 3, 7, 3).1);
        let (exterior, hole) = (&drawn[0], &drawn[1]);
        let (low, high) = (
            exterior.iter().map(|p| p.0).min().unwrap(),
            exterior.iter().map(|p| p.0).max().unwrap(),
        );
        assert!(
            hole.iter()
                .all(|p| low < p.0 && p.0 < high && low < p.1 && p.1 < high)
        );
    }

    #[test]
    fn a_line_winding_many_times_stays_within_the_tile_at_high_zoom() {
        let path: Vec<Position> = (0..40)
            .map(|i| Position::new(-170.0 + f64::from(i % 3) * 120.0, f64::from(i) * 0.001))
            .collect();
        let (_, commands) = encode(&Drawn::Line(path), 16, 0, 32_768);
        let (low, high) = (-MARGIN, i64::from(EXTENT) + MARGIN);
        for piece in rings(&commands) {
            assert!(
                piece
                    .iter()
                    .all(|p| (low..=high).contains(&p.0) && (low..=high).contains(&p.1)),
                "{piece:?}"
            );
        }
    }

    #[test]
    fn a_long_edge_is_drawn_where_the_index_has_it() {
        // From (-10, 0) to (10, 80) the edge passes (0, 40). In the tile
        // holding that point at zoom 6 the drawn line must come close.
        let line = Drawn::Line(vec![Position::new(-10.0, 0.0), Position::new(10.0, 80.0)]);
        for (z, on) in [
            (6, Position::new(0.0, 40.0)),
            (16, Position::new(7.656_25, 70.625)),
        ] {
            let (wx, wy) = world(on, z);
            let (x, y) = ((wx / 4096.0) as u32, (wy / 4096.0) as u32);
            let (target, _) = ((wx - f64::from(x * EXTENT), wy - f64::from(y * EXTENT)), ());
            let drawn = rings(&encode(&line, z, x, y).1);
            let nearest = drawn
                .iter()
                .flatten()
                .map(|p| ((p.0 as f64 - target.0).powi(2) + (p.1 as f64 - target.1).powi(2)).sqrt())
                .fold(f64::MAX, f64::min);
            assert!(nearest < 600.0, "zoom {z}: {nearest}");
        }
    }

    #[test]
    fn an_edge_bent_symmetrically_across_the_equator_is_still_traced() {
        // From (-10, -80) to (10, 80) the edge passes (5, 40); its Mercator
        // midpoint lies on the chord, the quarter points do not.
        let line = Drawn::Line(vec![Position::new(-10.0, -80.0), Position::new(10.0, 80.0)]);
        let z = 8;
        let (wx, wy) = world(Position::new(5.0, 40.0), z);
        let (x, y) = ((wx / 4096.0) as u32, (wy / 4096.0) as u32);
        let target = (wx - f64::from(x * EXTENT), wy - f64::from(y * EXTENT));
        let nearest = rings(&encode(&line, z, x, y).1)
            .iter()
            .flatten()
            .map(|p| ((p.0 as f64 - target.0).powi(2) + (p.1 as f64 - target.1).powi(2)).sqrt())
            .fold(f64::MAX, f64::min);
        assert!(nearest < 600.0, "{nearest}");
    }

    #[test]
    fn a_ring_too_wide_for_the_format_is_left_out_not_corrupted() {
        let mut ring: Vec<Position> = (0..28)
            .map(|i| Position::new([-170.0, -50.0, 70.0][i % 3], 80.0))
            .collect();
        ring.push(ring[0]);
        let (_, commands) = encode(&Drawn::Polygon(vec![ring]), 16, 0, 0);
        assert!(commands.is_empty());
    }

    #[test]
    fn a_path_winding_round_twice_keeps_winding() {
        let lons = [-170.0, -50.0, 70.0, -170.0, -50.0, 70.0, -170.0];
        let path: Vec<Position> = lons.iter().map(|lon| Position::new(*lon, 0.0)).collect();
        let out: Vec<f64> = unwrapped(&path, -170.0).iter().map(|p| p.lon).collect();
        assert_eq!(out, [-170.0, -50.0, 70.0, 190.0, 310.0, 430.0, 550.0]);
    }

    #[test]
    fn a_line_across_the_antimeridian_is_drawn_the_short_way_on_both_sides() {
        let line = Drawn::Line(vec![Position::new(179.0, 0.0), Position::new(-179.0, 0.0)]);
        // In the easternmost and the westernmost tile at zoom 2 the line is
        // two degrees long: about 91 pixels, not a span of the whole world.
        // At zoom 0 one tile spans the seam: the line shows at both edges.
        let (_, commands) = encode(&line, 0, 0, 0);
        let moves = commands.iter().filter(|c| **c == command(1, 1)).count();
        assert_eq!(moves, 2);
        for x in [3, 0] {
            let (_, commands) = encode(&line, 2, x, 2);
            let dx = (commands[4] >> 1) as i64 ^ -((commands[4] & 1) as i64);
            assert!((80..100).contains(&dx), "tile {x}: {dx}");
        }
    }
}
