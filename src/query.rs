//! Spatial queries over live entities: bounding box, geodesic radius,
//! polygon contains and intersects, and k-nearest, with typed field filters,
//! cursor paging and result caps.
//!
//! Planar tests run in longitude/latitude degrees with longitudes unwrapped
//! next to the query, so shapes across the antimeridian compare correctly.
//! Distances are great-circle metres. Edges run straight in longitude and
//! latitude; the nearest point on an edge is found numerically.

use crate::entity::{Entity, FieldValue};
use crate::geometry::{
    BoundingBox, Geometry, Position, encloses_pole, polar_north, query_boxes, short_step,
};

/// Mean Earth radius in metres.
pub const EARTH_RADIUS_M: f64 = 6_371_008.8;
/// Largest page a query returns.
pub const MAX_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relation {
    /// The entity lies entirely inside the query polygon.
    Contains,
    /// The entity and the query polygon share at least one point.
    Intersects,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    /// `[west, south, east, north]`; `west > east` wraps the antimeridian.
    Bbox([f64; 4]),
    Radius {
        center: Position,
        meters: f64,
    },
    /// Exterior ring first, then holes; rings are closed.
    Polygon {
        rings: Vec<Vec<Position>>,
        relation: Relation,
    },
    Nearest {
        center: Position,
        k: usize,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    Eq(String, FieldValue),
    /// Inclusive numeric range over an `Int` or `Float` field.
    Range {
        field: String,
        min: Option<f64>,
        max: Option<f64>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    pub collection: String,
    pub query: Query,
    pub filters: Vec<Filter>,
    /// Page size, at most [`MAX_LIMIT`]. For `Nearest`, at most `k`.
    pub limit: usize,
    /// The `next_cursor` of the previous page.
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryPage {
    /// In id order, or by distance then id for `Nearest`.
    pub entities: Vec<Entity>,
    pub next_cursor: Option<String>,
}

impl Filter {
    pub fn matches(&self, entity: &Entity) -> bool {
        match self {
            Self::Eq(field, value) => entity.fields.get(field) == Some(value),
            Self::Range { field, min, max } => {
                use std::cmp::Ordering;
                let compare = |bound: f64| -> Option<Ordering> {
                    match entity.fields.get(field) {
                        Some(FieldValue::Int(v)) => int_cmp_float(*v, bound),
                        Some(FieldValue::Float(v)) => v.partial_cmp(&bound),
                        _ => None,
                    }
                };
                matches!(
                    entity.fields.get(field),
                    Some(FieldValue::Int(_) | FieldValue::Float(_))
                ) && min.is_none_or(|min| compare(min).is_some_and(Ordering::is_ge))
                    && max.is_none_or(|max| compare(max).is_some_and(Ordering::is_le))
            }
        }
    }
}

/// Exact order of an integer against a float, without rounding the integer.
fn int_cmp_float(value: i64, bound: f64) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if bound.is_nan() {
        return None;
    }
    // i64 spans [-2^63, 2^63); both ends are exact in f64.
    if bound >= 9_223_372_036_854_775_808.0 {
        return Some(Ordering::Less);
    }
    if bound < -9_223_372_036_854_775_808.0 {
        return Some(Ordering::Greater);
    }
    let floor = bound.floor();
    Some(match value.cmp(&(floor as i64)) {
        Ordering::Equal if bound > floor => Ordering::Less,
        order => order,
    })
}

/// Validate a query; `None` when it is well formed.
pub(crate) fn invalid(query: &Query) -> Option<&'static str> {
    let position_ok = |p: &Position| {
        p.lon.is_finite()
            && p.lat.is_finite()
            && (-180.0..=180.0).contains(&p.lon)
            && (-90.0..=90.0).contains(&p.lat)
    };
    match query {
        Query::Bbox([west, south, east, north]) => (![west, east]
            .iter()
            .all(|x| x.is_finite() && (-180.0..=180.0).contains(*x))
            || ![south, north]
                .iter()
                .all(|y| y.is_finite() && (-90.0..=90.0).contains(*y))
            || south > north)
            .then_some("bbox needs lon in [-180, 180], lat in [-90, 90] and south <= north"),
        Query::Radius { center, meters } => {
            (!position_ok(center) || !meters.is_finite() || *meters < 0.0)
                .then_some("radius needs a valid center and a finite, non-negative distance")
        }
        Query::Nearest { center, k } => (!position_ok(center) || *k == 0)
            .then_some("nearest needs a valid center and k of at least 1"),
        Query::Polygon { rings, .. } => {
            let polygon = Geometry::Polygon(rings.clone());
            if polygon.validate().is_err() {
                return Some("query polygon is not a valid closed polygon");
            }
            if encloses_pole(rings) {
                return Some("query polygon may not enclose a pole");
            }
            None
        }
    }
}

/// Great-circle distance in metres.
pub fn haversine(a: Position, b: Position) -> f64 {
    let (lat1, lat2) = (a.lat.to_radians(), b.lat.to_radians());
    let dlat = lat2 - lat1;
    // The short way round, so -180 and 180 are the same meridian.
    let dlon = near(b.lon - a.lon, 0.0).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * h.sqrt().min(1.0).asin()
}

/// `lon` shifted by a whole turn to lie within 180° of `reference`.
fn near(lon: f64, reference: f64) -> f64 {
    let mut lon = lon;
    while lon - reference > 180.0 {
        lon -= 360.0;
    }
    while reference - lon > 180.0 {
        lon += 360.0;
    }
    lon
}

/// A ring unwrapped from its first vertex (placed next to `reference`),
/// each step taken as the index bounds take it, so every edge keeps its
/// direction from the original frame.
fn unwrap_ring(ring: &[Position], reference: f64) -> Vec<(f64, f64)> {
    let Some(first) = ring.first() else {
        return Vec::new();
    };
    let mut lon = near(first.lon, reference);
    let mut unwrapped = vec![(lon, first.lat)];
    for pair in ring.windows(2) {
        lon = follow(lon, pair[0].lon, pair[1].lon);
        unwrapped.push((lon, pair[1].lat));
    }
    unwrapped
}

/// The next vertex's longitude after one at unwrapped `from` (original
/// `from_lon`): its original value plus whole turns, never an accumulated
/// sum, so a vertex compares exactly with a point shifted by turns.
fn follow(from: f64, from_lon: f64, to_lon: f64) -> f64 {
    let target = from + short_step(from_lon, to_lon);
    to_lon + ((target - to_lon) / 360.0).round() * 360.0
}

/// A query polygon in one continuous longitude frame.
struct Frame {
    rings: Vec<Vec<(f64, f64)>>,
    reference: f64,
}

impl Frame {
    fn new(rings: &[Vec<Position>]) -> Self {
        let reference = rings
            .first()
            .and_then(|ring| ring.first())
            .map_or(0.0, |p| p.lon);
        // Holes are unwrapped next to the middle of the exterior, so they sit
        // inside its continuous longitude span.
        let exterior = rings
            .first()
            .map(|ring| unwrap_ring(ring, reference))
            .unwrap_or_default();
        let (lo, hi) = exterior
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), (x, _)| {
                (lo.min(*x), hi.max(*x))
            });
        let middle = if lo.is_finite() {
            (lo + hi) / 2.0
        } else {
            reference
        };
        let mut unwrapped = vec![exterior];
        unwrapped.extend(rings.iter().skip(1).map(|ring| unwrap_ring(ring, middle)));
        Self {
            rings: unwrapped,
            reference,
        }
    }

    /// Even-odd point in polygon; the point is tried a turn either side so
    /// the frame's wrap does not matter.
    fn contains(&self, p: Position) -> bool {
        // A pole has every longitude: a planar polygon reaches it only
        // through a boundary point there.
        if p.lat.abs() == 90.0 {
            return self.edges().any(|(r, s)| r.1 == p.lat || s.1 == p.lat);
        }
        let base = near(p.lon, self.reference);
        [base, base - 360.0, base + 360.0]
            .into_iter()
            .any(|lon| self.contains_at(lon, p.lat))
    }

    fn contains_at(&self, x: f64, y: f64) -> bool {
        // A point on the boundary shares a point with the polygon.
        if self
            .edges()
            .any(|(r, s)| orient(r, s, (x, y)) == 0.0 && on_segment(r, s, (x, y)))
        {
            return true;
        }
        let mut inside = false;
        for ring in &self.rings {
            for edge in ring.windows(2) {
                let ((x1, y1), (x2, y2)) = (edge[0], edge[1]);
                if (y1 > y) != (y2 > y) && x < x1 + (y - y1) * (x2 - x1) / (y2 - y1) {
                    inside = !inside;
                }
            }
        }
        inside
    }

    fn edges(&self) -> impl Iterator<Item = ((f64, f64), (f64, f64))> + '_ {
        self.rings
            .iter()
            .flat_map(|ring| ring.windows(2).map(|edge| (edge[0], edge[1])))
    }

    /// Whether segment `a`-`b` leaves the polygon. The segment is cut at
    /// every point where it meets the boundary, and each piece's middle must
    /// lie inside (boundary included), so touching stays inside while
    /// passing through a hole at its vertices does not.
    fn leaves(&self, a: Position, b: Position) -> bool {
        let a_lon = near(a.lon, self.reference);
        let b_lon = follow(a_lon, a.lon, b.lon);
        let mut cuts = vec![0.0, 1.0];
        for shift in [0.0, -360.0, 360.0] {
            let (p, q) = ((a_lon + shift, a.lat), (b_lon + shift, b.lat));
            for (r, s) in self.edges() {
                cuts.extend(meeting_params(p, q, r, s));
            }
        }
        cuts.sort_by(f64::total_cmp);
        cuts.windows(2).filter(|w| w[1] > w[0]).any(|w| {
            let t = (w[0] + w[1]) / 2.0;
            let lon = a_lon + t * (b_lon - a_lon);
            !self.contains(Position::new(near(lon, 0.0), a.lat + t * (b.lat - a.lat)))
        })
    }

    /// Strictly inside: inside by even-odd and not on any edge.
    fn strictly_contains_at(&self, x: f64, y: f64) -> bool {
        if self
            .edges()
            .any(|(r, s)| orient(r, s, (x, y)) == 0.0 && on_segment(r, s, (x, y)))
        {
            return false;
        }
        self.contains_at(x, y)
    }

    /// Whether segment `a`-`b` meets any polygon edge.
    fn crosses(&self, a: Position, b: Position) -> bool {
        let a_lon = near(a.lon, self.reference);
        let b_lon = follow(a_lon, a.lon, b.lon);
        [0.0, -360.0, 360.0].into_iter().any(|shift| {
            let (p, q) = ((a_lon + shift, a.lat), (b_lon + shift, b.lat));
            self.edges().any(|(r, s)| segments_meet(p, q, r, s))
        })
    }
}

/// The area of a stored polygon: planar, or a cap around a pole whose
/// exterior ring circles it. A cap holds a point on the pole side of the
/// ring at that point's longitude.
enum Region {
    Planar(Frame),
    Cap {
        ring: Vec<(f64, f64)>,
        north: bool,
        holes: Vec<Region>,
    },
}

impl Region {
    /// Inside a hole but not on its edge: the hole's boundary belongs to
    /// the polygon.
    fn strictly_contains(&self, p: Position) -> bool {
        match self {
            Self::Planar(frame) => {
                let base = near(p.lon, frame.reference);
                p.lat.abs() != 90.0
                    && [base, base - 360.0, base + 360.0]
                        .into_iter()
                        .any(|x| frame.strictly_contains_at(x, p.lat))
            }
            Self::Cap { ring, .. } => {
                let edge = Frame {
                    rings: vec![ring.clone()],
                    reference: ring.first().map_or(0.0, |v| v.0),
                };
                self.contains(p) && !on_query_segment(&edge, p)
            }
        }
    }

    fn of(rings: &[Vec<Position>]) -> Self {
        if !encloses_pole(rings) {
            return Self::Planar(Frame::new(rings));
        }
        let exterior = rings.first().map(Vec::as_slice).unwrap_or_default();
        Self::Cap {
            ring: unwrap_ring(exterior, exterior.first().map_or(0.0, |p| p.lon)),
            north: polar_north(rings),
            // A hole may itself circle the pole (an annulus); each hole is
            // read with the semantics of its own shape.
            holes: rings
                .iter()
                .skip(1)
                .map(|hole| Region::of(std::slice::from_ref(hole)))
                .collect(),
        }
    }

    fn contains(&self, p: Position) -> bool {
        match self {
            Self::Planar(frame) => frame.contains(p),
            Self::Cap { ring, north, holes } => {
                if holes.iter().any(|hole| hole.strictly_contains(p)) {
                    return false;
                }
                if p.lat.abs() == 90.0 {
                    return (p.lat > 0.0) == *north;
                }
                // Walk the meridian from the point to the pole, which is
                // inside: every boundary crossing flips the side, so an even
                // count leaves the point inside. Half-open edge spans count a
                // vertex once.
                let mut crossings = 0;
                for edge in ring.windows(2) {
                    let ((x1, y1), (x2, y2)) = (edge[0], edge[1]);
                    for turn in -2..=2 {
                        let x = p.lon + 360.0 * f64::from(turn);
                        if (x1 == x2 && x == x1) || (x1 <= x && x < x2) || (x2 <= x && x < x1) {
                            let at = if x1 == x2 {
                                if (y1.min(y2)..=y1.max(y2)).contains(&p.lat) {
                                    return true;
                                }
                                continue;
                            } else {
                                y1 + (x - x1) * (y2 - y1) / (x2 - x1)
                            };
                            if at == p.lat {
                                return true;
                            }
                            if (*north && at > p.lat) || (!*north && at < p.lat) {
                                crossings += 1;
                            }
                        }
                    }
                }
                crossings % 2 == 0
            }
        }
    }
}

fn orient(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
    (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
}

fn on_segment(a: (f64, f64), b: (f64, f64), p: (f64, f64)) -> bool {
    p.0 >= a.0.min(b.0) && p.0 <= a.0.max(b.0) && p.1 >= a.1.min(b.1) && p.1 <= a.1.max(b.1)
}

/// Parameters `t` along `p`-`q` where it meets edge `r`-`s`: one for a
/// crossing or touch, the edge's ends for a collinear overlap.
fn meeting_params(p: (f64, f64), q: (f64, f64), r: (f64, f64), s: (f64, f64)) -> Vec<f64> {
    let cross = |u: (f64, f64), v: (f64, f64)| u.0 * v.1 - u.1 * v.0;
    let (d, e, w) = (
        (q.0 - p.0, q.1 - p.1),
        (s.0 - r.0, s.1 - r.1),
        (r.0 - p.0, r.1 - p.1),
    );
    let denom = cross(d, e);
    if denom != 0.0 {
        let (t, u) = (cross(w, e) / denom, cross(w, d) / denom);
        return if (0.0..=1.0).contains(&t) && (0.0..=1.0).contains(&u) {
            vec![t]
        } else {
            Vec::new()
        };
    }
    let length = d.0 * d.0 + d.1 * d.1;
    if cross(w, d) != 0.0 || length == 0.0 {
        return Vec::new();
    }
    [r, s]
        .into_iter()
        .map(|end| ((end.0 - p.0) * d.0 + (end.1 - p.1) * d.1) / length)
        .filter(|t| (0.0..=1.0).contains(t))
        .collect()
}

fn segments_meet(p: (f64, f64), q: (f64, f64), r: (f64, f64), s: (f64, f64)) -> bool {
    let (d1, d2) = (orient(r, s, p), orient(r, s, q));
    let (d3, d4) = (orient(p, q, r), orient(p, q, s));
    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }
    (d1 == 0.0 && on_segment(r, s, p))
        || (d2 == 0.0 && on_segment(r, s, q))
        || (d3 == 0.0 && on_segment(p, q, r))
        || (d4 == 0.0 && on_segment(p, q, s))
}

fn vertices(geometry: &Geometry) -> Vec<Position> {
    match geometry {
        Geometry::Point(p) => vec![*p],
        Geometry::Line(positions) => positions.clone(),
        Geometry::Polygon(rings) => rings.iter().flatten().copied().collect(),
    }
}

fn segments(geometry: &Geometry) -> Vec<(Position, Position)> {
    let pairs = |ring: &[Position]| -> Vec<(Position, Position)> {
        ring.windows(2).map(|pair| (pair[0], pair[1])).collect()
    };
    match geometry {
        Geometry::Point(_) => Vec::new(),
        Geometry::Line(positions) => pairs(positions),
        Geometry::Polygon(rings) => rings.iter().flat_map(|ring| pairs(ring)).collect(),
    }
}

/// How `geometry` relates to the query polygon.
pub fn relates(geometry: &Geometry, rings: &[Vec<Position>], relation: Relation) -> bool {
    let frame = Frame::new(rings);
    let points = vertices(geometry);
    let crossing = || {
        segments(geometry)
            .into_iter()
            .any(|(a, b)| frame.crosses(a, b))
    };
    let own = match geometry {
        Geometry::Polygon(own) => Some(Region::of(own)),
        _ => None,
    };
    match relation {
        // A query hole inside the entity polygon is area the query excludes.
        // A cap that still holds its pole is inside only a query that holds
        // that pole too; an annulus whose hole removes the pole is judged by
        // its boundary like any polygon.
        Relation::Contains
            if own.as_ref().is_some_and(|own| match own {
                Region::Cap { north, .. } => {
                    let pole = Position::new(0.0, if *north { 90.0 } else { -90.0 });
                    own.contains(pole) && !frame.contains(pole)
                }
                Region::Planar(_) => false,
            }) =>
        {
            false
        }
        Relation::Contains => {
            points.iter().all(|p| frame.contains(*p))
                && !segments(geometry)
                    .into_iter()
                    .any(|(a, b)| frame.leaves(a, b))
                && !own.as_ref().is_some_and(|own| {
                    rings
                        .iter()
                        .skip(1)
                        .filter_map(|hole| interior_point(hole))
                        .any(|p| own.contains(p))
                })
        }
        Relation::Intersects => {
            crossing()
                || points.iter().any(|p| frame.contains(*p))
                || own
                    .as_ref()
                    .is_some_and(|own| rings.iter().flatten().any(|p| own.contains(*p)))
        }
    }
}

/// A point strictly inside `ring`: the centre of a corner triangle that
/// lies inside it. Every simple ring has such a corner.
fn interior_point(ring: &[Position]) -> Option<Position> {
    let frame = Frame::new(&[ring.to_vec()]);
    let unwrapped = frame.rings.first()?;
    unwrapped.windows(3).find_map(|corner| {
        let x = (corner[0].0 + corner[1].0 + corner[2].0) / 3.0;
        let y = (corner[0].1 + corner[1].1 + corner[2].1) / 3.0;
        frame
            .strictly_contains_at(x, y)
            .then(|| Position::new(near(x, 0.0), y))
    })
}

/// Whether `geometry` shares a point with the segment `a`-`b` (a point when
/// `a == b`).
fn meets_segment(geometry: &Geometry, a: Position, b: Position) -> bool {
    let segment = Frame {
        rings: vec![unwrap_ring(&[a, b], a.lon)],
        reference: a.lon,
    };
    vertices(geometry)
        .iter()
        .any(|p| on_query_segment(&segment, *p))
        || segments(geometry)
            .into_iter()
            .any(|(p, q)| segment.crosses(p, q))
        || matches!(geometry, Geometry::Polygon(own) if Region::of(own).contains(a))
}

fn on_query_segment(frame: &Frame, p: Position) -> bool {
    let base = near(p.lon, frame.reference);
    [base, base - 360.0, base + 360.0].into_iter().any(|x| {
        frame
            .edges()
            .any(|(r, s)| orient(r, s, (x, p.lat)) == 0.0 && on_segment(r, s, (x, p.lat)))
    })
}

/// Distance in metres from `center` to the nearest point of `geometry`; 0
/// inside a polygon.
pub fn distance(geometry: &Geometry, center: Position) -> f64 {
    if let Geometry::Polygon(rings) = geometry
        && Region::of(rings).contains(center)
    {
        return 0.0;
    }
    // On a vertex or an edge is exactly zero, before any numeric search.
    let same = |v: &Position| {
        v.lat == center.lat && (v.lat.abs() == 90.0 || near(v.lon - center.lon, 0.0) == 0.0)
    };
    if vertices(geometry).iter().any(same)
        || segments(geometry).into_iter().any(|(a, b)| {
            let edge = Frame {
                rings: vec![unwrap_ring(&[a, b], a.lon)],
                reference: a.lon,
            };
            on_query_segment(&edge, center)
        })
    {
        return 0.0;
    }
    let mut best = vertices(geometry)
        .into_iter()
        .map(|p| haversine(center, p))
        .fold(f64::INFINITY, f64::min);
    // Nearest point on each edge, which runs straight in longitude and
    // latitude: sampled along the edge, then refined by golden-section
    // search around the best sample. Numeric, to well under a metre for
    // edges of a few thousand kilometres.
    for (a, b) in segments(geometry) {
        let a_lon = near(a.lon, center.lon);
        let b_lon = follow(a_lon, a.lon, b.lon);
        let at = |t: f64| {
            let point = Position::new(
                near(a_lon + t * (b_lon - a_lon), 0.0),
                a.lat + t * (b.lat - a.lat),
            );
            haversine(center, point)
        };
        const SAMPLES: u32 = 64;
        let step = 1.0 / f64::from(SAMPLES);
        let (mut lo, mut hi, mut low) = (0.0, step, f64::INFINITY);
        for i in 0..=SAMPLES {
            let t = f64::from(i) * step;
            let d = at(t);
            if d < low {
                low = d;
                (lo, hi) = ((t - step).max(0.0), (t + step).min(1.0));
            }
        }
        const GOLDEN: f64 = 0.618_033_988_749_895;
        for _ in 0..48 {
            let (c, d) = (hi - GOLDEN * (hi - lo), lo + GOLDEN * (hi - lo));
            if at(c) < at(d) {
                hi = d;
            } else {
                lo = c;
            }
        }
        best = best.min(low).min(at((lo + hi) / 2.0));
    }
    best
}

/// Whether an entity's geometry satisfies `query`. `Nearest` matches all;
/// its order comes from [`distance`].
pub fn matches(query: &Query, geometry: &Geometry) -> bool {
    match query {
        Query::Bbox([west, south, east, north]) => {
            // Every longitude meets at a pole: a vertex there is inside a box
            // that reaches it.
            if vertices(geometry)
                .iter()
                .any(|v| (v.lat == 90.0 && *north == 90.0) || (v.lat == -90.0 && *south == -90.0))
            {
                return true;
            }
            let rings = bbox_rings(*west, *south, *east, *north);
            match rings {
                Some(rings) => relates(geometry, &rings, Relation::Intersects),
                // A point or a line of a box: test it as a segment.
                None if *west == *east || *south == *north => {
                    if *west == *east {
                        return meets_segment(
                            geometry,
                            Position::new(*west, *south),
                            Position::new(*east, *north),
                        );
                    }
                    // Walk eastward in pieces of at most 90°, so no piece is
                    // read the short way round the other side.
                    let end = if *west > *east { *east + 360.0 } else { *east };
                    if end == *west {
                        // [180, s, -180, n]: -180 and 180 are one meridian.
                        return meets_segment(
                            geometry,
                            Position::new(*west, *south),
                            Position::new(*west, *north),
                        );
                    }
                    let mut from = *west;
                    while from < end {
                        let to = (from + 90.0).min(end);
                        if meets_segment(
                            geometry,
                            Position::new(near(from, 0.0), *south),
                            Position::new(near(to, 0.0), *north),
                        ) {
                            return true;
                        }
                        from = to;
                    }
                    false
                }
                // A full-longitude band: test its two halves exactly.
                None => [(*west, *west + 180.0), (*west + 180.0, *west + 360.0)]
                    .into_iter()
                    .filter_map(|(w, e)| bbox_rings(w, *south, e, *north))
                    .any(|rings| relates(geometry, &rings, Relation::Intersects)),
            }
        }
        Query::Radius { center, meters } => distance(geometry, *center) <= *meters,
        Query::Polygon { rings, relation } => relates(geometry, rings, *relation),
        Query::Nearest { .. } => true,
    }
}

/// The bbox as a polygon, or `None` when it is degenerate (a point or a
/// line), where box overlap is the exact test.
fn bbox_rings(west: f64, south: f64, east: f64, north: f64) -> Option<Vec<Vec<Position>>> {
    if west == east || south == north {
        return None;
    }
    let east = if west > east { east + 360.0 } else { east };
    let corner = |lon: f64, lat| Position::new(near(lon, 0.0), lat);
    // A box wider than 180° cannot be one ring under shortest-step
    // unwrapping; split it at its middle meridian.
    let width = east - west;
    let ring = |w: f64, e: f64| {
        let mid = w + (e - w) / 2.0;
        vec![
            corner(w, south),
            corner(mid, south),
            corner(e, south),
            corner(e, north),
            corner(mid, north),
            corner(w, north),
            corner(w, south),
        ]
    };
    if width >= 360.0 {
        return None;
    }
    Some(vec![ring(west, east)])
}

/// Index windows that hold every entity within `meters` of `center`.
pub(crate) fn radius_windows(center: Position, meters: f64) -> Vec<BoundingBox> {
    let angle = meters / EARTH_RADIUS_M;
    let dlat = angle.to_degrees();
    let (south, north) = (
        (center.lat - dlat).max(-90.0),
        (center.lat + dlat).min(90.0),
    );
    let polar = center.lat + dlat >= 90.0 || center.lat - dlat <= -90.0;
    let dlon = if polar || angle >= std::f64::consts::FRAC_PI_2 {
        180.0
    } else {
        (angle.sin() / center.lat.to_radians().cos())
            .clamp(-1.0, 1.0)
            .asin()
            .to_degrees()
    };
    if dlon >= 180.0 {
        return query_boxes(-180.0, south, 180.0, north);
    }
    let (west, east) = (near(center.lon - dlon, 0.0), near(center.lon + dlon, 0.0));
    query_boxes(west, south, east, north)
}

/// Index windows for a non-nearest query.
pub(crate) fn windows(query: &Query) -> Vec<BoundingBox> {
    match query {
        Query::Bbox([west, south, east, north]) => query_boxes(*west, *south, *east, *north),
        Query::Radius { center, meters } => radius_windows(*center, *meters),
        Query::Polygon { rings, .. } => Geometry::Polygon(rings.clone()).boxes(),
        Query::Nearest { center, .. } => {
            radius_windows(*center, std::f64::consts::PI * EARTH_RADIUS_M)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(lon: f64, lat: f64) -> Position {
        Position::new(lon, lat)
    }

    #[test]
    fn haversine_matches_known_distances() {
        // One degree of latitude is about 111.2 km.
        assert!((haversine(p(0.0, 0.0), p(0.0, 1.0)) - 111_195.0).abs() < 10.0);
        // Across the antimeridian the short way round.
        assert!((haversine(p(179.5, 0.0), p(-179.5, 0.0)) - 111_195.0).abs() < 10.0);
    }

    #[test]
    fn polygons_across_the_antimeridian_contain_and_intersect() {
        let square = vec![vec![
            p(179.0, -1.0),
            p(-179.0, -1.0),
            p(-179.0, 1.0),
            p(179.0, 1.0),
            p(179.0, -1.0),
        ]];
        assert!(relates(
            &Geometry::Point(p(-179.5, 0.0)),
            &square,
            Relation::Contains
        ));
        assert!(relates(
            &Geometry::Point(p(179.5, 0.0)),
            &square,
            Relation::Contains
        ));
        assert!(!relates(
            &Geometry::Point(p(0.0, 0.0)),
            &square,
            Relation::Intersects
        ));
        let through = Geometry::Line(vec![p(178.0, 0.0), p(-178.0, 0.0)]);
        assert!(relates(&through, &square, Relation::Intersects));
        assert!(!relates(&through, &square, Relation::Contains));
    }

    #[test]
    fn boundaries_degenerate_boxes_holes_and_polar_caps_are_exact() {
        let square =
            |w: f64, s: f64, e: f64, n: f64| vec![p(w, s), p(e, s), p(e, n), p(w, n), p(w, s)];
        // A point on the edge of a box or polygon intersects it.
        assert!(matches(
            &Query::Bbox([0.0, 0.0, 1.0, 1.0]),
            &Geometry::Point(p(1.0, 0.5))
        ));
        assert!(relates(
            &Geometry::Point(p(1.0, 0.5)),
            &[square(0.0, 0.0, 1.0, 1.0)],
            Relation::Intersects
        ));
        // A point box tests the geometry itself, not its bounding box.
        let diagonal = Geometry::Line(vec![p(-1.0, -1.0), p(1.0, 1.0)]);
        assert!(!matches(&Query::Bbox([0.0, 0.5, 0.0, 0.5]), &diagonal));
        assert!(matches(&Query::Bbox([0.5, 0.5, 0.5, 0.5]), &diagonal));
        let holed = Geometry::Polygon(vec![
            square(-10.0, -10.0, 10.0, 10.0),
            square(-1.0, -1.0, 1.0, 1.0),
        ]);
        assert!(!matches(&Query::Bbox([0.0, 0.0, 0.0, 0.0]), &holed));
        // An entity over the query's hole is not contained.
        let query = vec![
            square(-10.0, -10.0, 10.0, 10.0),
            square(-1.0, -1.0, 1.0, 1.0),
        ];
        let over_hole = Geometry::Polygon(vec![square(-5.0, -5.0, 5.0, 5.0)]);
        assert!(!relates(&over_hole, &query, Relation::Contains));
        assert!(relates(
            &Geometry::Polygon(vec![square(2.0, 2.0, 3.0, 3.0)]),
            &query,
            Relation::Contains
        ));
        // A wide horizontal box keeps its eastward extent.
        assert!(matches(
            &Query::Bbox([-170.0, 0.0, 170.0, 0.0]),
            &Geometry::Point(p(0.0, 0.0))
        ));
        assert!(matches(
            &Query::Bbox([-180.0, 0.0, 180.0, 0.0]),
            &Geometry::Point(p(90.0, 0.0))
        ));
        assert!(!matches(
            &Query::Bbox([-170.0, 0.0, 170.0, 0.0]),
            &Geometry::Point(p(175.0, 0.0))
        ));
        // Touching the boundary from inside is still contained.
        let outer = vec![square(0.0, 0.0, 4.0, 4.0)];
        assert!(relates(
            &Geometry::Polygon(outer.clone()),
            &outer,
            Relation::Contains
        ));
        assert!(relates(
            &Geometry::Line(vec![p(0.0, 2.0), p(2.0, 2.0)]),
            &outer,
            Relation::Contains
        ));
        assert!(!relates(
            &Geometry::Line(vec![p(2.0, 2.0), p(5.0, 2.0)]),
            &outer,
            Relation::Contains
        ));
        // A hole sits inside its exterior's span across the seam-free frame.
        let wide = vec![
            vec![
                p(-170.0, -10.0),
                p(0.0, -10.0),
                p(170.0, -10.0),
                p(170.0, 10.0),
                p(0.0, 10.0),
                p(-170.0, 10.0),
                p(-170.0, -10.0),
            ],
            square(160.0, -1.0, 165.0, 1.0),
        ];
        assert!(!relates(
            &Geometry::Point(p(162.0, 0.0)),
            &wide,
            Relation::Intersects
        ));
        assert!(relates(
            &Geometry::Point(p(150.0, 0.0)),
            &wide,
            Relation::Intersects
        ));
        // Identical holes are contained; a line through a hole's vertices is not.
        assert!(relates(
            &holed,
            &[
                square(-10.0, -10.0, 10.0, 10.0),
                square(-1.0, -1.0, 1.0, 1.0)
            ],
            Relation::Contains
        ));
        let diamond = vec![
            square(0.0, 0.0, 10.0, 10.0),
            vec![
                p(1.0, 5.0),
                p(2.0, 4.0),
                p(3.0, 5.0),
                p(2.0, 6.0),
                p(1.0, 5.0),
            ],
        ];
        assert!(!relates(
            &Geometry::Line(vec![p(0.0, 5.0), p(10.0, 5.0)]),
            &diamond,
            Relation::Contains
        ));
        // Endpoint order and ring start do not change answers.
        let line = Geometry::Line(vec![p(0.0, 0.0), p(170.0, 80.0)]);
        let reversed = Geometry::Line(vec![p(170.0, 80.0), p(0.0, 0.0)]);
        let center = p(-170.0, 60.0);
        assert!((distance(&line, center) - distance(&reversed, center)).abs() < 1.0);
        let ring = |start: usize| {
            let corners = [
                p(70.0, 50.0),
                p(170.0, 50.0),
                p(-170.0, -60.0),
                p(-50.0, -60.0),
            ];
            let mut ring: Vec<_> = (0..4).map(|i| corners[(start + i) % 4]).collect();
            ring.push(ring[0]);
            Geometry::Polygon(vec![ring])
        };
        assert_eq!(
            distance(&ring(0), p(0.0, 80.0)) == 0.0,
            distance(&ring(2), p(0.0, 80.0)) == 0.0
        );
        // A boundary vertex across the seam compares exactly.
        let seam_box = vec![vec![
            p(170.0, -1.0),
            p(-170.1, -1.0),
            p(-170.1, 1.0),
            p(170.0, 1.0),
            p(170.0, -1.0),
        ]];
        assert!(relates(
            &Geometry::Point(p(-170.1, 0.0)),
            &seam_box,
            Relation::Intersects
        ));
        assert!(relates(
            &Geometry::Point(p(-170.1, 0.0)),
            &seam_box,
            Relation::Contains
        ));
        // A wide eastward box keeps its direction piece by piece.
        assert!(matches(
            &Query::Bbox([10.0, 0.0, -160.0, 0.0]),
            &Geometry::Point(p(20.0, 0.0))
        ));
        // A cap with an indentation is read from every crossing.
        let indented = |start: usize| {
            let corners = [
                p(0.0, 80.0),
                p(120.0, 80.0),
                p(60.0, 85.0),
                p(180.0, 85.0),
                p(-60.0, 80.0),
            ];
            let mut ring: Vec<_> = (0..5).map(|i| corners[(start + i) % 5]).collect();
            ring.push(ring[0]);
            Geometry::Polygon(vec![ring])
        };
        for start in 0..5 {
            assert!(
                distance(&indented(start), p(90.0, 84.0)) > 0.0,
                "start {start}"
            );
            assert_eq!(
                distance(&indented(start), p(90.0, 88.0)),
                0.0,
                "start {start}"
            );
        }
        // The nearest point on an edge minimizes the spherical distance.
        let slanted = Geometry::Line(vec![p(-60.0, 60.0), p(60.0, 80.0)]);
        assert!(
            distance(&slanted, p(0.0, 0.0)) <= haversine(p(0.0, 0.0), p(-20.0, 66.666_666_7)) + 1.0
        );
        // A pole point meets a polygon with a vertex there, at any longitude.
        let to_pole = vec![vec![
            p(0.0, 80.0),
            p(10.0, 80.0),
            p(0.0, 90.0),
            p(0.0, 80.0),
        ]];
        assert!(relates(
            &Geometry::Point(p(120.0, 90.0)),
            &to_pole,
            Relation::Intersects
        ));
        assert!(relates(
            &Geometry::Point(p(120.0, 90.0)),
            &to_pole,
            Relation::Contains
        ));
        // A zero-width box on the seam is that meridian's point.
        assert!(matches(
            &Query::Bbox([180.0, 0.0, -180.0, 0.0]),
            &Geometry::Point(p(180.0, 0.0))
        ));
        // Coincident points are exactly zero apart, also across the seam.
        assert_eq!(
            distance(&Geometry::Point(p(180.0, 0.0)), p(-180.0, 0.0)),
            0.0
        );
        let edge = Geometry::Line(vec![p(0.0, 0.0), p(3.0, 3.0)]);
        assert_eq!(distance(&edge, p(1.234_567, 1.234_567)), 0.0);
        // A polar annulus excludes its inner cap.
        let circle = |lat: f64| {
            let mut ring: Vec<_> = [-170.0, -50.0, 70.0, 170.0]
                .iter()
                .map(|&lon| p(lon, lat))
                .collect();
            ring.push(ring[0]);
            ring
        };
        let annulus = Geometry::Polygon(vec![circle(80.0), circle(85.0)]);
        assert!(distance(&annulus, p(0.0, 89.0)) > 0.0);
        assert_eq!(distance(&annulus, p(0.0, 82.0)), 0.0);
        // A full-longitude band is tested exactly, so the annulus's hole is
        // not a match; a half-turn edge keeps its own direction.
        assert!(!matches(
            &Query::Bbox([-180.0, 87.0, 180.0, 88.0]),
            &annulus
        ));
        assert!(matches(&Query::Bbox([-180.0, 81.0, 180.0, 82.0]), &annulus));
        let half_turn = Geometry::Line(vec![p(-90.0, 0.0), p(90.0, 0.0)]);
        assert!(distance(&half_turn, p(180.0, 0.0)) > 9_000_000.0);
        assert!(!matches(
            &Query::Radius {
                center: p(180.0, 60.0),
                meters: 7_000_000.0
            },
            &half_turn
        ));
        // Touching a pole is not circling it.
        let triangle = Geometry::Polygon(vec![vec![
            p(0.0, 80.0),
            p(10.0, 80.0),
            p(0.0, 90.0),
            p(0.0, 80.0),
        ]]);
        assert!(distance(&triangle, p(5.0, 89.0)) > 0.0);
        // Integer ranges compare exactly beyond 2^53.
        let big = Entity {
            collection: "c".into(),
            id: "i".into(),
            geometry: Geometry::Point(p(0.0, 0.0)),
            ts_ms: 0,
            speed: None,
            heading: None,
            ttl_ms: None,
            fields: [("n".to_string(), FieldValue::Int(9_007_199_254_740_993))].into(),
        };
        let range = |min, max| Filter::Range {
            field: "n".into(),
            min,
            max,
        };
        assert!(!range(None, Some(9_007_199_254_740_992.0)).matches(&big));
        assert!(range(Some(9_007_199_254_740_992.0), None).matches(&big));
        // A cap around the north pole holds the area poleward of its ring.
        let cap = Geometry::Polygon(vec![vec![
            p(-170.0, 85.0),
            p(-50.0, 85.0),
            p(70.0, 85.0),
            p(170.0, 85.0),
            p(-170.0, 85.0),
        ]]);
        assert_eq!(distance(&cap, p(0.0, 89.0)), 0.0);
        assert!(distance(&cap, p(0.0, 80.0)) > 0.0);
        assert!(matches(&Query::Bbox([10.0, 87.0, 11.0, 88.0]), &cap));
        assert!(!matches(&Query::Bbox([10.0, 70.0, 11.0, 71.0]), &cap));
        // A band contains the annulus around the pole but never a full cap.
        let wide_band = vec![vec![
            p(-180.0, 79.0),
            p(0.0, 79.0),
            p(180.0, 79.0),
            p(180.0, 86.0),
            p(0.0, 86.0),
            p(-180.0, 86.0),
            p(-180.0, 79.0),
        ]];
        assert!(relates(&annulus, &wide_band, Relation::Contains));
        // A band up to the pole contains the cap; one short of it does not.
        let to_pole = vec![vec![
            p(-180.0, 80.0),
            p(0.0, 80.0),
            p(180.0, 80.0),
            p(180.0, 90.0),
            p(0.0, 90.0),
            p(-180.0, 90.0),
            p(-180.0, 80.0),
        ]];
        assert!(relates(&cap, &to_pole, Relation::Contains));
        // No band contains a polar cap, which reaches the pole.
        let band = vec![vec![
            p(-180.0, 84.0),
            p(0.0, 84.0),
            p(180.0, 84.0),
            p(180.0, 86.0),
            p(0.0, 86.0),
            p(-180.0, 86.0),
            p(-180.0, 84.0),
        ]];
        assert!(!relates(&cap, &band, Relation::Contains));
    }

    #[test]
    fn radius_windows_hold_the_circle_near_poles_and_the_seam() {
        for center in [p(179.9, 10.0), p(0.0, 89.5), p(-45.0, -89.9), p(10.0, 60.0)] {
            let windows = radius_windows(center, 50_000.0);
            for bearing in 0..36 {
                // Points at ~49 km in every direction stay inside a window.
                let angle = 49_000.0 / EARTH_RADIUS_M;
                let theta = f64::from(bearing * 10).to_radians();
                let lat1 = center.lat.to_radians();
                let lat2 =
                    (lat1.sin() * angle.cos() + lat1.cos() * angle.sin() * theta.cos()).asin();
                let lon2 = center.lon.to_radians()
                    + (theta.sin() * angle.sin() * lat1.cos())
                        .atan2(angle.cos() - lat1.sin() * lat2.sin());
                let at = p(near(lon2.to_degrees(), 0.0), lat2.to_degrees());
                let probe = query_boxes(at.lon, at.lat, at.lon, at.lat);
                assert!(
                    windows
                        .iter()
                        .any(|w| probe.iter().any(|q| w.intersects(q))),
                    "{center:?} bearing {bearing}"
                );
            }
        }
    }
}
