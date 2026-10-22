//! Geographic geometries in WGS 84 longitude/latitude degrees, and the
//! bounding boxes the index stores for them.
//!
//! Every segment is taken the short way round, so a shape that crosses the
//! antimeridian is stored as two boxes, one on each side, rather than one box
//! wrapping the globe. -180 and 180 are the same
//! meridian, and every longitude meets at a pole: boxes touching either get a
//! sliver for the equivalent coordinates. A polygon ring around a pole is one box over every
//! longitude, reaching that pole.

use std::fmt;

/// A longitude/latitude position in degrees.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Position {
    pub lon: f64,
    pub lat: f64,
}

impl Position {
    pub const fn new(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point(Position),
    Line(Vec<Position>),
    /// Exterior ring first, then holes. Rings are closed (first == last).
    Polygon(Vec<Vec<Position>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeometryError {
    OutOfRange,
    TooFewPositions,
    OpenRing,
}

impl fmt::Display for GeometryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OutOfRange => "coordinates must be finite, lon in [-180, 180], lat in [-90, 90]",
            Self::TooFewPositions => "a line needs two positions and a polygon ring four (closed)",
            Self::OpenRing => "a polygon ring must end where it starts",
        })
    }
}

/// An axis-aligned box that never wraps: `min_lon <= max_lon`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl BoundingBox {
    pub fn intersects(&self, other: &Self) -> bool {
        self.min_lon <= other.max_lon
            && other.min_lon <= self.max_lon
            && self.min_lat <= other.max_lat
            && other.min_lat <= self.max_lat
    }
}

impl Geometry {
    pub fn vertex_count(&self) -> usize {
        match self {
            Self::Point(_) => 1,
            Self::Line(positions) => positions.len(),
            Self::Polygon(rings) => rings.iter().map(Vec::len).sum(),
        }
    }

    pub fn validate(&self) -> Result<(), GeometryError> {
        let in_range = |p: &Position| {
            p.lon.is_finite()
                && p.lat.is_finite()
                && (-180.0..=180.0).contains(&p.lon)
                && (-90.0..=90.0).contains(&p.lat)
        };
        match self {
            Self::Point(p) => in_range(p).then_some(()).ok_or(GeometryError::OutOfRange),
            Self::Line(positions) => {
                if positions.len() < 2 {
                    return Err(GeometryError::TooFewPositions);
                }
                positions
                    .iter()
                    .all(in_range)
                    .then_some(())
                    .ok_or(GeometryError::OutOfRange)
            }
            Self::Polygon(rings) => {
                if rings.is_empty() {
                    return Err(GeometryError::TooFewPositions);
                }
                for ring in rings {
                    if ring.len() < 4 {
                        return Err(GeometryError::TooFewPositions);
                    }
                    if ring.first() != ring.last() {
                        return Err(GeometryError::OpenRing);
                    }
                    if !ring.iter().all(in_range) {
                        return Err(GeometryError::OutOfRange);
                    }
                }
                Ok(())
            }
        }
    }

    /// The boxes that cover the shape: one, two across the antimeridian,
    /// plus a seam sliver when it touches ±180. Assumes [`Self::validate`]
    /// passed.
    pub fn boxes(&self) -> Vec<BoundingBox> {
        let positions: Vec<&Position> = match self {
            Self::Point(p) => vec![p],
            Self::Line(positions) => positions.iter().collect(),
            Self::Polygon(rings) => rings.iter().flatten().collect(),
        };
        let (min_lat, max_lat) = positions
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), p| {
                (lo.min(p.lat), hi.max(p.lat))
            });
        // A ring that winds once around a pole contains every longitude.
        if let Self::Polygon(rings) = self
            && let Some(winding) = rings.first().map(|ring| lon_winding(ring))
            && winding.abs() > 180.0
        {
            let north = min_lat + max_lat >= 0.0;
            return vec![BoundingBox {
                min_lon: -180.0,
                min_lat: if north { min_lat } else { -90.0 },
                max_lon: 180.0,
                max_lat: if north { 90.0 } else { max_lat },
            }];
        }
        // Each segment covers [min, max] of its two longitudes, or the two
        // pieces up to the seam when it crosses the antimeridian. Merging
        // those intervals uses comparisons only, so the result is exact and
        // covers every segment. A polygon's interior adds no longitude: any
        // meridian through it crosses its boundary.
        let mut intervals: Vec<(f64, f64)> = match self {
            Self::Point(p) => vec![(p.lon, p.lon)],
            Self::Line(positions) => segment_intervals(positions),
            Self::Polygon(rings) => rings
                .iter()
                .flat_map(|ring| segment_intervals(ring))
                .collect(),
        };
        intervals.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut merged: Vec<(f64, f64)> = Vec::new();
        for (start, end) in intervals {
            match merged.last_mut() {
                Some(last) if start <= last.1 => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        let mut boxes: Vec<BoundingBox> = merged
            .into_iter()
            .map(|(min_lon, max_lon)| BoundingBox {
                min_lon,
                min_lat,
                max_lon,
                max_lat,
            })
            .collect();
        add_seam_mirrors(&mut boxes);
        boxes
    }
}

/// -180 and 180 are one meridian, and every longitude meets at a pole. A box
/// touching the seam gets a sliver on its other side, and one touching a pole
/// a sliver along that pole, so any query reaching the same place finds it.
fn add_seam_mirrors(boxes: &mut Vec<BoundingBox>) {
    let touches = |lon: f64| boxes.iter().any(|b| b.min_lon <= lon && lon <= b.max_lon);
    let (east, west) = (touches(180.0), touches(-180.0));
    let (min_lat, max_lat) = boxes
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), b| {
            (lo.min(b.min_lat), hi.max(b.max_lat))
        });
    let sliver = |lon| BoundingBox {
        min_lon: lon,
        min_lat,
        max_lon: lon,
        max_lat,
    };
    if east && !west {
        boxes.push(sliver(-180.0));
    }
    if west && !east {
        boxes.push(sliver(180.0));
    }
    // Every longitude meets at a pole: touching it covers the whole pole.
    for pole in [90.0, -90.0] {
        if (min_lat..=max_lat).contains(&pole) {
            boxes.push(BoundingBox {
                min_lon: -180.0,
                min_lat: pole,
                max_lon: 180.0,
                max_lat: pole,
            });
        }
    }
}

/// Longitude intervals of each segment, split at the seam where it crosses.
fn segment_intervals(positions: &[Position]) -> Vec<(f64, f64)> {
    positions
        .windows(2)
        .flat_map(|pair| {
            let (a, b) = (pair[0].lon, pair[1].lon);
            let (lo, hi) = (a.min(b), a.max(b));
            if hi - lo > 180.0 {
                vec![(hi, 180.0), (-180.0, lo)]
            } else {
                vec![(lo, hi)]
            }
        })
        .collect()
}

fn short_step(from: f64, to: f64) -> f64 {
    let step = to - from;
    if step > 180.0 {
        step - 360.0
    } else if step < -180.0 {
        step + 360.0
    } else {
        step
    }
}

/// Sum of longitude steps, each taken the short way round. About ±360 for a
/// ring around a pole, about 0 otherwise.
fn lon_winding(ring: &[Position]) -> f64 {
    ring.windows(2)
        .map(|pair| short_step(pair[0].lon, pair[1].lon))
        .sum()
}

/// Query boxes for a window from `west` to `east`; a window with
/// `west > east` wraps across the antimeridian and becomes two boxes.
pub fn query_boxes(west: f64, south: f64, east: f64, north: f64) -> Vec<BoundingBox> {
    let make = |min_lon, max_lon| BoundingBox {
        min_lon,
        min_lat: south,
        max_lon,
        max_lat: north,
    };
    let mut boxes = if west <= east {
        vec![make(west, east)]
    } else {
        vec![make(west, 180.0), make(-180.0, east)]
    };
    add_seam_mirrors(&mut boxes);
    boxes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(lon: f64, lat: f64) -> Position {
        Position::new(lon, lat)
    }

    #[test]
    fn a_line_across_the_antimeridian_is_two_boxes_not_a_world_box() {
        let line = Geometry::Line(vec![p(179.5, 10.0), p(-179.5, 11.0)]);
        line.validate().unwrap();
        let boxes = line.boxes();
        assert_eq!(boxes.len(), 2);
        let mut spans: Vec<(f64, f64)> = boxes.iter().map(|b| (b.min_lon, b.max_lon)).collect();
        spans.sort_by(|a, b| a.0.total_cmp(&b.0));
        assert_eq!(spans, [(-180.0, -179.5), (179.5, 180.0)]);
        // A query far from the line, at lon 0, must not match.
        let far = query_boxes(-1.0, 10.0, 1.0, 11.0);
        assert!(!boxes.iter().any(|b| far.iter().any(|q| b.intersects(q))));
        // A wrapping query window finds it.
        let wrap = query_boxes(179.0, 9.0, -179.0, 12.0);
        assert!(boxes.iter().any(|b| wrap.iter().any(|q| b.intersects(q))));
    }

    #[test]
    fn every_segment_of_a_wrapping_line_is_covered_and_the_seam_is_one_meridian() {
        let line = Geometry::Line(vec![
            p(170.0, 0.0),
            p(-170.0, 0.0),
            p(-10.0, 0.0),
            p(10.0, 0.0),
        ]);
        let boxes = line.boxes();
        let at_zero = query_boxes(-1.0, -1.0, 1.0, 1.0);
        assert!(
            boxes
                .iter()
                .any(|b| at_zero.iter().any(|q| b.intersects(q)))
        );
        let seam = Geometry::Point(p(-180.0, 0.0)).boxes();
        let east_edge = query_boxes(179.0, -1.0, 180.0, 1.0);
        assert!(
            seam.iter()
                .any(|b| east_edge.iter().any(|q| b.intersects(q)))
        );
        let east_point = Geometry::Point(p(180.0, 0.0)).boxes();
        let west_edge = query_boxes(-180.0, -1.0, -179.0, 1.0);
        assert!(
            east_point
                .iter()
                .any(|b| west_edge.iter().any(|q| b.intersects(q)))
        );
    }

    #[test]
    fn wrapped_bounds_keep_every_vertex() {
        let line = Geometry::Line(vec![p(170.0, 0.0), p(-170.1, 0.0)]);
        let boxes = line.boxes();
        let at_vertex = query_boxes(-170.1, 0.0, -170.1, 0.0);
        assert!(
            boxes
                .iter()
                .any(|b| at_vertex.iter().any(|q| b.intersects(q)))
        );
        // Lines ending or starting on the seam keep ordered boxes on the
        // right side of it.
        let meets = |boxes: &[BoundingBox], lon: f64| {
            let window = query_boxes(lon, -1.0, lon, 1.0);
            boxes.iter().any(|b| window.iter().any(|q| b.intersects(q)))
        };
        let tiny = Geometry::Line(vec![
            p(170.0, 0.0),
            p(-170.0, 0.0),
            p(0.0, 0.0),
            p(1e-15, 0.0),
        ]);
        assert!(meets(&tiny.boxes(), 1e-15));
        for (line, inside, outside) in [
            (
                Geometry::Line(vec![p(170.0, 0.0), p(-180.0, 0.0)]),
                175.0,
                0.0,
            ),
            (
                Geometry::Line(vec![p(180.0, 0.0), p(-170.0, 0.0)]),
                -175.0,
                0.0,
            ),
        ] {
            let boxes = line.boxes();
            assert!(boxes.iter().all(|b| b.min_lon <= b.max_lon));
            assert!(meets(&boxes, inside));
            assert!(!meets(&boxes, outside));
        }
    }

    #[test]
    fn a_point_at_a_pole_is_found_from_any_longitude() {
        let pole = Geometry::Point(p(45.0, 90.0)).boxes();
        let window = query_boxes(-1.0, 89.0, 1.0, 90.0);
        assert!(pole.iter().any(|b| window.iter().any(|q| b.intersects(q))));
        let south = Geometry::Point(p(-120.0, -90.0)).boxes();
        let window = query_boxes(10.0, -90.0, 11.0, -89.0);
        assert!(south.iter().any(|b| window.iter().any(|q| b.intersects(q))));
    }

    #[test]
    fn polar_and_edge_coordinates_are_valid_and_out_of_range_ones_are_not() {
        for point in [p(0.0, 90.0), p(0.0, -90.0), p(180.0, 0.0), p(-180.0, 0.0)] {
            assert!(Geometry::Point(point).validate().is_ok());
        }
        // A ring around the north pole covers every longitude up to the pole,
        // although its closing edge crosses the antimeridian.
        let cap = Geometry::Polygon(vec![vec![
            p(-170.0, 85.0),
            p(-50.0, 85.0),
            p(70.0, 85.0),
            p(170.0, 85.0),
            p(-170.0, 85.0),
        ]]);
        cap.validate().unwrap();
        let boxes = cap.boxes();
        assert_eq!(boxes.len(), 1);
        assert_eq!(
            (
                boxes[0].min_lon,
                boxes[0].max_lon,
                boxes[0].min_lat,
                boxes[0].max_lat
            ),
            (-180.0, 180.0, 85.0, 90.0)
        );
        let near_pole_at_lon_0 = query_boxes(-1.0, 89.0, 1.0, 89.5);
        assert!(boxes[0].intersects(&near_pole_at_lon_0[0]));
        for bad in [p(180.5, 0.0), p(0.0, 90.1), p(f64::NAN, 0.0)] {
            assert_eq!(
                Geometry::Point(bad).validate(),
                Err(GeometryError::OutOfRange)
            );
        }
        let open = Geometry::Polygon(vec![vec![
            p(0.0, 0.0),
            p(1.0, 0.0),
            p(1.0, 1.0),
            p(0.0, 1.0),
        ]]);
        assert_eq!(open.validate(), Err(GeometryError::OpenRing));
    }
}
