//! Spatial queries against a brute-force scan on randomized fixtures (#2):
//! every query kind, filters and paging return exactly what a scan of all
//! entities with the same predicates returns, including at the antimeridian
//! and the poles.

use ezu::query::{distance, matches};
use ezu::{
    Engine, Entity, FieldValue, Filter, Geometry, Identity, Namespace, Position, Query,
    QueryRequest, Quotas, Relation,
};
use std::collections::BTreeMap;

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }

    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next()
    }

    /// Longitudes and latitudes biased toward the seam and the poles.
    fn position(&mut self) -> Position {
        let lon = match (self.next() * 3.0) as u32 {
            0 => self.range(170.0, 180.0),
            1 => self.range(-180.0, -170.0),
            _ => self.range(-180.0, 180.0),
        };
        let lat = if self.next() < 0.2 {
            self.range(80.0, 90.0) * if self.next() < 0.5 { 1.0 } else { -1.0 }
        } else {
            self.range(-80.0, 80.0)
        };
        Position::new(lon, lat)
    }

    fn near(&mut self, at: Position, spread: f64) -> Position {
        let lon = at.lon + self.range(-spread, spread);
        let lon = if lon > 180.0 {
            lon - 360.0
        } else if lon < -180.0 {
            lon + 360.0
        } else {
            lon
        };
        Position::new(
            lon,
            (at.lat + self.range(-spread, spread)).clamp(-89.0, 89.0),
        )
    }
}

fn geometry(rng: &mut Rng) -> Geometry {
    let at = rng.position();
    match (rng.next() * 3.0) as u32 {
        0 => Geometry::Point(at),
        1 => Geometry::Line(vec![at, rng.near(at, 2.0), rng.near(at, 2.0)]),
        _ => {
            let at = Position::new(at.lon, at.lat.clamp(-85.0, 85.0));
            let (w, e) = (at.lon, at.lon + rng.range(0.5, 3.0));
            let wrap = |lon: f64| if lon > 180.0 { lon - 360.0 } else { lon };
            let (s, n) = (at.lat, at.lat + rng.range(0.5, 3.0));
            Geometry::Polygon(vec![vec![
                Position::new(w, s),
                Position::new(wrap(e), s),
                Position::new(wrap(e), n),
                Position::new(w, n),
                Position::new(w, s),
            ]])
        }
    }
}

fn fixture(rng: &mut Rng) -> (Engine, Namespace, Vec<Entity>) {
    let engine = Engine::new(Quotas {
        max_entities: 10_000,
        max_collections: 4,
        max_vertices: 64,
        max_updates_per_second: 1_000_000,
        max_fences: 1_000,
    });
    let ns = Namespace::of(&Identity::authenticated("svc", "t"));
    let entities: Vec<Entity> = (0..1_500)
        .map(|i| Entity {
            collection: "c".into(),
            id: format!("e{i:05}"),
            geometry: geometry(rng),
            ts_ms: 1,
            speed: None,
            heading: None,
            ttl_ms: (i % 10 == 0).then_some(1),
            fields: BTreeMap::from([
                (
                    "kind".into(),
                    FieldValue::Text(["ship", "truck"][i % 2].into()),
                ),
                ("size".into(), FieldValue::Int((i % 7) as i64)),
            ]),
            labels: Default::default(),
        })
        .collect();
    engine.bulk_load(&ns, "c", entities.clone()).unwrap();
    (engine, ns, entities)
}

const NOW: i64 = 10;

fn brute(entities: &[Entity], query: &Query, filters: &[Filter]) -> Vec<String> {
    let mut ids: Vec<String> = entities
        .iter()
        .filter(|e| !e.expired(NOW) && filters.iter().all(|f| f.matches(e)))
        .filter(|e| matches(query, &e.geometry))
        .map(|e| e.id.clone())
        .collect();
    ids.sort();
    ids
}

/// All pages of a query, following cursors.
fn paged(engine: &Engine, ns: &Namespace, query: &Query, filters: &[Filter]) -> Vec<String> {
    let mut ids = Vec::new();
    let mut cursor = None;
    loop {
        let page = engine
            .query(
                ns,
                &QueryRequest {
                    collection: "c".into(),
                    query: query.clone(),
                    filters: filters.to_vec(),
                    limit: 37,
                    cursor,
                    readable: None,
                },
                NOW,
            )
            .unwrap();
        assert!(page.entities.len() <= 37);
        ids.extend(page.entities.into_iter().map(|e| e.id));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return ids,
        }
    }
}

fn filters(rng: &mut Rng) -> Vec<Filter> {
    match (rng.next() * 3.0) as u32 {
        0 => Vec::new(),
        1 => vec![Filter::Eq("kind".into(), FieldValue::Text("ship".into()))],
        _ => vec![Filter::Range {
            field: "size".into(),
            min: Some(2.0),
            max: Some(4.0),
        }],
    }
}

#[test]
fn every_query_kind_matches_a_brute_force_scan() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let (engine, ns, entities) = fixture(&mut rng);
    let mut hits = [0usize; 4];
    for round in 0..60 {
        let at = rng.position();
        let query = match round % 4 {
            0 => {
                let (w, e) = (at.lon, at.lon + rng.range(1.0, 40.0));
                let e = if e > 180.0 { e - 360.0 } else { e };
                let s = at.lat.clamp(-90.0, 85.0);
                Query::Bbox([w, s, e, (s + rng.range(1.0, 20.0)).min(90.0)])
            }
            1 => Query::Radius {
                center: at,
                meters: rng.range(10_000.0, 2_000_000.0),
            },
            _ => {
                let at = Position::new(at.lon, at.lat.clamp(-80.0, 80.0));
                let wrap = |lon: f64| if lon > 180.0 { lon - 360.0 } else { lon };
                let (w, e) = (at.lon, wrap(at.lon + rng.range(2.0, 30.0)));
                let (s, n) = (at.lat, at.lat + rng.range(2.0, 9.0));
                Query::Polygon {
                    rings: vec![vec![
                        Position::new(w, s),
                        Position::new(e, s),
                        Position::new(e, n),
                        Position::new(w, n),
                        Position::new(w, s),
                    ]],
                    relation: if round % 4 == 2 {
                        Relation::Intersects
                    } else {
                        Relation::Contains
                    },
                }
            }
        };
        let filters = filters(&mut rng);
        let expected = brute(&entities, &query, &filters);
        hits[round % 4] += expected.len();
        assert_eq!(
            paged(&engine, &ns, &query, &filters),
            expected,
            "round {round}: {query:?}"
        );
    }
    // Every query kind found something, so the comparison is not vacuous.
    assert!(hits.iter().all(|&n| n > 10), "{hits:?}");
}

#[test]
fn nearest_matches_a_brute_force_ranking() {
    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let (engine, ns, entities) = fixture(&mut rng);
    for round in 0..40 {
        let center = rng.position();
        let k = 1 + round % 10;
        let filters = filters(&mut rng);
        let page = engine
            .query(
                &ns,
                &QueryRequest {
                    collection: "c".into(),
                    query: Query::Nearest { center, k },
                    filters: filters.clone(),
                    limit: 100,
                    cursor: None,
                    readable: None,
                },
                NOW,
            )
            .unwrap();
        let mut expected: Vec<f64> = entities
            .iter()
            .filter(|e| !e.expired(NOW) && filters.iter().all(|f| f.matches(e)))
            .map(|e| distance(&e.geometry, center))
            .collect();
        expected.sort_by(f64::total_cmp);
        expected.truncate(k);
        let got: Vec<f64> = page
            .entities
            .iter()
            .map(|e| distance(&e.geometry, center))
            .collect();
        assert_eq!(got, expected, "round {round}: {center:?} k={k}");
        assert_eq!(page.next_cursor, None);
    }
}

#[test]
fn bad_queries_are_refused() {
    let mut rng = Rng(7);
    let (engine, ns, _) = fixture(&mut rng);
    let request = |query, limit| QueryRequest {
        collection: "c".into(),
        query,
        filters: Vec::new(),
        limit,
        cursor: None,
        readable: None,
    };
    let center = Position::new(0.0, 0.0);
    for (query, limit) in [
        (Query::Nearest { center, k: 0 }, 10),
        (
            Query::Radius {
                center,
                meters: -1.0,
            },
            10,
        ),
        (Query::Bbox([0.0, 5.0, 1.0, 1.0]), 10),
        (Query::Bbox([0.0, 0.0, 1.0, 1.0]), 0),
        (Query::Bbox([0.0, 0.0, 1.0, 1.0]), 10_001),
    ] {
        assert!(engine.query(&ns, &request(query, limit), NOW).is_err());
    }
}
