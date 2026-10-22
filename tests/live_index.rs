//! The live index acceptance for #1: writes and expiry show in queries at
//! once, quotas refuse with clear errors, namespaces never see each other,
//! and shapes at the antimeridian and the poles are found.

use ezu::{Engine, Entity, Geometry, Identity, IndexError, Namespace, Position, Quotas, Upsert};
use std::collections::BTreeMap;

const WORLD: [f64; 4] = [-180.0, -90.0, 180.0, 90.0];

fn engine() -> Engine {
    Engine::new(Quotas {
        max_entities: 3,
        max_collections: 2,
        max_vertices: 6,
        max_updates_per_second: 5,
    })
}

fn ns(name: &str) -> Namespace {
    Namespace::of(&Identity::authenticated("svc", name))
}

fn point(id: &str, lon: f64, lat: f64, ts_ms: i64) -> Entity {
    Entity {
        collection: "ships".into(),
        id: id.into(),
        geometry: Geometry::Point(Position::new(lon, lat)),
        ts_ms,
        speed: Some(4.2),
        heading: Some(90.0),
        ttl_ms: None,
        fields: BTreeMap::new(),
    }
}

fn ids(found: Vec<Entity>) -> Vec<String> {
    found.into_iter().map(|e| e.id).collect()
}

#[test]
fn upsert_delete_and_expiry_show_in_queries_at_once() {
    let engine = engine();
    let a = ns("tenant-a");
    assert_eq!(
        engine.upsert(&a, point("s1", 10.0, 50.0, 1_000), 1_000),
        Ok(Upsert::Inserted)
    );
    assert_eq!(
        ids(engine
            .query_bbox(&a, "ships", [9.0, 49.0, 11.0, 51.0], 1_000)
            .unwrap()),
        ["s1"]
    );
    // A move is visible at once, and the old position no longer matches.
    assert_eq!(
        engine.upsert(&a, point("s1", 20.0, 50.0, 2_000), 2_000),
        Ok(Upsert::Updated)
    );
    assert!(
        engine
            .query_bbox(&a, "ships", [9.0, 49.0, 11.0, 51.0], 2_000)
            .unwrap()
            .is_empty()
    );
    // An older observation does not move it back.
    assert_eq!(
        engine.upsert(&a, point("s1", 10.0, 50.0, 1_500), 2_000),
        Ok(Upsert::Stale)
    );
    assert_eq!(
        engine
            .get(&a, "ships", "s1", 2_000)
            .unwrap()
            .unwrap()
            .geometry,
        point("s1", 20.0, 50.0, 0).geometry
    );
    assert!(engine.delete(&a, "ships", "s1").unwrap());
    assert!(
        engine
            .query_bbox(&a, "ships", WORLD, 2_000)
            .unwrap()
            .is_empty()
    );

    let mut brief = point("s2", 0.0, 0.0, 3_000);
    brief.ttl_ms = Some(500);
    engine.upsert(&a, brief, 3_000).unwrap();
    assert_eq!(
        ids(engine.query_bbox(&a, "ships", WORLD, 3_499).unwrap()),
        ["s2"]
    );
    assert!(
        engine
            .query_bbox(&a, "ships", WORLD, 3_500)
            .unwrap()
            .is_empty()
    );
    assert_eq!(engine.get(&a, "ships", "s2", 3_500).unwrap(), None);
    assert_eq!(engine.sweep_expired(3_500).unwrap(), 1);
    assert_eq!(engine.entity_count(&a).unwrap(), 0);
}

#[test]
fn quotas_refuse_with_clear_errors_and_namespaces_stay_apart() {
    let engine = engine();
    let a = ns("tenant-a");
    for (i, id) in ["s1", "s2", "s3"].into_iter().enumerate() {
        engine
            .upsert(&a, point(id, i as f64, 0.0, 1), 1_000)
            .unwrap();
    }
    let full = engine
        .upsert(&a, point("s4", 5.0, 0.0, 1), 1_000)
        .unwrap_err();
    assert_eq!(full, IndexError::EntityQuota { limit: 3 });
    assert_eq!(
        full.to_string(),
        "namespace entity quota reached (3 entities)"
    );
    // Updating an existing entity is not a new one.
    engine.upsert(&a, point("s1", 7.0, 0.0, 2), 1_000).unwrap();
    // Five updates per second: the sixth in the same second is refused.
    assert_eq!(
        engine.upsert(&a, point("s1", 8.0, 0.0, 3), 1_999),
        Err(IndexError::RateQuota { limit: 5 })
    );
    engine.upsert(&a, point("s1", 8.0, 0.0, 3), 2_000).unwrap();
    // A late call stamped in the previous second does not reopen it.
    for _ in 0..4 {
        engine.upsert(&a, point("s1", 8.0, 0.0, 3), 1_999).unwrap();
    }
    assert_eq!(
        engine.upsert(&a, point("s1", 8.0, 0.0, 3), 2_001),
        Err(IndexError::RateQuota { limit: 5 })
    );
    let after = 3_000;
    let mut shape = point("big", 0.0, 0.0, 1);
    shape.geometry = Geometry::Line((0..7).map(|i| Position::new(i as f64, 0.0)).collect());
    assert_eq!(
        engine.upsert(&a, shape, after).unwrap_err().to_string(),
        "shape has 7 vertices; the namespace allows 6"
    );
    // A refused write into a new collection leaves no collection behind, and
    // the collection count is bounded.
    let mut other = point("t1", 0.0, 0.0, 1);
    other.collection = "trucks".into();
    assert_eq!(
        engine.upsert(&a, other.clone(), after),
        Err(IndexError::EntityQuota { limit: 3 })
    );
    let c = ns("tenant-c");
    engine.upsert(&c, other.clone(), 2_500).unwrap();
    other.collection = "planes".into();
    engine.upsert(&c, other.clone(), 2_500).unwrap();
    other.collection = "bikes".into();
    assert_eq!(
        engine.upsert(&c, other, 2_500),
        Err(IndexError::CollectionQuota { limit: 2 })
    );
    // Another namespace has its own quota and sees none of these.
    let b = ns("tenant-b");
    assert!(
        engine
            .query_bbox(&b, "ships", WORLD, 2_000)
            .unwrap()
            .is_empty()
    );
    engine.upsert(&b, point("s1", 0.0, 0.0, 1), 2_000).unwrap();
    assert_eq!(
        engine.query_bbox(&a, "ships", WORLD, 2_000).unwrap().len(),
        3
    );
}

#[test]
fn antimeridian_and_polar_entities_are_found_where_they_are() {
    let engine = engine();
    let a = ns("tenant-a");
    let mut route = point("route", 0.0, 0.0, 1);
    route.geometry = Geometry::Line(vec![
        Position::new(179.0, -16.0),
        Position::new(-179.0, -17.0),
    ]);
    engine.upsert(&a, route, 1_000).unwrap();
    engine
        .upsert(&a, point("pole", 45.0, 90.0, 1), 1_000)
        .unwrap();
    for bad in [
        [f64::NAN, -1.0, 0.0, 1.0],
        [0.0, 2.0, 1.0, 1.0],
        [0.0, -91.0, 1.0, 1.0],
        [181.0, 0.0, 1.0, 1.0],
    ] {
        assert_eq!(
            engine.query_bbox(&a, "ships", bad, 1_000),
            Err(IndexError::InvalidWindow)
        );
    }
    // A window wrapping the antimeridian finds the route; one at lon 0 does not.
    assert_eq!(
        ids(engine
            .query_bbox(&a, "ships", [178.0, -18.0, -178.0, -15.0], 1_000)
            .unwrap()),
        ["route"]
    );
    assert!(
        engine
            .query_bbox(&a, "ships", [-1.0, -18.0, 1.0, -15.0], 1_000)
            .unwrap()
            .is_empty()
    );
    // East and west halves of the window each find it once.
    assert_eq!(
        ids(engine
            .query_bbox(&a, "ships", [-179.5, -18.0, -178.0, -15.0], 1_000)
            .unwrap()),
        ["route"]
    );
    assert_eq!(
        ids(engine
            .query_bbox(&a, "ships", [-180.0, 89.0, 180.0, 90.0], 1_000)
            .unwrap()),
        ["pole"]
    );
    // Bulk load builds a fresh collection in one pass, under the same quota.
    let loaded = engine
        .bulk_load(
            &ns("tenant-c"),
            "ships",
            vec![
                point("x", 1.0, 1.0, 1),
                point("x", 2.0, 2.0, 2),
                point("y", 3.0, 3.0, 1),
            ],
        )
        .unwrap();
    assert_eq!(loaded, 2);
    assert_eq!(
        engine.bulk_load(&ns("tenant-c"), "ships", Vec::new()),
        Err(IndexError::PartitionNotEmpty)
    );
}
