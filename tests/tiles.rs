//! Offline map packages (#13): a package for a fixture region opens in a
//! standard PMTiles reader, holds the entity and fence layers within the
//! label filter only, and its digest verifies.

use ezu::entity::Labels;
use ezu::fence::{Fence, Shape};
use ezu::tiles::{PackageRequest, VectorTile, package};
use ezu::{
    DurabilityConfig, DurableEngine, Entity, Geometry, Identity, Namespace, Position, Quotas,
};
use pmtiles::{AsyncPmTilesReader, TileCoord};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ezu-tiles-{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn ns() -> Namespace {
    Namespace::of(&Identity::authenticated("svc", "t"))
}

fn labels(names: &[&str]) -> Labels {
    names.iter().map(|name| name.to_string()).collect()
}

fn truck(id: &str, lon: f64, lat: f64, marks: &[&str]) -> Entity {
    Entity {
        collection: "trucks".into(),
        id: id.into(),
        geometry: Geometry::Point(Position::new(lon, lat)),
        ts_ms: 7,
        speed: None,
        heading: None,
        ttl_ms: None,
        fields: BTreeMap::new(),
        labels: labels(marks),
    }
}

/// The ids in a layer of a decoded tile.
fn ids(tile: &VectorTile, layer: &str) -> Vec<String> {
    let Some(layer) = tile.layers.iter().find(|l| l.name == layer) else {
        return Vec::new();
    };
    let key = layer.keys.iter().position(|k| k == "id").unwrap() as u32;
    let mut found: Vec<String> = layer
        .features
        .iter()
        .filter_map(|feature| {
            let at = feature.tags.chunks(2).find(|pair| pair[0] == key)?[1];
            layer.values[at as usize].string_value.clone()
        })
        .collect();
    found.sort();
    found
}

#[tokio::test]
async fn a_package_opens_in_a_pmtiles_reader_and_holds_only_what_the_filter_allows() {
    let dir = scratch("package");
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir: dir.join("data"),
            fsync: false,
            segment_bytes: 1 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: 10,
            keys: None,
        },
        Quotas {
            max_entities: 100,
            max_collections: 4,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 10,
        },
        None,
        0,
    )
    .unwrap();
    // A fixture region around a harbour, and one truck far outside it.
    for entity in [
        truck("open", 10.001, 50.001, &[]),
        truck("eu", 10.002, 50.002, &["eu"]),
        truck("secret", 10.003, 50.003, &["secret"]),
        truck("far", 30.0, 10.0, &[]),
    ] {
        engine.upsert(&ns(), entity, 0).unwrap();
    }
    for (id, marks) in [("harbour", &[][..]), ("restricted", &["secret"][..])] {
        let fence = Fence {
            id: id.into(),
            shape: Shape::Circle {
                center: Position::new(10.0, 50.0),
                meters: 500.0,
            },
            collections: Vec::new(),
            filters: Vec::new(),
            dwell_ms: None,
            inside_per_second: None,
            labels: labels(marks),
        };
        engine.put_fence(&ns(), fence, 0).unwrap();
    }
    let request = PackageRequest {
        region: [9.9, 49.9, 10.1, 50.1],
        min_zoom: 8,
        max_zoom: 12,
        readable: Some(labels(&["eu"])),
    };
    let (archive, manifest) = package(&engine, &ns(), &request, 1_000).unwrap();

    // The manifest describes this archive.
    assert_eq!((manifest.min_zoom, manifest.max_zoom), (8, 12));
    assert_eq!(manifest.size, archive.len() as u64);
    let digest: String = Sha256::digest(&archive)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(manifest.sha256, digest);
    assert!(manifest.lsn >= 6);

    let path = dir.join("region.pmtiles");
    std::fs::write(&path, &archive).unwrap();
    let reader = AsyncPmTilesReader::new_with_path(&path).await.unwrap();
    let header = reader.get_header();
    assert_eq!((header.min_zoom, header.max_zoom), (8, 12));
    for z in 8..=12u8 {
        let scale = f64::from(1u32 << z);
        let x = ((10.002 + 180.0) / 360.0 * scale) as u32;
        let lat = 50.002f64.to_radians();
        let y = ((1.0 - (lat.tan() + 1.0 / lat.cos()).ln() / std::f64::consts::PI) / 2.0 * scale)
            as u32;
        let bytes = reader
            .get_tile_decompressed(TileCoord::new(z, x, y).unwrap())
            .await
            .unwrap()
            .expect("the tile holding the trucks");
        let tile = VectorTile::decode(bytes.as_ref()).unwrap();
        assert_eq!(ids(&tile, "entities"), ["eu", "open"], "zoom {z}");
        assert_eq!(ids(&tile, "fences"), ["harbour"], "zoom {z}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn oversized_or_malformed_requests_are_refused() {
    let dir = scratch("refused");
    let engine = DurableEngine::open(
        DurabilityConfig {
            dir: dir.clone(),
            fsync: false,
            segment_bytes: 1 << 20,
            snapshot_every_records: u64::MAX,
            snapshot_every_ms: i64::MAX,
            events_retained: 10,
            keys: None,
        },
        Quotas {
            max_entities: 10,
            max_collections: 4,
            max_vertices: 64,
            max_updates_per_second: u32::MAX,
            max_fences: 10,
        },
        None,
        0,
    )
    .unwrap();
    for (region, min_zoom, max_zoom) in [
        ([-180.0, -80.0, 180.0, 80.0], 0, 16),
        ([10.0, 50.0, 9.0, 51.0], 0, 4),
        ([0.0, 0.0, 1.0, 1.0], 5, 4),
        ([0.0, 0.0, 1.0, 89.0], 0, 4),
    ] {
        let request = PackageRequest {
            region,
            min_zoom,
            max_zoom,
            readable: None,
        };
        assert!(package(&engine, &ns(), &request, 0).is_err(), "{region:?}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
