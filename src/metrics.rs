//! Operational metrics in the Prometheus text format. Every series is an
//! aggregate over all namespaces: no label names a tenant, so the endpoint
//! reveals no tenant's data.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Upper bounds of the query latency histogram, in seconds.
const BUCKETS: [f64; 9] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

/// Counters the engine bumps as it works.
#[derive(Default)]
pub struct Metrics {
    writes: AtomicU64,
    fence_events: AtomicU64,
    /// Per bucket, then the overflow above the last bound.
    latency: [AtomicU64; BUCKETS.len() + 1],
    latency_micros: AtomicU64,
}

/// Point-in-time values read when metrics are rendered.
pub struct Gauges {
    pub entities: u64,
    pub wal_bytes: u64,
    /// Logged records not yet covered by a snapshot.
    pub wal_records_since_snapshot: u64,
    pub snapshot_age_seconds: f64,
    pub sealed_namespaces: u64,
    pub healthy: bool,
}

impl Metrics {
    pub(crate) fn wrote(&self, records: usize) {
        self.writes.fetch_add(records as u64, Ordering::Relaxed);
    }

    pub(crate) fn raised(&self, events: usize) {
        self.fence_events
            .fetch_add(events as u64, Ordering::Relaxed);
    }

    pub(crate) fn queried(&self, took: Duration) {
        let seconds = took.as_secs_f64();
        let bucket = BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(BUCKETS.len());
        self.latency[bucket].fetch_add(1, Ordering::Relaxed);
        self.latency_micros.fetch_add(
            u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// The exposition text of these counters and `gauges`.
    pub fn render(&self, gauges: &Gauges) -> String {
        let mut out = String::new();
        let mut series = |name: &str, kind: &str, help: &str, value: String| {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
            );
        };
        series(
            "ezu_entities",
            "gauge",
            "Entities held across all namespaces.",
            gauges.entities.to_string(),
        );
        series(
            "ezu_writes_total",
            "counter",
            "Logged entity writes (upserts and deletes, expiry included).",
            self.writes.load(Ordering::Relaxed).to_string(),
        );
        series(
            "ezu_fence_events_total",
            "counter",
            "Fence events raised.",
            self.fence_events.load(Ordering::Relaxed).to_string(),
        );
        series(
            "ezu_wal_bytes",
            "gauge",
            "Bytes the write-ahead log holds.",
            gauges.wal_bytes.to_string(),
        );
        series(
            "ezu_wal_records_since_snapshot",
            "gauge",
            "Logged records a recovery would replay.",
            gauges.wal_records_since_snapshot.to_string(),
        );
        series(
            "ezu_snapshot_age_seconds",
            "gauge",
            "Seconds since the last snapshot.",
            gauges.snapshot_age_seconds.to_string(),
        );
        series(
            "ezu_sealed_namespaces",
            "gauge",
            "Namespaces sealed for a missing or revoked key.",
            gauges.sealed_namespaces.to_string(),
        );
        series(
            "ezu_up",
            "gauge",
            "1 while the engine accepts writes, 0 after a durability failure.",
            u8::from(gauges.healthy).to_string(),
        );
        let _ = writeln!(
            out,
            "# HELP ezu_query_duration_seconds Query latency.\n# TYPE ezu_query_duration_seconds histogram"
        );
        let mut cumulative = 0;
        for (bound, count) in BUCKETS.iter().zip(&self.latency) {
            cumulative += count.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "ezu_query_duration_seconds_bucket{{le=\"{bound}\"}} {cumulative}"
            );
        }
        cumulative += self.latency[BUCKETS.len()].load(Ordering::Relaxed);
        let micros = self.latency_micros.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "ezu_query_duration_seconds_bucket{{le=\"+Inf\"}} {cumulative}\n\
             ezu_query_duration_seconds_sum {}\n\
             ezu_query_duration_seconds_count {cumulative}",
            micros as f64 / 1e6
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_histogram_is_cumulative_and_series_carry_no_labels_but_le() {
        let metrics = Metrics::default();
        metrics.queried(Duration::from_micros(500));
        metrics.queried(Duration::from_millis(30));
        metrics.queried(Duration::from_secs(3));
        metrics.wrote(5);
        let text = metrics.render(&Gauges {
            entities: 7,
            wal_bytes: 10,
            wal_records_since_snapshot: 2,
            snapshot_age_seconds: 1.5,
            sealed_namespaces: 0,
            healthy: true,
        });
        assert!(text.contains("ezu_query_duration_seconds_bucket{le=\"0.001\"} 1"));
        assert!(text.contains("ezu_query_duration_seconds_bucket{le=\"0.05\"} 2"));
        assert!(text.contains("ezu_query_duration_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(text.contains("ezu_writes_total 5"));
        assert!(text.contains("ezu_up 1"));
        for line in text.lines().filter(|line| !line.starts_with('#')) {
            assert!(!line.contains('{') || line.contains("{le="), "{line}");
        }
    }
}
