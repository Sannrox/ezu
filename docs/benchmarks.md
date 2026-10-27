# Benchmarks

`ezu-bench` measures one node in process, through the durable engine as the
server runs it:

- update throughput: a load and then a move of every entity, in batches of
  10,000 with each batch synced to disk;
- query latency: bbox (1° square), radius (5 km) and nearest (k = 10);
- fence evaluation: single updates against 10^5 fences, without the disk
  sync (the throughput numbers carry that cost);
- memory per entity: the growth of the resident set during the load (Linux
  only);
- recovery: reopening from a snapshot plus a WAL holding one full move.
- follower convergence: a follower tailing the leader in process (as over
  the API, without the network) while the leader loads at most 10^6
  entities: the leader's load rate, the follower's lag in records after
  each batch it applies, and the time it takes to catch up after the
  leader's last write.
- sync standby: the same load against a leader that waits for its sync
  standby (tailing in process, applying with its own sync, then
  acknowledging), and 2,000 single writes after it: the load rate and the
  latency until each batch, and each single write, was acknowledged.

```sh
cargo build --release --locked --bin ezu-bench
./target/release/ezu-bench --entities 1000000            # the CI smoke size
./target/release/ezu-bench --entities 10000000           # the manual size
./target/release/ezu-bench --baseline earlier.json       # fail on > 10% worse
```

CI runs the 10^6 smoke on every change. The nightly workflow runs it against
the result of the last successful nightly and fails on a regression of more
than 10% in any measure. A run that fails keeps the previous baseline. Shared
runners are noisy, so an isolated failure is rerun before it is chased.

At 10^6 the process peaks at about 4 GB of resident memory, most of it the
snapshot taken after the load. Expect roughly ten times that at 10^7, so the
manual run needs a host with well over 40 GB.

## Results

10^6 entities, 10^5 fences. Hardware: Apple M2 Pro (10 cores), 32 GB, internal
NVMe SSD, macOS. Memory per entity is not reported on macOS.

| Measure | Result |
| --- | --- |
| Load | 293,000 updates/s |
| Move | 219,000 updates/s |
| Bbox query | p50 87 µs, p99 108 µs |
| Radius query | p50 2 µs, p99 6 µs |
| Nearest query | p50 37 µs, p99 77 µs |
| Fence evaluation | p50 11 µs, p99 20 µs |
| Recovery | 4.7 s |
| Async follower: leader load | 298,000 updates/s |
| Async follower: lag during the 10^6 load | p50 115,000, p99 224,000 records |
| Async follower: catch-up after the load | 0.8 s |
| Sync standby: leader load | 128,000 updates/s |
| Sync standby: acknowledgement of a 10,000 batch | p50 70 ms, p99 164 ms |
| Sync standby: acknowledgement of a single write | p50 21 ms, p99 49 ms |

The update rate is below the 500,000 to 1,000,000 updates/s target. Each
batch pays for one sync, and the WAL and the index are written under one
lock, so that lock is where the rate goes next.

The follower applies each batch with its own sync, in the same process as
the leader, so during a load at the full write rate it trails by well
under a second of writes and catches up within a second after.

With a sync standby, a write is acknowledged only after the leader's two
syncs (records, then commit) and the standby's two, one after the other.
The single writer of the load waits for each batch before it starts the
next, so the leader's and the standby's syncs do not overlap and the rate
drops to about 43% of the async load; concurrent writers share one
acknowledgement and overlap them. In exchange, the standby holds every
acknowledged write: there is no lag to lose.

## Promotion under load

`tests/kill_leader.rs` is the kill-leader evidence for promotion. The
leader runs as the `ezu` binary with a sync standby and an async follower
(both in the test process, each applying with its own sync), four
concurrent writers of 1,000-entity batches moving through a fence, and an
event subscriber. After 2 seconds of load the leader is SIGKILLed and the
standby promoted. The test checks that every acknowledged write is on the
promoted standby, that the subscriber resumes there from its last event id
and reads every event the standby holds with no gap (a write after the
promotion raises the next ids), and that the async follower switches to the
standby through the lineage check and converges. It prints the numbers
below.

```sh
cargo test --release --locked --test kill_leader -- --nocapture
```

Same hardware as above, two runs (promotion time from a third run after the
wait changed to the lease plus a twentieth):

| Measure | Result |
| --- | --- |
| Acknowledged writes before the kill | 56,000 and 57,000 in 2 s (about 28,000/s) |
| Standby lag during the load | p50 1,000, p99 1,000 records (one batch, about 36 ms) |
| Async follower lag during the load | p50 1,000, p99 1,000 records |
| Events read before the kill | every acknowledged one (56,000 and 57,000) |
| Acknowledged writes missing on the promoted standby | none |
| Event ids skipped or repeated after resuming | none |
| Promotion, from the request to taking writes | 10.5 s |
| Async follower behind the promotion point | 1,000 records |
| Async follower converged on the new leader | 0.11 s after it switched |

Lag is sampled every 20 ms and counts whole batches, as a follower learns
the leader's last LSN with each batch. The async follower is shipped only
what the standby acknowledged, so it never holds a write the standby lacks;
its lag includes the standby's round trip. For lag at the full benchmark
load (one writer of 10,000-entity batches, 10^6 entities), see the async
follower rows above; that run has no sync standby, so it is not capped.

Promotion time is the old leader's lease (10 s) plus a twentieth for clock
rates, counted from the standby's last acknowledgement: the promoted standby takes writes only once the old leader
refuses reads and writes. The write rate is below the benchmark's sync
standby rate because each round trip carries 1,000 entities, not 10,000,
and the async follower syncs on the same disk.
