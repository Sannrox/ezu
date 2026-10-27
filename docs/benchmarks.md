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
| Load | 315,000 updates/s |
| Move | 225,000 updates/s |
| Bbox query | p50 83 µs, p99 123 µs |
| Radius query | p50 2 µs, p99 6 µs |
| Nearest query | p50 38 µs, p99 94 µs |
| Fence evaluation | p50 7 µs, p99 15 µs |
| Recovery | 5.8 s |
| Follower lag during the 10^6 load | p50 115,000, p99 224,000 records |
| Follower catch-up after the load | 0.8 s |

The update rate is below the 500,000 to 1,000,000 updates/s target. Each
batch pays for one sync, and the WAL and the index are written under one
lock, so that lock is where the rate goes next.

The follower applies each batch with its own sync, in the same process as
the leader, so during a load at the full write rate it trails by well
under a second of writes and catches up within a second after.
