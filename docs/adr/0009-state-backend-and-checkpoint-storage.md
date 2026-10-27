# ADR-0009 — State backend and checkpoint storage

- **Status:** Accepted. Intervals are tuned in Phase 5 with measurements.
- **Date:** 2026-10-26

## Context
- Checkpoints must survive the failure of the container that produced them.
- In Flink, TaskManagers write state files and the JobManager writes checkpoint metadata, so **all Flink containers must see the same checkpoint path**.
- Per-page analytics state is high-cardinality: about 2,000 distinct pages per 2 minutes, growing all day unless TTL bounds it.

## Decision
- **State backend:** RocksDB (`state.backend.type: rocksdb`) with **incremental checkpoints** (`execution.checkpointing.incremental: true`). This is the production default for large or growing keyed state, and it makes TTL cleanup via the compaction filter a teachable topic. The hashmap backend (the default) would be faster at our size, which we will measure in Phase 5.
- **Checkpoint storage:** a **named Docker volume** `flink-data`, mounted at `/flink-data` in the JobManager and every TaskManager:
  - `execution.checkpointing.dir: file:///flink-data/checkpoints`
  - `execution.checkpointing.savepoint-dir: file:///flink-data/savepoints`
- **Initial checkpoint settings** (ASSUMPTION, tuned in Phase 5):

  | Setting | Value |
  |---|---|
  | Interval | 30 s |
  | Mode | EXACTLY_ONCE (aligned barriers; cheap at low throughput) |
  | Timeout | 2 min |
  | Minimum pause | 10 s |
  | Maximum concurrent | 1 |
  | Retained checkpoints | 3, retained on cancellation |
  | Tolerable failed checkpoints | 3 |
  | Restart strategy | `exponential-delay` |

## Alternatives
- **MinIO (S3-compatible).** It mirrors production object storage, and Flink's `s3://` filesystem plugin would be exercised. It is a separate service with credentials and buckets, though, and it teaches little that a shared volume doesn't, until multiple hosts exist. Revisit for the Kubernetes or cloud stretch.
- **Bind mount to a host directory.** Easy to inspect, but on Windows/WSL bind mounts have permission and performance pitfalls. A named volume can still be inspected with `docker run -v flink-data:/x …`.
- **The container filesystem.** Rejected: checkpoints would die with the container, which defeats the purpose.

## Consequences
- This survives TaskManager and JobManager container restarts and `docker compose down` (but not `down -v`).
- A single Docker host is the failure domain. That is acceptable locally and documented.
- **No JobManager HA locally.** Standalone Flink HA requires ZooKeeper or Kubernetes. If the JobManager dies, the job is resubmitted from the latest retained checkpoint, and the Phase 5 runbook documents this. On Kubernetes, the Flink Kubernetes Operator with Kubernetes HA would do it automatically.
