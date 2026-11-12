# Checkpointing and Recovery

## Scope

The job uses Flink-managed checkpointing for lifecycle, deduplication, and
timer state. Kafka sinks remain at-least-once, so this document distinguishes
Flink state recovery from end-to-end exactly-once delivery.

## Checkpoint configuration

| Setting | Value |
|---|---|
| Checkpoint interval | `60 s` |
| Checkpoint timeout | `5 min` |
| Minimum pause between checkpoints | `10 s` |
| Maximum concurrent checkpoints | `1` |
| Tolerable consecutive checkpoint failures | `3` |
| Checkpointing mode | `EXACTLY_ONCE` |
| Storage | `filesystem` |
| Checkpoint directory | `file:///opt/flink/usrlib/flink-state/checkpoints` |
| Savepoint directory | `file:///opt/flink/usrlib/flink-state/savepoints` |
| State backend | `hashmap` |

The configuration is applied in `configure_recovery()` and
`build_recovery_configuration()` in
`src/order_lifecycle/flink/order_lifecycle_job.py`.

`hashmap` is appropriate for the small, keyed lifecycle state used in the
local Docker environment. It keeps working state on the JVM heap, while
completed checkpoints persist state to disk. RocksDB is not bundled in the
Flink image. A larger production deployment should evaluate RocksDB and a
remote durable filesystem such as HDFS or S3.

## Durable Docker storage

The host directory `F:\DockerLab\processing\flink_docker\jobs` is bind-mounted
to `/opt/flink/usrlib` in both the JobManager and TaskManager. Checkpoints and
savepoints therefore survive container replacement and are readable by both
containers:

- Checkpoints: `F:\DockerLab\processing\flink_docker\jobs\flink-state\checkpoints`
- Savepoints: `F:\DockerLab\processing\flink_docker\jobs\flink-state\savepoints`

Each job writes to its own checkpoint subdirectory:

```text
/opt/flink/usrlib/flink-state/checkpoints/<job-id>/chk-<number>
```

Savepoints are independent of the current job ID:

```text
/opt/flink/usrlib/flink-state/savepoints/savepoint-<job-prefix>-<suffix>
```

Do not delete these directories while their recovery history may still be
needed.

## Restart and recovery behavior

The restart strategy is `fixed-delay` with `10` attempts and a `10 s` delay
between attempts. After a worker failure, Flink restarts the job and restores
the latest completed checkpoint. If all restart attempts are exhausted, the
job transitions to `FAILED` and requires operator intervention.

Container lifecycle is outside Flink. A stopped TaskManager must be started
again by Docker or the surrounding orchestrator. For recovery testing, use:

```powershell
docker restart flink-taskmanager
```

Do not use `docker kill flink-taskmanager` for planned tests. Docker treats it
as a manual stop and may bypass the `unless-stopped` restart policy, leaving
Flink with no worker to restart onto.

## State restored from checkpoints

All lifecycle state is Flink-managed and uses stable descriptor names:

- `event_seen`
- `current_order_status`
- `pending_order_events`
- `last_accepted_event_timestamp`
- `order_placed_timestamp`
- `stage_started_processing_ms`
- `stage_alert_deadline_ms`
- `stage_alert_sent`

Flink also restores event-time timers and processing-time timers. This means:

- Out-of-order events buffered before the checkpoint remain pending.
- Event-time timers that were registered continue to fire after recovery.
- Processing-time delay alerts scheduled before the checkpoint fire at their
  original deadline.
- Previously seen event IDs remain deduplicated after recovery.
- The current lifecycle status, stage timestamps, and alert flags survive.

Descriptor names and types must remain stable across compatible upgrades.
Changing a state name without a migration or explicit savepoint state removal
can prevent restoration.

## Creating a savepoint

Create a savepoint while the job continues running:

```powershell
docker exec flink-jobmanager flink savepoint <job-id> file:///opt/flink/usrlib/flink-state/savepoints
```

Alternatively, stop the job and create a savepoint in one operation:

```powershell
docker exec flink-jobmanager flink stop -p file:///opt/flink/usrlib/flink-state/savepoints <job-id>
```

The command prints the resulting path, for example:

```text
Savepoint completed. Path: file:/opt/flink/usrlib/flink-state/savepoints/savepoint-11cd1e-0eb7f04900f4
```

A job stopped this way ends in `FINISHED`, not `FAILED`. The savepoint remains
on the shared volume.

If a standalone savepoint was created first, the running job can be canceled
without deleting the savepoint:

```powershell
docker exec flink-jobmanager flink cancel <job-id>
```

## Restarting from a savepoint

Submit the job with the savepoint path:

```powershell
docker exec flink-jobmanager flink run -d -s file:/opt/flink/usrlib/flink-state/savepoints/savepoint-11cd1e-0eb7f04900f4 -py /tmp/order_lifecycle_job.py
```

This creates a new Flink job ID and restores the saved state. New checkpoints
for the new job are written under its new checkpoint directory. The Flink REST
checkpoint history should report a restored entry with `is_savepoint=true`.

Do not use `--allowNonRestoredState` unless intentionally removing operators.
The lifecycle descriptors and stable state names allow normal restoration
without skipping state.

## Kafka delivery semantics

Flink checkpointing gives exactly-once semantics for Flink-managed state and
source offsets. It does not make the Kafka sinks transactional:

- Kafka sink delivery remains at-least-once.
- Records written after the last completed checkpoint may be written again
  after recovery.
- The restored deduplication state suppresses duplicate input events, but it
  cannot remove output records that were already written before the failure.
- Downstream consumers should be idempotent or deduplicate by `event_id` where
  output duplication would be harmful.

Topic names, message keys, and schemas are unchanged.

## Validation performed

- Unit suite: `62 tests OK`, including checkpoint settings, storage paths,
  state backend, restart strategy, and stable state descriptor names.
- Checkpoints completed on a running job and were written to the shared volume.
- A TaskManager restart moved the job through `RESTARTING` to `RUNNING` and
  restored checkpoint `chk-2` (`restored=1`) within seconds.
- A buffered event restored from the checkpoint was processed after recovery.
- A restored processing-time timer emitted `STAGE_DELAYED` at the original
  180-second test deadline.
- Republishing a previously seen event ID produced `DUPLICATE_EVENT` after
  recovery.
- State, processed, and exceptions topics all emitted records after recovery.
- Stop-with-savepoint and restore-from-savepoint both completed; the restore
  reported `is_savepoint=true`, and lifecycle state continued to advance.

## Operational limitations

- Checkpoint and savepoint retention/cleanup is not automated; disk usage must
  be monitored.
- The local Docker setup has a single JobManager and no HA metadata store.
  Production use should add JobManager HA and remote checkpoint storage.
- The chosen restart strategy is bounded. Persistent failures stop the job
  after 10 attempts.
- End-to-end exactly-once Kafka delivery is not provided by the current sink.
