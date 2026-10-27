# Rust NATS Messaging Lab

A hands-on distributed messaging engineering project built with **Rust, Tokio, and NATS**, progressing from fundamental publish/subscribe messaging toward production-oriented event-driven architectures.

The project explores messaging patterns, delivery semantics, reliability, scalability, and deployment through practical implementations and experiments.

## Overview

Distributed applications often need to exchange messages without being directly coupled to one another.

This repository explores how NATS enables asynchronous communication between independent services using subjects, publishers, and subscribers.

The learning approach combines theoretical understanding with practical Rust implementations, experiments, and incremental improvements.

## Technology Stack

| Technology | Purpose |
|---|---|
| Rust | Publisher and subscriber applications |
| Tokio | Asynchronous runtime |
| async-nats | Rust NATS client |
| NATS Server / JetStream | Core messaging, persistent streams, and durable consumers |
| PostgreSQL | Durable GPS event storage and uniqueness constraints |
| SQLx | Asynchronous PostgreSQL access and schema migrations |
| Docker | Local broker deployment |
| Cargo Workspace | Multi-crate project management |
| GitHub | Version control and project documentation |

## Current Architecture

Core NATS experiments demonstrate live publish/subscribe fan-out and queue groups. The durable JetStream pipeline is:

```text
Rust JetStream Publisher -- orbi.gps.location --> ORBI_GPS (file-backed stream)
                                                      |
                                             GPS_PROCESSOR (durable pull)
                                                      |
                                              Rust GPS Worker
                                                      |
                                         PostgreSQL gps_events (unique event_id)
                                                      |
                                          confirmed ACK after database write

On max deliveries (3):
  JetStream advisory --> ORBI_ADVISORIES (file-backed stream)
                               |
                      GPS_DLQ_HANDLER (durable pull)
                               |
                         Rust DLQ Handler
                               |-- retrieve original ORBI_GPS message by stream_seq
                               |-- publish structured record with Nats-Msg-Id
                               v
                      ORBI_GPS_DLQ (file-backed stream)
                               |
                       confirm publish ACK
                               |
                     confirm advisory ACK
```

The streams store messages independently of application processes. Consumers track delivery and acknowledgment state; the Rust workers perform processing. The dead-letter flow **copies** the original payload into a structured record rather than deleting the original stream message.

## Repository Structure


rust-nats-messaging-lab/
├── Cargo.toml
├── Cargo.lock
├── publisher/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       └── bin/
│           └── jetstream_publisher.rs
├── subscriber/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       └── bin/
│           ├── jetstream_worker.rs
│           ├── dlq_handler.rs
│           ├── dlq_replay.rs
│           └── consumer_info.rs
├── stream-admin/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── migrations/
│   └── 20261023223631_create_gps_events.sql
├── .env                 # Local only; not committed
├── .gitignore
└── README.md


## Getting Started

### Prerequisites

- Rust and Cargo
- Docker
- Git

### 1. Clone the repository


git clone https://github.com/wtekpeh/rust-nats-messaging-lab.git
cd rust-nats-messaging-lab


### 2. Start the NATS server


docker run -d \
  --name nats-lab \
  -p 127.0.0.1:4222:4222 \
  -v nats-lab-data:/data \
  nats:2.12 -js -sd /data


Verify that the server is ready:


docker logs nats-lab


### 3. Run the subscriber


cargo run -p subscriber


The subscriber connects to NATS and listens on the `orbi.gps.location` subject.

### 4. Run the publisher

Open another terminal:

cargo run -p publisher


The publisher sends the message:

Vehicle ORBI-001 is moving


The subscriber receives:


Received: b"Vehicle ORBI-001 is moving"


## Experiments Completed

### Experiment 1: Basic Publish/Subscribe

- Connected Rust applications to a local NATS server.
- Published a message using `async-nats`.
- Received the message through an asynchronous subscriber.
- Verified message routing through a NATS subject.

### Experiment 2: Fan-Out Messaging

- Started two independent subscriber processes.
- Published one message to a shared subject.
- Verified that both active subscribers received the same message.

**Key observation:** Publishers do not need to know how many subscribers exist.

## Messaging Concepts Explored

- TCP message framing versus NATS protocol framing
- Subjects and publish/subscribe communication
- Loose coupling between distributed services
- Asynchronous message consumption
- Byte-oriented message payloads
- Client/server synchronization using `flush()`
- Core NATS at-most-once delivery semantics

### Experiment 3: Queue Groups and Load Balancing

Implemented NATS queue subscriptions using the `gps-workers` queue group.

**Test configuration:**
- Two independent Rust worker processes
- One NATS subject: `orbi.gps.location`
- Ten published messages
- One shared queue group: `gps-workers`

**Observed results:**

| Worker | Messages received |
|---|---|
| Worker A | 1, 2, 3, 4, 6 |
| Worker B | 5, 7, 8, 9, 10 |

All ten messages were received, with no duplicate deliveries observed.

The experiment demonstrated that NATS distributes messages among members of the same queue group without guaranteeing an equal split.

**Reliability limitation:** Core NATS uses at-most-once delivery semantics. Messages are not automatically redelivered when a worker crashes after receiving them.

### Experiment 4: Structured GPS Telemetry with Serde

Implemented structured messaging between independent Rust publisher and subscriber applications using Core NATS.

**Implementation:**
- Defined a `GpsTelemetry` struct containing device ID, latitude, longitude, and speed.
- Serialized Rust telemetry into JSON bytes using `serde_json::to_vec()`.
- Published the JSON payload to `orbi.gps.location`.
- Deserialized received bytes into a strongly typed Rust struct using `serde_json::from_slice()`.
- Implemented error handling with `match` to skip invalid messages without terminating the subscriber.

**Verified results:**
- Successfully transmitted and reconstructed GPS telemetry.
- Confirmed that missing required fields cause deserialization errors.
- Confirmed that the subscriber can remain running after a malformed message.
- Successfully processed valid GPS telemetry.

**Current limitations:**

Core NATS provides at-most-once delivery. Invalid messages are logged and skipped, without persistence, retry, or dead-letter handling.

**Subsequent milestone (completed):** JetStream persistence, acknowledgments, and redelivery.

## JetStream: Persistent and Reliable Messaging

The project has progressed from Core NATS to NATS JetStream to explore persistent messaging, durable consumers, explicit acknowledgments, and message redelivery.

JetStream extends NATS with server-managed streams and consumers while retaining subject-based message routing.

### Architecture

```text
Rust GPS Publisher
        |
        | Subject: orbi.gps.location
        v
    NATS Server
        |
        +-- JetStream Stream: ORBI_GPS
        |       |
        |       +-- File-backed message storage
        |
        +-- Durable Pull Consumer: GPS_PROCESSOR
                |
                +-- Delivery tracking
                +-- Acknowledgment tracking
                +-- Redelivery of unacknowledged messages
                        |
                        v
                Rust GPS Worker
                        |
                        +-- Process telemetry
                        +-- Send explicit ACK
```

The stream stores messages, the consumer manages delivery state, and the Rust application performs the actual processing.

### Running NATS with JetStream

```bash
docker run -d \
  --name nats-lab \
  -p 127.0.0.1:4222:4222 \
  -p 127.0.0.1:8222:8222 \
  -v nats-lab-data:/data \
  nats:2.12 \
  -js -sd /data -m 8222
```

The named Docker volume provides persistent storage across container restarts and replacements, provided the volume is retained.

This configuration uses a single NATS server and does not provide high availability. The `-m 8222` flag enables the HTTP monitoring endpoint on the mapped port.

### JetStream Applications

**Stream administrator**

```bash
cargo run -p stream-admin
```

Creates or retrieves the `ORBI_GPS` stream, configured to capture `orbi.gps.location` with file-backed storage, and displays the stored-message count.

**JetStream processing worker**

```bash
cargo run -p subscriber --bin jetstream_worker
```

Uses the durable pull consumer `GPS_PROCESSOR` to retrieve messages, acknowledge processing, and inspect consumer state.

```bash
cargo run -p publisher --bin jetstream_publisher
```

### Experiment 5: Persistent Message Storage

Published structured GPS telemetry while no application subscriber was running.

Verified that `ORBI_GPS` retained the message and reported one stored message.

**Result:** JetStream retained the message independently of an active application subscriber.

### Experiment 6: Persistence Across Server Restart

Restarted the NATS container:

```bash
docker restart nats-lab
```

Queried the stream again using `stream-admin`.

**Result:** The previously stored GPS message remained available after the restart.

This demonstrates local persistence, not replicated fault tolerance.

### Experiment 7: Durable Consumer and Redelivery

Created the durable pull consumer `GPS_PROCESSOR` with explicit acknowledgment policy.

Fetched a GPS message without acknowledging it, then restarted the Rust worker after the acknowledgment timeout.

**Observed state:**

```text
Pending messages: 0
Awaiting ACK: 1
Redelivered messages: 1
```

The same GPS telemetry was delivered again.

**Result:** Unacknowledged deliveries can be retried, providing the foundation for at-least-once processing.

### Experiment 8: Explicit Acknowledgment

Updated the Rust worker to acknowledge successful message processing:

```rust
message.ack().await?;
```

Inspected the durable consumer after processing.

**Observed state:**

```text
Pending messages: 0
Awaiting ACK: 0
Redelivered messages: 0
```

**Result:** JetStream recorded the acknowledgment and cleared the outstanding delivery state.

Under the stream's default limits-based retention policy, acknowledgment does not automatically delete the stored message.

### Experiment 9: JetStream Publish Acknowledgments

Implemented a dedicated JetStream publisher in `publisher/src/bin/jetstream_publisher.rs`, preserving the original Core NATS publisher for comparison.

**Implementation:**
- Connected to the NATS server using `async-nats`.
- Created a JetStream context.
- Serialized structured GPS telemetry into JSON.
- Published to `orbi.gps.location`.
- Awaited JetStream's publish acknowledgment.
- Inspected the returned stream name and sequence number.

**Verified output:**

```text
JetStream publish acknowledged!
Stream: ORBI_GPS
Sequence: 2
```

Queried the stream using `stream-admin` and confirmed that it contained two stored messages.

**Result:** The publisher received confirmation that JetStream accepted and stored the GPS message. Unlike Core NATS `flush()`, the JetStream publish acknowledgment explicitly confirms stream acceptance.

### Experiment 10: Server-Confirmed Consumer Acknowledgments

Updated `subscriber/src/bin/jetstream_worker.rs` to use:

```rust
message.double_ack().await?;
```

This waits for JetStream to confirm receipt of the consumer acknowledgment, rather than only sending the acknowledgment.

**Observed consumer state:**

| Metric | Before processing | After confirmed ACK |
|---|---:|---:|
| Pending messages | 1 | 0 |
| Awaiting ACK | 0 | 0 |
| Redelivered messages | 0 | 0 |

**Result:** The durable consumer `GPS_PROCESSOR` successfully processed the second GPS message, and JetStream confirmed its acknowledgment.

Server-confirmed acknowledgments improve delivery-state certainty but do not make external database writes and message acknowledgments atomic.

### Experiment 11: PostgreSQL Persistence with SQLx

Added SQLx to the Rust JetStream worker and created the dedicated `rust_nats_lab` PostgreSQL database. The migration `migrations/20261023223631_create_gps_events.sql` creates `gps_events` with `event_id TEXT PRIMARY KEY`, `device_id`, latitude, longitude, speed, and a database-generated `received_at` timestamp.

The worker uses a Tokio-compatible SQLx PostgreSQL connection pool (`PgPoolOptions`, maximum five connections). It inserts telemetry before acknowledging delivery to JetStream.

**Verified result:** JetStream sequence 7, carrying `GPS-001`, was inserted into PostgreSQL; SQLx reported `rows_affected() = 1`, and the worker then received a confirmed JetStream ACK.

### Experiment 12: Durable Database-Backed Idempotency

Replaced the worker's in-memory `HashSet` with a PostgreSQL uniqueness constraint and conflict-safe insertion:

```sql
INSERT INTO gps_events (event_id, device_id, latitude, longitude, speed_kmh)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (event_id) DO NOTHING;
```

SQLx's `rows_affected()` distinguishes a new row (`1`) from a duplicate (`0`). This prevents duplicate GPS rows across worker restarts and competing database inserts.

**Verified result:** Publishing `GPS-001` again at JetStream sequence 8 caused the worker to log a duplicate, skip insertion, and acknowledge the delivery. A PostgreSQL query confirmed exactly one `GPS-001` row.

**Scope:** This guarantees idempotent database insertion for a given event ID, not exactly-once message delivery or exactly-once execution of arbitrary external side effects.

### Experiment 13: Crash After Database Commit, Before ACK

Simulated an abrupt worker termination with `std::process::exit(1)` after the SQLx insert succeeded but before `message.double_ack().await?` for event `GPS-CRASH-001`. The temporary crash condition was removed after the experiment.

**Observed sequence:**

1. JetStream accepted `GPS-CRASH-001` at stream sequence 9.
2. PostgreSQL persisted the event successfully.
3. The worker exited before acknowledging the delivery.
4. A separate Rust diagnostic (`subscriber/src/bin/consumer_info.rs`) reported `Awaiting ACK: 1`.
5. The restarted worker received the event again; PostgreSQL identified it as a duplicate.
6. The worker sent a server-confirmed acknowledgment.
7. The final diagnostic showed `Pending messages: 0` and `Awaiting ACK: 0`.

**Result:** At-least-once delivery combined with durable, idempotent PostgreSQL writes safely recovered from a crash between the database commit and message acknowledgment. The database write and JetStream ACK are not a single atomic transaction.

### Running the PostgreSQL-Backed JetStream Worker

Prerequisites: NATS with JetStream enabled, PostgreSQL reachable at `127.0.0.1:5432`, and the SQLx CLI for migrations. Create the `rust_nats_lab` database before applying migrations.

Create a **local, untracked** `.env` file at the workspace root:

```dotenv
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@127.0.0.1:5432/rust_nats_lab
```

Do not commit `.env` or real credentials. Run migrations:

```bash
cargo sqlx migrate run
```

The current Rust worker reads `DATABASE_URL` from the process environment. For a trusted, shell-compatible local `.env` file, load it into the current shell before running the worker:

```bash
set -a
source .env
set +a
```

Start the publisher and worker (in separate terminal sessions if needed):

```bash
cargo run -p publisher --bin jetstream_publisher
cargo run -p subscriber --bin jetstream_worker
```

Inspect the durable consumer without fetching or acknowledging messages:

```bash
cargo run -p subscriber --bin consumer_info
```

Check persisted records in the existing development PostgreSQL container:

```bash
docker exec dev_postgres \
  psql -U postgres -d rust_nats_lab \
  -c "SELECT event_id, device_id, received_at FROM gps_events;"
```

The `dev_postgres` container is part of this lab's local environment; other setups may use a different container or PostgreSQL host.

### Engineering Lessons

- Core NATS and JetStream provide different delivery and persistence capabilities.
- A JetStream stream stores messages; a durable consumer tracks delivery progress.
- Durable consumer state survives application worker restarts.
- A delivered message is not necessarily an acknowledged message.
- At-least-once delivery requires application-level idempotency to handle duplicates safely.
- Persistent storage does not automatically provide high availability.
- A basic acknowledgment send is different from a server-confirmed acknowledgment.
- Persisting data before ACK enables safe recovery when combined with idempotent writes.
- An acknowledged message and a committed database transaction are independent operations.

### Experiment 14: Retry Policy, Backoff, and Maximum Deliveries

Configured the durable pull consumer `GPS_PROCESSOR` with explicit acknowledgments, `max_deliver = 3`, and retry backoff intervals of 5 and 10 seconds. A deliberately unacknowledged GPS event was delivered three times. After the final acknowledgment timeout, JetStream generated a maximum-deliveries advisory.

**Result:** Exhausted delivery attempts are observable through JetStream advisories; JetStream does not automatically transfer the original message to a dead-letter stream.

### Experiment 15: Durable Failure Advisories

Created file-backed `ORBI_ADVISORIES`, subscribed to `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORBI_GPS.GPS_PROCESSOR`, and created the durable pull consumer `GPS_DLQ_HANDLER` with explicit acknowledgments, a 30-second ACK wait, and a five-delivery limit.

**Result:** A maximum-deliveries advisory remained available even when the Rust DLQ handler was offline. Its JSON identified the original stream, consumer, stream sequence, delivery count, and advisory timestamp.

### Experiment 16: Confirmed Dead-Letter Transfer

Created file-backed `ORBI_GPS_DLQ` on subject `orbi.gps.dlq`. The Rust handler (`subscriber/src/bin/dlq_handler.rs`) consumes a failure advisory, retrieves the original message from `ORBI_GPS` using `get_raw_message(stream_seq)`, publishes it to the DLQ, waits for the JetStream publish acknowledgment, then confirms the advisory ACK with `double_ack()`.

**Verified:** `GPS-RETRY-002` (original stream sequence 11) was stored at DLQ sequence 1. Afterward, `GPS_DLQ_HANDLER` reported zero pending and zero awaiting-ACK messages.

### Experiment 17: Crash Between DLQ Publish and Advisory ACK

The handler published `GPS-RETRY-003` to DLQ sequence 2 and then deliberately exited before acknowledging the advisory. On restart, JetStream redelivered the advisory. The handler used the deterministic `Nats-Msg-Id` `dlq:ORBI_GPS:GPS_PROCESSOR:12`; JetStream returned `duplicate: true`, kept the DLQ message count at 2, and confirmed the advisory acknowledgment.

**Scope:** This demonstrates duplicate suppression **within JetStream's configured deduplication window**. It does not guarantee permanent uniqueness or an atomic transaction spanning publication and acknowledgment.

### Experiment 18: Structured Dead-Letter Records

Updated the handler to publish a JSON envelope containing failure metadata and the original GPS event. `GPS-RETRY-004` was stored at DLQ sequence 3, and an independent `get_raw_message(3)` inspection confirmed the persisted JSON:

```json
{
  "dlq_id": "dlq:ORBI_GPS:GPS_PROCESSOR:13",
  "original_stream": "ORBI_GPS",
  "original_subject": "orbi.gps.location",
  "original_sequence": 13,
  "consumer": "GPS_PROCESSOR",
  "failure_reason": "MAX_DELIVERIES_EXCEEDED",
  "delivery_attempts": 3,
  "failed_at": "2026-10-24T16:29:55.411842444Z",
  "original_event": {
    "device_id": "ORBI-001",
    "event_id": "GPS-RETRY-004",
    "latitude": 5.6037,
    "longitude": -0.187,
    "speed_kmh": 42.5
  }
}
```

`failed_at` is the advisory timestamp, not a captured application exception timestamp. The failure reason indicates exhausted delivery attempts, not the root cause of processing failure.

### Running the Dead-Letter Handler

First configure the streams and consumers:

```bash
cargo run -p stream-admin
```

Start the GPS worker and publisher in separate terminals, with the PostgreSQL `DATABASE_URL` environment variable set for the worker:

```bash
cargo run -p subscriber --bin jetstream_worker
cargo run -p publisher --bin jetstream_publisher
```

For a failure advisory already waiting in `ORBI_ADVISORIES`, process one batch using:

```bash
cargo run -p subscriber --bin dlq_handler
```

The current DLQ handler processes a fetched batch and exits; it is not yet a continuously running production service. The normal publisher and worker no longer contain the hardcoded simulated-failure condition. Reproducing a maximum-deliveries failure requires controlled fault injection.

### Reliability Boundaries and Next JetStream Milestones

- `Nats-Msg-Id` deduplication is window-limited; durable, long-term DLQ idempotency remains future work.
- The handler currently expects the original message to remain in `ORBI_GPS` and its payload to be valid JSON. Missing or malformed originals require explicit error handling and a fallback strategy.
- DLQ advisory processing has its own maximum-delivery limit; exhausted handler retries need monitoring and recovery.
- A dead-letter record is a copy; it does not delete the original stream message.
- Automated integration tests, generalized replay tooling, multi-worker coordination, observability, and deployment remain future work.

### Experiment 19: Validated Manual DLQ Replay and End-to-End Recovery

Implemented `subscriber/src/bin/dlq_replay.rs` to inspect and manually replay a failed GPS event from `ORBI_GPS_DLQ` back to the original `orbi.gps.location` subject.

**Implementation:**

- Retrieved DLQ sequence 3 directly with `get_raw_message(3)` without consuming or acknowledging it.
- Deserialized the structured record into strongly typed `DeadLetterRecord` and `GpsTelemetry` Rust structs using Serde.
- Validated the expected source stream, subject, consumer, failure reason, event and device IDs, finite GPS coordinates within valid ranges, nonnegative finite speed, delivery count, and deterministic DLQ identifier.
- Defaulted to read-only inspection; publishing requires the explicit `--confirm` command-line argument.
- Serialized only `original_event` into GPS JSON for the original processing pipeline.
- Published with deterministic `Nats-Msg-Id` (`replay:dlq:ORBI_GPS:GPS_PROCESSOR:13`) and awaited the JetStream publication acknowledgment.
- Restarted the normal GPS worker, which inserted the recovered event into PostgreSQL before sending a server-confirmed consumer acknowledgment.

**Verified results:**

| Check | Observed result |
|---|---|
| Recovered event | `GPS-RETRY-004` |
| Original stream sequence | 13 |
| DLQ sequence | 3 |
| New `ORBI_GPS` sequence | 14 |
| Replay publish acknowledgment | Confirmed; `duplicate: false` |
| `ORBI_GPS` stored messages after replay | 14 |
| `ORBI_GPS_DLQ` stored messages | 3 (failure history retained) |
| PostgreSQL rows for `GPS-RETRY-004` | 1 |
| `GPS_PROCESSOR` pending / awaiting ACK | 0 / 0 |

**Run inspection only:**

```bash
cargo run -p subscriber --bin dlq_replay
```

**Explicitly authorize replay:**

```bash
cargo run -p subscriber --bin dlq_replay -- --confirm
```

Run `cargo run -p stream-admin` to inspect stream counts, `cargo run -p subscriber --bin consumer_info` to inspect the durable consumer, and query `gps_events` in PostgreSQL to verify persistence. The PostgreSQL-backed worker requires `DATABASE_URL` in its environment.

**Result:** Demonstrated the complete recovery path: failed processing → JetStream retries → maximum-deliveries advisory → structured DLQ record → manual replay → idempotent PostgreSQL persistence → server-confirmed ACK.

**Boundaries:** This is a lab replay tool, not a production replay service. It currently selects fixed DLQ sequence 3, so older unwrapped DLQ records are unsupported. `--confirm` is an operator safeguard, not access control. The deterministic `Nats-Msg-Id` prevents duplicates only within the configured JetStream deduplication window; it does not enforce permanent replay uniqueness or prevent repeated replay loops. No durable replay-status tracking or automated integration test has yet been implemented.

## Development Roadmap

The following checklist tracks implemented and experimentally verified capabilities, alongside upcoming work.

### Core NATS

- [x] Core NATS publisher and subscriber
- [x] Fan-out messaging experiment
- [x] Queue groups and load balancing
- [x] Structured JSON messages using Serde
- [x] Malformed message handling

### NATS JetStream

- [x] JetStream server configuration with persistent Docker storage
- [x] File-backed stream creation (`ORBI_GPS`)
- [x] Message persistence without active subscribers
- [x] Message persistence across server restarts
- [x] Durable pull consumer creation (`GPS_PROCESSOR`)
- [x] Pull-based message fetching
- [x] Explicit consumer acknowledgments
- [x] Unacknowledged message redelivery
- [x] Consumer acknowledgment state inspection
- [x] Validated manual DLQ replay of a selected structured record
- [ ] Generalized message replay and delivery policies
- [x] JetStream publish acknowledgments
- [x] Server-confirmed consumer acknowledgments
- [x] Retry strategies and acknowledgment timeouts
- [x] Maximum delivery attempts and dead-letter handling
- [x] PostgreSQL-backed idempotent GPS inserts
- [ ] Multiple workers sharing a durable pull consumer
- [x] Recovery from worker crash after PostgreSQL commit and before ACK
- [x] DLQ handler crash recovery with window-limited duplicate suppression
- [x] Structured dead-letter record persistence verified
- [ ] Reconnection handling and broader failure recovery

### PostgreSQL and SQLx

- [x] Dedicated PostgreSQL lab database
- [x] Version-controlled SQLx schema migration
- [x] SQLx asynchronous connection pool
- [x] Persist GPS telemetry before JetStream ACK
- [x] Database-enforced unique event IDs
- [x] Consumer-state diagnostic binary
- [ ] Automated database integration tests

### Testing, Operations and Deployment

- [ ] Integration and automated testing
- [ ] Observability and monitoring
- [ ] Containerized Rust application deployment
- [ ] Kubernetes deployment and scaling
- [ ] Performance and reliability experiments
- [ ] JetStream clustering and replication experiments

## Engineering Goals

The long-term objective is to understand and implement reliable distributed messaging patterns, including message delivery guarantees, consumer coordination, fault tolerance, and production deployment considerations.

The repository will evolve incrementally, with each major feature supported by practical experiments and documentation.

## Author

[William Tekpeh](https://github.com/wtekpeh)

## Project Status

**Active development — Core NATS, JetStream persistence, durable consumers, confirmed acknowledgments, PostgreSQL-backed idempotency, retry/backoff policies, persistent failure advisories, structured DLQ handling, crash recovery, and validated manual DLQ replay experimentally verified.**

**Next focus:** Add a small set of automated replay-validation tests, then begin a separate Apache Kafka lab using Rust. Generalized DLQ replay, long-term replay deduplication, advanced observability, and Kubernetes deployment remain future improvements.

This repository is an evolving engineering lab. Additional messaging capabilities will be introduced and documented as development progresses.
