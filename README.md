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
| NATS Server | Messaging broker |
| Docker | Local broker deployment |
| Cargo Workspace | Multi-crate project management |
| GitHub | Version control and project documentation |

## Current Architecture


                    NATS Server
                   localhost:4222
                         |
          +--------------+--------------+
          |                             |
     Rust Publisher              Rust Subscribers
          |                             |
          |       Publish/Subscribe     |
          +-----------------------------+

Subject: orbi.gps.location


The publisher sends messages to a NATS subject. The server routes each message to all active, matching independent subscribers.

## Repository Structure


rust-nats-messaging-lab/
├── Cargo.toml
├── Cargo.lock
├── publisher/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── subscriber/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
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
  nats:2.12


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

**Next milestone:** Explore JetStream persistence, acknowledgments, and redelivery.

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
  -js \
  -sd /data
```

The named Docker volume provides persistent storage across container restarts and replacements, provided the volume is retained.

This configuration uses a single NATS server and does not provide high availability.

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

### Engineering Lessons

- Core NATS and JetStream provide different delivery and persistence capabilities.
- A JetStream stream stores messages; a durable consumer tracks delivery progress.
- Durable consumer state survives application worker restarts.
- A delivered message is not necessarily an acknowledged message.
- At-least-once delivery requires application-level idempotency to handle duplicates safely.
- Persistent storage does not automatically provide high availability.
- A basic acknowledgment send is different from a server-confirmed acknowledgment.

### Next JetStream Milestones

- Confirmed publish and acknowledgment operations
- Typed GPS telemetry processing
- Retry policies and maximum delivery attempts
- Dead-letter handling
- Idempotent PostgreSQL writes
- Multiple workers sharing a durable pull consumer
- Failure recovery, monitoring, and performance testing

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
- [ ] Message replay and delivery policies
- [ ] JetStream publish acknowledgments
- [ ] Server-confirmed consumer acknowledgments
- [ ] Retry strategies and acknowledgment timeouts
- [ ] Maximum delivery attempts and dead-letter handling
- [ ] Idempotent message processing
- [ ] Multiple workers sharing a durable pull consumer
- [ ] Reconnection and failure recovery

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

**Active development — Core NATS publish/subscribe completed.**

This repository is an evolving engineering lab. Additional messaging capabilities will be introduced and documented as development progresses.