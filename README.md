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

## Development Roadmap

The following capabilities are planned and are not yet implemented.

- [x] Core NATS publisher and subscriber
- [x] Fan-out messaging experiment
- [ ] Queue groups and load balancing
- [ ] Structured JSON messages using Serde
- [ ] JetStream persistence and message replay
- [ ] Consumer acknowledgments and redelivery
- [ ] Retry strategies and dead-letter handling
- [ ] Idempotent message processing
- [ ] Reconnection and failure recovery
- [ ] Integration and automated testing
- [ ] Observability and monitoring
- [ ] Containerized application deployment
- [ ] Kubernetes deployment and scaling
- [ ] Performance and reliability experiments

## Engineering Goals

The long-term objective is to understand and implement reliable distributed messaging patterns, including message delivery guarantees, consumer coordination, fault tolerance, and production deployment considerations.

The repository will evolve incrementally, with each major feature supported by practical experiments and documentation.

## Author

[William Tekpeh](https://github.com/wtekpeh)

## Project Status

**Active development — Core NATS publish/subscribe completed.**

This repository is an evolving engineering lab. Additional messaging capabilities will be introduced and documented as development progresses.