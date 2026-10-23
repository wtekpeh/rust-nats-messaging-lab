use async_nats::jetstream::{self, consumer};
use futures_util::StreamExt;

use serde::{Deserialize, Serialize};

use sqlx::postgres::PgPoolOptions;
use std::env;

#[derive(Debug, Serialize, Deserialize)]
struct GpsTelemetry {
    event_id: String,
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connect to the NATS server.
    let client = async_nats::connect("127.0.0.1:4222").await?;
    println!("Connected to NATS");

    // Load the database connection string from the environment.
    let database_url = env::var("DATABASE_URL")?;

    // Create a PostgreSQL connection pool.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    println!("Connected to PostgreSQL");

    // Create a JetStream API context.
    let jetstream = jetstream::new(client);

    // Retrieve our existing GPS stream.
    let stream = jetstream.get_stream("ORBI_GPS").await?;
    println!("Connected to stream: ORBI_GPS");

    // Create or retrieve a durable pull consumer.
    let mut consumer = stream
        .get_or_create_consumer(
            "GPS_PROCESSOR",
            consumer::pull::Config {
                durable_name: Some("GPS_PROCESSOR".to_string()),
                ack_policy: consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await?;

    println!("Durable consumer ready: GPS_PROCESSOR");

    let info = consumer.info().await?;

    println!("Consumer name: {}", info.name);
    println!("Pending messages: {}", info.num_pending);
    println!("Awaiting ACK: {}", info.num_ack_pending);
    println!("Redelivered messages: {}", info.num_redelivered);

    // Request one message from the durable pull consumer.
    let mut messages = consumer.fetch().max_messages(2).messages().await?;

    while let Some(message_result) = messages.next().await {
        let message = message_result?;

        let telemetry: GpsTelemetry = serde_json::from_slice(&message.payload)?;

        println!("Received GPS event: {}", telemetry.event_id);

        let result = sqlx::query(
            r#"
    INSERT INTO gps_events (
        event_id,
        device_id,
        latitude,
        longitude,
        speed_kmh
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (event_id) DO NOTHING
    "#,
        )
        .bind(&telemetry.event_id)
        .bind(&telemetry.device_id)
        .bind(telemetry.latitude)
        .bind(telemetry.longitude)
        .bind(telemetry.speed_kmh)
        .execute(&pool)
        .await?;

        if result.rows_affected() == 1 {
            println!("New GPS event persisted successfully.");
            println!("Device ID: {}", telemetry.device_id);
            println!("Latitude: {}", telemetry.latitude);
            println!("Longitude: {}", telemetry.longitude);
            println!("Speed: {} km/h", telemetry.speed_kmh);
        } else {
            println!(
                "Duplicate GPS event {}. Already stored in PostgreSQL. Skipping.",
                telemetry.event_id
            );
        }

        // Acknowledge only after PostgreSQL has completed successfully.
        message.double_ack().await?;
        println!("GPS message acknowledgment confirmed by JetStream.");
    }

    let updated_info = consumer.info().await?;

    println!("--- Consumer state after processing ---");
    println!("Pending messages: {}", updated_info.num_pending);
    println!("Awaiting ACK: {}", updated_info.num_ack_pending);
    println!("Redelivered messages: {}", updated_info.num_redelivered);

    Ok(())
}
