use async_nats::jetstream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct GpsTelemetry {
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the NATS server.
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Connected to NATS");

    // Create a JetStream context.
    let jetstream = jetstream::new(client);

    // Prepare GPS telemetry.
    let telemetry = GpsTelemetry {
        device_id: "ORBI-001".to_string(),
        latitude: 5.6037,
        longitude: -0.1870,
        speed_kmh: 42.5,
    };

    // Serialize telemetry into JSON bytes.
    let json_bytes = serde_json::to_vec(&telemetry)?;

    println!("Publishing GPS telemetry: {:?}", telemetry);

    // Publish and wait for JetStream's persistence acknowledgment.
    let acknowledgment = jetstream
        .publish("orbi.gps.location", json_bytes.into())
        .await?
        .await?;

    println!("JetStream publish acknowledged!");
    println!("Stream: {}", acknowledgment.stream);
    println!("Sequence: {}", acknowledgment.sequence);

    Ok(())
}
