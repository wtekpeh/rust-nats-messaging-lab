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
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Publisher connected to NATS");

    let telemetry = GpsTelemetry {
        device_id: String::from("ORBI-001"),
        latitude: 5.6037,
        longitude: -0.1870,
        speed_kmh: 42.5,
    };

    let json_bytes = serde_json::to_vec(&telemetry)?;

    println!("Original Rust struct: {:?}", telemetry);
    println!("Serialized JSON bytes: {:?}", json_bytes);

    let json_text = std::str::from_utf8(&json_bytes)?;

    println!("Readable JSON: {}", json_text);

    let subject = "orbi.gps.location";

    client.publish(subject, json_bytes.into()).await?;

    println!("Published structured GPS telemetry");

    client.flush().await?;

    println!("GPS telemetry successfully published");

    Ok(())
}
