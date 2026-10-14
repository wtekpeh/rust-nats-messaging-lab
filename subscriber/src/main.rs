use futures_util::StreamExt;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct GpsTelemetry {
    device_id: String,
    latitude: f64,
    longitude: f64,
    speed_kmh: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Successfully connected to NATS");

    let mut subscriber = client
        .queue_subscribe("orbi.gps.location", String::from("gps-workers"))
        .await?;

    println!("Listening for GPS messages...");

    while let Some(message) = subscriber.next().await {
        let telemetry: GpsTelemetry = match serde_json::from_slice(&message.payload) {
            Ok(telemetry) => telemetry,

            Err(error) => {
                eprintln!("Failed to deserialize GPS telemetry: {}", error);
                continue;
            }
        };

        println!("Deserialized GPS telemetry: {:?}", telemetry);

        println!("Device ID: {}", telemetry.device_id);
        println!("Latitude: {}", telemetry.latitude);
        println!("Longitude: {}", telemetry.longitude);
        println!("Speed: {} km/h", telemetry.speed_kmh);
    }

    Ok(())
}
