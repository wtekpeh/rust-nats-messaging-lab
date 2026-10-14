#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Publisher connected to NATS");

    let subject = "orbi.gps.location";
    let payload = "Vehicle ORBI-001 is moving";

    client.publish(subject, payload.into()).await?;

    client.flush().await?;

    println!("Message published successfully");

    Ok(())
}
