#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Publisher connected to NATS");

    let subject = "orbi.gps.location";

    for i in 1..=10 {
        let payload = format!("GPS Message {}", i);

        client.publish(subject, payload.into()).await?;

        println!("Published message {}", i);
    }

    client.flush().await?;

    println!("All 10 messages published");

    Ok(())
}
