use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Successfully connected to NATS");

    let mut subscriber = client
        .queue_subscribe("orbi.gps.location", String::from("gps-workers"))
        .await?;

    println!("Listening for GPS messages...");

    while let Some(message) = subscriber.next().await {
        println!("Received: {:?}", message.payload);
    }

    Ok(())
}
