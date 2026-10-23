use async_nats::jetstream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = async_nats::connect("127.0.0.1:4222").await?;

    let jetstream = jetstream::new(client);

    let stream = jetstream.get_stream("ORBI_GPS").await?;

    let mut consumer = stream
        .get_consumer::<jetstream::consumer::pull::Config>("GPS_PROCESSOR")
        .await?;

    let info = consumer.info().await?;

    println!("Consumer: {}", info.name);
    println!("Pending messages: {}", info.num_pending);
    println!("Awaiting ACK: {}", info.num_ack_pending);
    println!("Redelivered messages: {}", info.num_redelivered);

    Ok(())
}
