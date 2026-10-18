use async_nats::jetstream::{self, consumer};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connect to the NATS server.
    let client = async_nats::connect("127.0.0.1:4222").await?;
    println!("Connected to NATS");

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
    let mut messages = consumer.fetch().max_messages(1).messages().await?;

    if let Some(message_result) = messages.next().await {
        let message = message_result?;

        println!("Received GPS message:");
        println!("{}", String::from_utf8_lossy(&message.payload));

        // Acknowledge successful processing.
        message.ack().await?;

        println!("GPS message acknowledged successfully.");

        // Refresh the consumer state after acknowledging.
        let updated_info = consumer.info().await?;

        println!("--- Consumer state after ACK ---");
        println!("Pending messages: {}", updated_info.num_pending);
        println!("Awaiting ACK: {}", updated_info.num_ack_pending);
        println!("Redelivered messages: {}", updated_info.num_redelivered);
    }

    Ok(())
}
