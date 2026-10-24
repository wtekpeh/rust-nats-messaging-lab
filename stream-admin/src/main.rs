use async_nats::jetstream::{self, consumer, stream};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Connect to the NATS server.
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("Connected to NATS");

    // 2. Create a JetStream context.
    let jetstream = jetstream::new(client);

    // 3. Create or retrieve our GPS stream.
    let mut gps_stream = jetstream
        .get_or_create_stream(stream::Config {
            name: "ORBI_GPS".to_string(),
            subjects: vec!["orbi.gps.location".to_string()],
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;

    println!("JetStream stream ready: ORBI_GPS");

    // 4. Retrieve the stream's information.
    let info = gps_stream.info().await?;

    println!("Stream name: {}", info.config.name);
    println!("Stored messages: {}", info.state.messages);

    // Create or retrieve the Dead-Letter Stream.
    let mut dlq_stream = jetstream
        .get_or_create_stream(stream::Config {
            name: "ORBI_GPS_DLQ".to_string(),
            subjects: vec!["orbi.gps.dlq".to_string()],
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;

    println!("Dead-letter stream ready: ORBI_GPS_DLQ");

    // Retrieve DLQ stream information.
    let dlq_info = dlq_stream.info().await?;

    println!("DLQ stream name: {}", dlq_info.config.name);
    println!("DLQ subjects: {:?}", dlq_info.config.subjects);
    println!("DLQ stored messages: {}", dlq_info.state.messages);

    // Persist JetStream maximum-deliveries advisories.
    let mut advisory_stream = jetstream
        .get_or_create_stream(stream::Config {
            name: "ORBI_ADVISORIES".to_string(),
            subjects: vec![
                "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORBI_GPS.GPS_PROCESSOR".to_string(),
            ],
            storage: stream::StorageType::File,
            ..Default::default()
        })
        .await?;

    println!("Advisory stream ready: ORBI_ADVISORIES");

    let advisory_info = advisory_stream.info().await?;

    println!("Advisory stream name: {}", advisory_info.config.name);
    println!("Advisory subjects: {:?}", advisory_info.config.subjects);
    println!(
        "Stored failure advisories: {}",
        advisory_info.state.messages
    );

    // 5. Configure the durable GPS pull consumer.
    let consumer_config = consumer::pull::Config {
        durable_name: Some("GPS_PROCESSOR".to_string()),
        ack_policy: consumer::AckPolicy::Explicit,
        max_deliver: 3,
        backoff: vec![Duration::from_secs(5), Duration::from_secs(10)],
        ..Default::default()
    };

    // 6. Create or update the existing durable consumer.
    let mut gps_consumer = gps_stream.create_consumer(consumer_config).await?;

    // 7. Verify the configuration returned by JetStream.
    let consumer_info = gps_consumer.info().await?;

    println!("Consumer ready: {}", consumer_info.name);
    println!("ACK policy: {:?}", consumer_info.config.ack_policy);
    println!("Max deliveries: {}", consumer_info.config.max_deliver);
    println!("Backoff schedule: {:?}", consumer_info.config.backoff);

    // Create or update the durable consumer for failure advisories.
    let mut dlq_consumer = advisory_stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some("GPS_DLQ_HANDLER".to_string()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            max_deliver: 5,
            ..Default::default()
        })
        .await?;

    // Verify the DLQ handler consumer configuration.
    let dlq_consumer_info = dlq_consumer.info().await?;

    println!("DLQ handler consumer: {}", dlq_consumer_info.name);
    println!(
        "DLQ handler ACK policy: {:?}",
        dlq_consumer_info.config.ack_policy
    );
    println!(
        "DLQ handler maximum deliveries: {}",
        dlq_consumer_info.config.max_deliver
    );

    println!(
        "DLQ handler pending messages: {}",
        dlq_consumer_info.num_pending
    );

    println!(
        "DLQ handler awaiting ACK: {}",
        dlq_consumer_info.num_ack_pending
    );

    println!(
        "DLQ handler redelivered messages: {}",
        dlq_consumer_info.num_redelivered
    );

    Ok(())
}
