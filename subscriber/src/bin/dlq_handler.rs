use async_nats::jetstream;
use futures_util::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct MaxDeliverAdvisory {
    #[serde(rename = "type")]
    advisory_type: String,
    id: String,
    timestamp: String,
    stream: String,
    consumer: String,
    stream_seq: u64,
    deliveries: u64,
}

#[derive(Debug, serde::Serialize)]
struct DeadLetterRecord {
    dlq_id: String,
    original_stream: String,
    original_subject: String,
    original_sequence: u64,
    consumer: String,
    failure_reason: String,
    delivery_attempts: u64,
    failed_at: String,
    original_event: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Connect to NATS.
    let client = async_nats::connect("127.0.0.1:4222").await?;

    println!("DLQ handler connected to NATS");

    // Create JetStream context.
    let jetstream = jetstream::new(client);

    // Retrieve the persistent advisory stream.
    let advisory_stream = jetstream.get_stream("ORBI_ADVISORIES").await?;

    println!("Connected to advisory stream: ORBI_ADVISORIES");

    // Retrieve information about the advisory stream.
    let advisory_info = advisory_stream.get_info().await?;

    println!(
        "Current advisory message count: {}",
        advisory_info.state.messages
    );

    // Retrieve the dead-letter stream.
    let dlq_stream = jetstream.get_stream("ORBI_GPS_DLQ").await?;

    println!("Connected to dead-letter stream: ORBI_GPS_DLQ");

    let dlq_info = dlq_stream.get_info().await?;

    println!("Current DLQ message count: {}", dlq_info.state.messages);

    // Retrieve the existing durable advisory consumer.
    let consumer = advisory_stream
        .get_consumer::<jetstream::consumer::pull::Config>("GPS_DLQ_HANDLER")
        .await?;

    println!("Connected to durable consumer: GPS_DLQ_HANDLER");

    // Fetch up to one advisory for inspection.
    let mut messages = consumer.fetch().max_messages(1).messages().await?;

    while let Some(message_result) = messages.next().await {
        let message = message_result?;

        println!("--- Maximum-deliveries advisory received ---");

        let payload = std::str::from_utf8(&message.payload)?;

        println!("Advisory payload: {payload}");

        let advisory: MaxDeliverAdvisory = serde_json::from_str(payload)?;

        println!("Advisory type: {}", advisory.advisory_type);
        println!("Advisory ID: {}", advisory.id);
        println!("Timestamp: {}", advisory.timestamp);
        println!("Original stream: {}", advisory.stream);
        println!("Original consumer: {}", advisory.consumer);
        println!("Original message sequence: {}", advisory.stream_seq);
        println!("Delivery attempts: {}", advisory.deliveries);

        // Retrieve the original stream containing the failed GPS message.
        let original_stream = jetstream.get_stream(&advisory.stream).await?;

        // Retrieve the original message using its stream sequence number.
        let original_message = original_stream.get_raw_message(advisory.stream_seq).await?;

        println!("--- Original failed GPS message ---");

        println!("Original subject: {}", original_message.subject);
        println!("Original sequence: {}", original_message.sequence);

        // Convert the original payload into readable text.
        let original_payload = std::str::from_utf8(&original_message.payload)?;

        println!("Original GPS payload: {original_payload}");

        // Create a stable identifier for this failed stream message.
        let dlq_message_id = format!(
            "dlq:{}:{}:{}",
            advisory.stream, advisory.consumer, advisory.stream_seq
        );

        println!("DLQ message ID: {dlq_message_id}");

        // Deserialize the original GPS payload into a JSON value.
        let original_event: serde_json::Value = serde_json::from_slice(&original_message.payload)?;

        // Construct the structured dead-letter record.
        let dlq_record = DeadLetterRecord {
            dlq_id: dlq_message_id.clone(),
            original_stream: advisory.stream.clone(),
            original_subject: original_message.subject.to_string(),
            original_sequence: advisory.stream_seq,
            consumer: advisory.consumer.clone(),
            failure_reason: "MAX_DELIVERIES_EXCEEDED".to_string(),
            delivery_attempts: advisory.deliveries,
            failed_at: advisory.timestamp.clone(),
            original_event,
        };

        // Serialize the structured record into JSON bytes.
        let dlq_payload = serde_json::to_vec(&dlq_record)?;

        println!("--- Structured dead-letter record ---");
        println!("{}", serde_json::to_string_pretty(&dlq_record)?);

        // Attach a deterministic message ID for JetStream deduplication.
        let mut headers = async_nats::HeaderMap::new();

        headers.insert("Nats-Msg-Id", dlq_message_id.as_str());

        // Publish the serialized DeadLetterRecord, which includes the GPS telemetry and its failure metadata.
        let publish_ack = jetstream
            .publish_with_headers("orbi.gps.dlq", headers, dlq_payload.into())
            .await?
            .await?;
        // Confirm that JetStream accepted the publication.
        println!("Failed GPS event published to DLQ.");
        println!("DLQ stream: {}", publish_ack.stream);
        println!("DLQ sequence: {}", publish_ack.sequence);
        println!("Duplicate publication: {}", publish_ack.duplicate);

        // Acknowledge the advisory only after the DLQ publish succeeds.
        message.double_ack().await?;

        println!("Advisory acknowledgment confirmed.");
    }

    Ok(())
}
