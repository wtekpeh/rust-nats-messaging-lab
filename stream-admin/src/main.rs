use async_nats::jetstream::{self, stream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    Ok(())
}
