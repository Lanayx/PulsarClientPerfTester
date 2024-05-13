use std::time::Instant;
use futures::{TryStreamExt};
use pulsar::{
    message::{proto::command_subscribe::SubType},
    Consumer, Pulsar, TokioExecutor,
};


#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    let now = Instant::now();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<_> = Pulsar::builder(addr, TokioExecutor).build().await?;

    let mut consumer: Consumer<Vec<u8>, _> = pulsar
        .consumer()
        .with_topic("test")
        .with_consumer_name("test_consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("test_subscription")
        .build()
        .await?;

    let n = 10000000;
    let mut i = 0;
    while i<n/10 {
        let msg = consumer.try_next().await?.unwrap();
        consumer.ack(&msg).await?;

        i += 1;
    }

    i = 0;
    while i<n {
        let msg = consumer.try_next().await?.unwrap();
        consumer.ack(&msg).await?;

        i += 1;
        if i % 100000 == 0 {
            println!("received {i} messages");
        }
    }

    let elapsed = now.elapsed().as_millis();
    let speed = n/elapsed;

    println!(
        "Received {n} messages in {elapsed}ms. Speed: {speed}K msg/s");

    Ok(())
}