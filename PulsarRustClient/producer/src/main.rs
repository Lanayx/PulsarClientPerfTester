use std::time::Instant;
use pulsar::{
    Pulsar, TokioExecutor,
};


#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    let now = Instant::now();

    let addr = "pulsar://127.0.0.1:6650";
    let pulsar: Pulsar<TokioExecutor> = Pulsar::builder(addr, TokioExecutor).build().await?;
    let mut producer = pulsar
        .producer()
        .with_topic("non-persistent://public/default/test")
        .with_name("my producer")
        .build()
        .await?;

    let data = Vec::<u8>::with_capacity(750).truncate(750);


    let n = 1000000;
    let mut i = 0;
    while i<n/10 {
        producer.send(data).await?;
        i += 1;
    }

    i = 0;
    while i<n {
        producer.send(data).await?;
        i += 1;
        if i % 100000 == 0 {
            println!("sent {i} messages");
        }
    }

    let elapsed = now.elapsed().as_millis();
    let speed = n/elapsed;

    println!(
        "Sent {n} messages in {elapsed}ms. Speed: {speed}K msg/s");

    Ok(())
}