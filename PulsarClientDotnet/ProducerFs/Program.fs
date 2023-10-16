open System.Diagnostics;
open Pulsar.Client.Api
open System

let serviceUrl = "pulsar://127.0.0.1:6650";
let topicName = $"my-topic-{DateTime.Now.Ticks}"


(task {
    let! client =
        PulsarClientBuilder()
            .ServiceUrl(serviceUrl)
            .BuildAsync();

    let! producer =
        client.NewProducer()
            .Topic(topicName)
            .BlockIfQueueFull(true)
            .CreateAsync();

    let n = 10000000
    let bytes: byte[] = Array.zeroCreate 750

    // warmup
    for i in 0..n/10 do
        let! _ = producer.SendAndForgetAsync(bytes)
        ()

    let sw = new Stopwatch();
    sw.Start();
    for i in 0..n do
        let! _ = producer.SendAndForgetAsync(bytes)

        if (i % 100000 = 0) then
            Console.WriteLine($"Sent {i} messages");

    sw.Stop();
    Console.WriteLine(
        $"Sent {n} messages in {sw.ElapsedMilliseconds}ms. Speed: {n / (int sw.ElapsedMilliseconds)}K msg/s");

}).Result

