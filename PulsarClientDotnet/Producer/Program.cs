using System.Diagnostics;
using Pulsar.Client.Api;

const string serviceUrl = "pulsar://127.0.0.1:6650";
var topicName = $"my-topic-{DateTime.Now.Ticks}";

var client = await new PulsarClientBuilder()
    .ServiceUrl(serviceUrl)
    .BuildAsync();

var producer =
    await client.NewProducer()
        .Topic(topicName)
        .BlockIfQueueFull(true)
        .CreateAsync();

var n = 10000000;
var bytes = new byte[750];

// warmup
for (var i = 0; i < n / 10; i++)
{
    await producer.SendAndForgetAsync(bytes);
}

var sw = new Stopwatch();
sw.Start();
for (var i = 0; i < n-1; i++)
{
    await producer.SendAndForgetAsync(bytes);

    if (i % 100000 == 0)
        Console.WriteLine($"Sent {i} messages");
}
await producer.SendAsync(bytes);
sw.Stop();
Console.WriteLine(
    $"Sent {n} messages in {sw.ElapsedMilliseconds}ms. Speed: {n / (sw.ElapsedMilliseconds)}K msg/s");