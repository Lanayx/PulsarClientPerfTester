using System.Diagnostics;
using DotPulsar.Extensions;
using DotPulsar.Internal;

const string serviceUrl = "pulsar://127.0.0.1:6650";
const string subscriptionName = "my-subscription";
var topicName = $"my-topic-{DateTime.Now.Ticks}";

var client = new PulsarClientBuilder()
    .ServiceUrl(new Uri(serviceUrl))
    .Build();

var consumer = client.NewConsumer()
    .Topic(topicName)
    .SubscriptionName(subscriptionName)
    .Create();

var n = 10000000;

// warmup
for (var i = 0; i < n / 10; i++)
{
    var message = await consumer.Receive();
    await consumer.Acknowledge(message);
}

var sw = new Stopwatch();
sw.Start();
for (var i = 0; i < n; i++)
{
    var message = await consumer.Receive();
    await consumer.Acknowledge(message);

    if (i % 100000 == 0)
        Console.WriteLine($"Received {i} messages");
}

sw.Stop();
Console.WriteLine(
    $"Received {n} messages in {sw.ElapsedMilliseconds}ms. Speed: {n / (sw.ElapsedMilliseconds)}K msg/s");