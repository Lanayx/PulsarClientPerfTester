using System.Diagnostics;
using Pulsar.Client.Api;

const string serviceUrl = "pulsar://127.0.0.1:6650";
const string subscriptionName = "my-subscription";
var topicName = $"my-topic-{DateTime.Now.Ticks}";

var client = await new PulsarClientBuilder()
    .ServiceUrl(serviceUrl)
    .BuildAsync();

var consumer = await client.NewConsumer()
    .Topic(topicName)
    .SubscriptionName(subscriptionName)
    .SubscribeAsync();

var n = 10000000;

// warmup
for (var i = 0; i < n / 10; i++)
{
    var message = await consumer.ReceiveAsync();
    await consumer.AcknowledgeAsync(message.MessageId);
}

var sw = new Stopwatch();
sw.Start();
for (var i = 0; i < n; i++)
{
    var message = await consumer.ReceiveAsync();
    await consumer.AcknowledgeAsync(message.MessageId);

    if (i % 100000 == 0)
        Console.WriteLine($"Received {i} messages");
}

sw.Stop();
Console.WriteLine(
    $"Received {n} messages in {sw.ElapsedMilliseconds}ms. Speed: {n / (sw.ElapsedMilliseconds)}K msg/s");