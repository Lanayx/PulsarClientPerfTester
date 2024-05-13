const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: "pulsar://127.0.0.1:6650",
    log: (level, file, line, message) => level>=Pulsar.LogLevel.WARN && console.log(`[${level}] ${file}:${line} ${message}`)
  });

  const producer = await client.createProducer({
    topic: 'my-topic', 
    blockIfQueueFull: true
  });
  
  const data = Buffer.alloc(750)

  const n = 10000
  let i = 0;
  // warmup
  while(i < n/10) {
    producer.send({
        data
    })
    i ++
  }
  await producer.send({data})

  let start = performance.now()
  let j = 0;
  while(j < n-1) {
    producer.send({
        data
    })
    j ++
    if (j  % (n/100) == 0)
    {
        console.log("Sent " + j  + " messages");
    }
  }
  await producer.send({data})
  let elapsedMs = performance.now() - start
  console.log("Time taken: " + elapsedMs + "ms.Speed: " + n / elapsedMs  + "K msg/s")
  
})();