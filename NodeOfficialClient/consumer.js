const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: "pulsar://127.0.0.1:6650",
    log: (level, file, line, message) => level>=Pulsar.LogLevel.INFO && console.log(`[${level}] ${file}:${line} ${message}`)
  });

  const consumer = await client.subscribe({
    topic: 'my-topic', 
    subscription: "test7"
  });

  const n = 10000
  let i = 0;
  // warmup
  while(i < n/10) {
    const msg = await consumer.receive()
    await consumer.acknowledge(msg)
    i ++
  }

  let start = performance.now()
  let j = 0;
  while(j < n) {    
    const msg = await consumer.receive()
    await consumer.acknowledge(msg)
    if (j  % (n/100) == 0)
    {
        console.log("Received " + j  + " messages");
    }
    j ++
  }
  let elapsedMs = performance.now() - start
  console.log("Time taken: " + elapsedMs + "ms.Speed: " + n / elapsedMs  + "K msg/s")
  
})();