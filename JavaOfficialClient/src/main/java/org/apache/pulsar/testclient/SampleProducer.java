package org.apache.pulsar.testclient;

import java.io.IOException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

public class SampleProducer {
    public static void main(String[] args) throws InterruptedException, IOException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();

        Producer<byte[]> producer = client
            .newProducer()
            .topic("non-persistent://public/default/test7")
            .blockIfQueueFull(true)
            .create();

        byte[] array = new byte[750];
        
        int n = 10000000;
        int i = 0;
        // warmup
        while(i < n/10) {
            producer.sendAsync(array);
            i++;
        }
        producer.sendAsync(array).thenRun(() -> {            
            
            //warmup end
            var j = 0;
            long start = System.nanoTime();
            while(j  < n-1) {
                if (j  % (n/100) == 0)
                {
                    System.out.println("Sent " + j  + " messages");
                }
                producer.sendAsync(array);
                j ++;
            }
            producer.sendAsync(array).thenRun(() -> {
                long end = System.nanoTime();
                long elapsedMs = (end - start) / 1000000;
                System.out.println("Time taken: " + elapsedMs + "ms.Speed: " + n / elapsedMs  + "K msg/s");
            });                
        });
        
    }
}

