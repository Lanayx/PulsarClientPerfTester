package org.apache.pulsar.testclient;

import java.io.IOException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

public class SampleConsumer {
    public static void main(String[] args) throws InterruptedException, IOException {
        PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://127.0.0.1:6650").build();

        Consumer<byte[]> consumer = client
            .newConsumer()
            .topic("non-persistent://public/default/test7")
            .subscriptionName("test7")
            .subscribe();

        long start = System.nanoTime();
        int n = 10000000;
        int i = 0;        
        // warmup
        while(i < n/10) {
            Message<byte[]> msg = consumer.receive();
            consumer.acknowledge(msg);
            i++;
        }
        //warmup end
        var j = 0;
        while(j < n) {
            if (j % (n/100) == 0)
            {
                System.out.println("Received " + j + " messages");
            }
            Message<byte[]> msg = consumer.receive();
            consumer.acknowledge(msg);
            j++;
        }
        long end = System.nanoTime();
        long elapsedMs = (end - start) / 1000000;
        System.out.println("Time taken: " + elapsedMs + "ms.Speed: " + n / elapsedMs  + "K msg/s");
    }
}