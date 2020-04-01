package com.vnbdc.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Topic name is required");
            System.exit(1);
        }

        final String topic = args[0];
        final String brokers = args.length >= 2 ? args[1] : "localhost:9092";

        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        final Producer<String, String> producer = new KafkaProducer<>(settings);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("### Stopping Simple Producer ###");
            producer.close();
        }));

        String[] data = {"APP,Galaxy S10", "WEB,iPhone 11 Pro Max", "WEB, iPhone X",
                "WEB,Galaxy S20 Ultra", "WEB,Galaxy 20+"};
        for (int i = 0; i < data.length; i++) {
            producer.send(new ProducerRecord<>(topic, String.valueOf(i), data[i]));
            System.out.println(data[i]);
        }

    }
}
