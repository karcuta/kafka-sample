package com.vnbdc.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Topic name is required");
            System.exit(1);
        }
        final String topic = args[0];
        final String brokers = args.length >= 2 ? args[1] : "localhost:9092";

        Properties settings = new Properties();

        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(settings);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(),
                        record.offset(), record.key(), record.value());
        }
    }
}
