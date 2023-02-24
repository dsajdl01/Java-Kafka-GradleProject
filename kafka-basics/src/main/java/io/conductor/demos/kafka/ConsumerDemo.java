package io.conductor.demos.kafka;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    private static final String TOPIC = "demo_java";
    public static void main(String[] args)
    {
        LOGGER.info("Kafka loading ...");
        // create / producer properties
        LOGGER.info("Kafka Consumer loading ...");
        PropertiesLoader propertiesLoader = new PropertiesLoader();

        try
        {
            // create the Consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesLoader.getKafkaConsumerProperties());
            // subscribe to a topic (topics)
            consumer.subscribe(Arrays.asList(TOPIC));
            // poll data
            while (true) {
                LOGGER.info("POLLING");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record: records) {
                    LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                    LOGGER.info("Partition: " + record.partition() + ", offset: " + record.offset());
                }
            }

//            LOGGER.info("Kafka closing ...");
        }
        catch ( Exception e) {
            LOGGER.error("Internal error"  + e);
        }
    }
}
