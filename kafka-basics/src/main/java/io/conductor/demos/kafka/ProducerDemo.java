package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

public class ProducerDemo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args)
    {
        LOGGER.info("Kafka loading ...");
        // create / producer properties
        LOGGER.info("Kafka With Callback loading ...");
        PropertiesLoader propertiesLoader = new PropertiesLoader();

        try
        {
            // create the Producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesLoader.getKafkaProducerProperties());
            //create and produce record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
            // send dato to kafka cluster
            producer.send(producerRecord);

            // tell the producer to send all data and block until done --synchronous
            producer.flush();
            // flash and close the producer
            producer.close();
            LOGGER.info("Kafka closing ...");
        }
        catch ( Exception e) {
            LOGGER.error("Internal error"  + e);
        }
    }
}
