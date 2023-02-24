package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args)
    {
        LOGGER.info("Kafka With Keys loading ...");
        PropertiesLoader propertiesLoader = new PropertiesLoader();
        try
        {
            // create the Producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesLoader.getKafkaProducerProperties());
            for (int j = 0; j < 2; j++) {
                //create and produce record
                for (int i = 0; i < 10; i++) {
                    String topic = "demo_java";
                    String key = "id_" + i;
                    String value = "Keys base message " + i;
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                    // send dato to kafka cluster
                    producer.send(producerRecord, new Callback()
                    {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception)
                        {
                            // executed every time a record successfully sent or an exception is thrown
                            if (exception == null) {
                                // the record was successfully sent
                                LOGGER.info("Key: " +  key + " | Partition: " +  metadata.partition());
                            } else {
                                LOGGER.error("Error while producing: ", exception);
                            }
                        }
                    });
                }
                Thread.sleep(500);
            }
            // tell the producer to send all data and block until done --synchronous
            producer.flush();
            // flash and close the producer
            producer.close();
            LOGGER.info("Kafka With Callback Closing ...");
        }
        catch ( Exception e) {
            LOGGER.error("Internal error ", e);
        }
    }
}

