package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args)
    {
        LOGGER.info("Kafka With Callback loading ...");
        PropertiesLoader propertiesLoader = new PropertiesLoader();
        propertiesLoader.setProperty("batch.size", "400"); // to set you batch size default is 600
//      this would make batch to go to different partitioner, but it is not recommended
//        propertiesLoader.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        try
        {
            // create the Producer
            KafkaProducer<String, String> producer = new KafkaProducer<>(propertiesLoader.getKafkaProducerProperties());
            for (int j = 0; j < 2; j++) {
                //create and produce record
                for (int i = 0; i < 5; i++) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Testing Refactoring - with callback_" + i + ":" + j);
                    // send dato to kafka cluster
                    producer.send(producerRecord, new Callback()
                    {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception)
                        {
                            // executed every time a record successfully sent or an exception is thrown
                            if (exception == null) {
                                // the record was successfully sent
                                LOGGER.info("Receive new megadata \n" +
                                        "Topic: " +  metadata.topic() + "\n" +
                                        "Partition: " +  metadata.partition() + "\n" +
                                        "Offset: " +  metadata.offset() + "\n" +
                                        "Timestamp: " +  metadata.timestamp()
                                );
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
