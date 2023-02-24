package io.conductor.demos.kafka;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    private static final String TOPIC = "demo_java";
    public static void main(String[] args)
    {
        LOGGER.info("Kafka loading ...");
        // create / producer properties
        LOGGER.info("Kafka Consumer with shutdown loading ...");
        PropertiesLoader propertiesLoader = new PropertiesLoader();

        try
        {
            // create the Consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(propertiesLoader.getKafkaConsumerProperties());

            // get reference to main thread
            final Thread mainThread  = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LOGGER.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
                    consumer.wakeup();

                    // join the main thread to allow the execution of the code in the main thread
                    try {
                        mainThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            try
            {
                // subscribe to a topic (topics)
                consumer.subscribe(Arrays.asList(TOPIC));
                // poll data
                while (true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records)
                    {
                        LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                        LOGGER.info("Partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                LOGGER.info("Consumer is starting to shut down");

            } catch (Exception e) {
                LOGGER.error("Unexpected exception in the consumer" , e);
            } finally
            {
                consumer.close(); // close the consumer, this will also commit offset
                LOGGER.info("The consumer is now gracefully shut down");
            }
        }
        catch ( Exception e) {
            LOGGER.error("Internal error"  + e);
        }
    }
}
