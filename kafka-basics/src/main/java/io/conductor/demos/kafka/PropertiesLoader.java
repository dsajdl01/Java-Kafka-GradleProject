package io.conductor.demos.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoader
{
    private Properties properties;

    private final String BOOTSTRAP_SERVERS_VALUE = "*****.******.******.**:9092";
    private final String SECURITY_PROTOCOL_VALUE = "********";
    private final String SASL_JAAS_CONFIG_VALUE = "******************************";
    private final String SASL_MECHANISM_VALUE = "****";
    private final String GROUP_ID = "java-application";
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class.getSimpleName());

    public PropertiesLoader() {
        LOGGER.info("Loading Properties ...");
        this.loadProperties();
    }

    public Properties getKafkaProducerProperties() {
        // set producer config
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
    public Properties getKafkaConsumerProperties() {
        // set consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", this.GROUP_ID);
        // to set auto.offset.reset it has 3 values none/earliest/latest
        // none - means we need to set up group id from starter
        // earliest - means to read from beginning
        // latest - means to get latest message
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    private void loadProperties() {
        // create / producer properties
        properties = new Properties();
        // connect kafka to localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect kafka to Conductor playground
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS_VALUE);
        properties.setProperty("security.protocol", SECURITY_PROTOCOL_VALUE);
        properties.setProperty("sasl.jaas.config", SASL_JAAS_CONFIG_VALUE);
        properties.setProperty("sasl.mechanism", SASL_MECHANISM_VALUE);
    }
}
