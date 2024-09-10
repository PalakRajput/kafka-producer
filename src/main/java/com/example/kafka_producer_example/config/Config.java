package com.example.kafka_producer_example.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to create kafka topic programmatically.
 */
@Configuration
public class Config {
    /*@Bean
    public NewTopic createTopic() {
        return new NewTopic("demo", 3, (short) 1);
    }*/

    /**
     * Instead of giving these values in application.properties/yml file. We can create the configuration programmatically.
     *
     * @return map of properties
     */
    @Bean
    public Map<String, Object> producerConfig() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(JsonSerializer.TYPE_MAPPINGS, "user:com.example.kafka_producer_example.model.User");
//        here we can also add username and password
//        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
//        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//        properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
//                "%s required username=\"" + "saslUsername" + "\" password=\"" + "saslPassword" + "\";", PlainLoginModule.class.getName(), "username", "password"
//        ));
//        security.protocol=SASL_PLAINTEXT
//        sasl.mechanism=PLAIN
//        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="alice";

        return properties;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }
}
