package com.example.kafka_producer_example.service;

import com.example.kafka_producer_example.model.Greeting;
import com.example.kafka_producer_example.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {


    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);
    private final KafkaTemplate<String, Object> template;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void sendMessageToTopic(User user) {
        CompletableFuture<SendResult<String, Object>> future = template.send("user", user.getName(), user);
        future.whenComplete((result, exception) -> {
            if (result != null) {

                log.info("Sent user=[ {} ] with offset= {} on partition= {}", user.getName(), result.getRecordMetadata().offset(), result.getProducerRecord().partition());
            } else {
                log.error("Exception occurred while sending user: {}", exception.getMessage(), exception);
            }
        });

//        Below method will send messages to partition 2 only.
//        template.send("demo-topic", 2, "key", user);
    }

    public void sendMessageToTopicGreeting(Greeting greeting) {
        CompletableFuture<SendResult<String, Object>> future = template.send("greeting-1", greeting.getFrom(), greeting);
        future.whenComplete((result, exception) -> {
            if (result != null) {
                log.info("Sent greeting from=[ {} ], to= [ {} ] with offset= {} on partition= {}", greeting.getFrom(), greeting.getTo(), result.getRecordMetadata().offset(), result.getProducerRecord().partition());
            } else {
                log.error("Exception occurred while sending greeting: {}", exception.getMessage(), exception);
            }
        });
    }
}
