package com.example.kafka_producer_example.controller;

import com.example.kafka_producer_example.model.Greeting;
import com.example.kafka_producer_example.model.User;
import com.example.kafka_producer_example.service.KafkaMessagePublisher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/produce")
public class KafkaProducerController {

    private final KafkaMessagePublisher service;

    public KafkaProducerController(KafkaMessagePublisher service){
        this.service = service;
    }

    @GetMapping
    public ResponseEntity<String> publishMessage(@RequestBody User message) {
        try {
            service.sendMessageToTopic(message);
            return ResponseEntity.ok("Message sent for publishing....");
        } catch (RuntimeException re) {
            System.out.println(re.getMessage());
            return ResponseEntity.status(500).body(re.getMessage());
        }
    }

    @GetMapping("/greeting")
    public ResponseEntity<String> publishMessage(@RequestBody Greeting message) {
        try {
            service.sendMessageToTopicGreeting(message);
            return ResponseEntity.ok("Greeting sent for publishing....");
        } catch (RuntimeException re) {
            System.out.println(re.getMessage());
            return ResponseEntity.status(500).body(re.getMessage());
        }
    }

}
