package com.kafkaLearning.kafka.service;

import com.kafkaLearning.kafka.model.Course;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private String message = "Message not received yet";


    @KafkaListener(topics="mahato", groupId = "mahato-group")
    public void consume(Course course) {
        System.out.println("Received: " + course);
        message = "Received: " + course;
    }

    public String getMessage() {
        return message;
    }
}
