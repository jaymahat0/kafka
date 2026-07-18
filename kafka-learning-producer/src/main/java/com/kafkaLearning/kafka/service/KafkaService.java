package com.kafkaLearning.kafka.service;

import com.kafkaLearning.kafka.model.Course;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private final KafkaTemplate<String, Course> kafkaTemplate;

    public KafkaService(KafkaTemplate<String, Course> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String sendMessage(Course course) {
        try {
            kafkaTemplate.send("mahato", "course", course).get();
            return "Message sent successfully.";
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to send message: " + e.getMessage();
        }
    }
}
