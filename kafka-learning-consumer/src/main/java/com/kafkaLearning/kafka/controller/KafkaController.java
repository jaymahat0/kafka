package com.kafkaLearning.kafka.controller;

import com.kafkaLearning.kafka.model.Course;
import com.kafkaLearning.kafka.service.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaService kafkaService;

    @GetMapping("/message")
    public ResponseEntity<String> addCourse() {
        String message = kafkaService.getMessage();
        return ResponseEntity.ok().body(
                message
        );
    }
}
