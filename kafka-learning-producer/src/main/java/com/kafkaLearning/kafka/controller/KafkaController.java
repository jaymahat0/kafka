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

    @GetMapping
    public String test() {
        return "Running UP";
    }

    @PostMapping("/add-course")
    public ResponseEntity<String> addCourse(@RequestBody Course course) {
        String message = kafkaService.sendMessage(course);
        return ResponseEntity.ok().body(
                message
        );
    }
}
