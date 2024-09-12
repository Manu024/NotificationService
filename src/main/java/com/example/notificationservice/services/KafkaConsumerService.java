package com.example.notificationservice.services;

import com.example.notificationservice.dtos.EmailDto;
import com.example.notificationservice.dtos.OrderCreationEmailDto;
import com.example.notificationservice.dtos.UserSignInEmailDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private ObjectMapper objectMapper;

    public KafkaConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "user-sign-in", groupId = "user-auth")
    public void consumeUserSignInMail(ConsumerRecord<String, String> record) {
        try {
            UserSignInEmailDto userSignInEmailDto = (UserSignInEmailDto) objectMapper.readValue(record.value(), UserSignInEmailDto.class);
            System.out.println("Consumed: " + userSignInEmailDto.getFrom() + " " + userSignInEmailDto.getTo() + " " + userSignInEmailDto.getMessage());
        } catch (Exception e) {
            System.out.println("Error consuming msg from kafka");
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "order-create", groupId = "order")
    public void consumeOrderCreatedEmail(ConsumerRecord<String, String> record) {
        System.out.println("Consumed: " + record.value());
        try {
            EmailDto emailDto = objectMapper.readValue(record.value(), OrderCreationEmailDto.class);
        } catch (Exception e) {
            System.out.println("Error consuming msg from kafka");
            throw new RuntimeException(e);
        }
    }
}
