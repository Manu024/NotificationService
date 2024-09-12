package com.example.notificationservice.dtos;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class EmailDto {
    private String from;
    private String to;
    private String message;
}