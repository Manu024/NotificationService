package com.example.notificationservice.dtos;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserSignInEmailDto {
    private String from;
    private String to;
    private String message;

    public UserSignInEmailDto(String from, String to, String message) {
        this.from = from;
        this.to = to;
        this.message = message;
    }

    public UserSignInEmailDto() {
    }
}