package com.de314.kdt.models;

import lombok.Data;
import org.springframework.http.HttpStatus;

@Data
public class RestException extends Exception {

    private HttpStatus status;

    public RestException(Exception e, HttpStatus status) {
        super(e.getMessage(), e);
        this.status = status;
    }

    public RestException(String message, HttpStatus status) {
        super(message);
        this.status = status;
    }
}
