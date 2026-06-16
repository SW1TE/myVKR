package ru.mephi.kafka.example.exception;

public class NonRecoverableException extends RuntimeException {
    public NonRecoverableException(String message) {
        super(message);
    }
}
