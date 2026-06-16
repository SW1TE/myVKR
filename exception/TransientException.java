package ru.mephi.kafka.example.exception;

public class TransientException extends RuntimeException {
    public TransientException(String message) {
        super(message);
    }
}
