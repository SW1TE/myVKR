package ru.mephi.kafka.example.model;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "outbox")
public class OutboxMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String topic;
    private String key;          // ключ сообщения (orderId)
    @Column(columnDefinition = "TEXT")
    private String payload;      // сериализованный JSON события
    private LocalDateTime createdAt;
    private boolean published;

    // геттеры и сеттеры
}
