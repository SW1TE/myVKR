package ru.mephi.kafka.example.model;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {
    @Id
    private String eventId;
    private LocalDateTime processedAt;

    public ProcessedEvent() {}
    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = LocalDateTime.now();
    }
    // геттеры и сеттеры
}
