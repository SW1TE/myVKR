package ru.mephi.kafka.example.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.mephi.kafka.example.model.ProcessedEvent;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
    boolean existsByEventId(String eventId);
}
