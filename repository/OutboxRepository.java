package ru.mephi.kafka.example.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import ru.mephi.kafka.example.model.OutboxMessage;
import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {
    @Query("SELECT m FROM OutboxMessage m WHERE m.published = false ORDER BY m.createdAt ASC")
    List<OutboxMessage> findUnpublishedMessages();
}
