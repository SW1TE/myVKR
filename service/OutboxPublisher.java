package ru.mephi.kafka.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.mephi.kafka.example.model.OutboxMessage;
import ru.mephi.kafka.example.repository.OutboxRepository;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000) // каждые 5 секунд
    @Transactional("transactionManager")
    public void publishUnsentMessages() {
        List<OutboxMessage> messages = outboxRepository.findUnpublishedMessages();
        for (OutboxMessage msg : messages) {
            try {
                // Отправляем в Kafka с ключом (для сохранения порядка в партиции)
                kafkaTemplate.send(msg.getTopic(), msg.getKey(), msg.getPayload())
                        .addCallback(
                                result -> {
                                    msg.setPublished(true);
                                    outboxRepository.save(msg);
                                    log.debug("Сообщение {} отправлено", msg.getId());
                                },
                                ex -> log.error("Ошибка отправки {}: {}", msg.getId(), ex.getMessage())
                        );
            } catch (Exception e) {
                log.error("Не удалось отправить сообщение {}: {}", msg.getId(), e.getMessage());
            }
        }
    }
}
