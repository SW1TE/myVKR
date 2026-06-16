package ru.mephi.kafka.example.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.mephi.kafka.example.exception.NonRecoverableException;
import ru.mephi.kafka.example.exception.TransientException;
import ru.mephi.kafka.example.model.OrderCreatedEvent;
import ru.mephi.kafka.example.model.ProcessedEvent;
import ru.mephi.kafka.example.repository.ProcessedEventRepository;
import ru.mephi.kafka.example.service.BusinessLogicService;

import java.io.IOException;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final ProcessedEventRepository processedEventRepository;
    private final BusinessLogicService businessLogicService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order-created", groupId = "order-processor")
    @Transactional("transactionManager")
    public void consume(ConsumerRecord<String, String> record) {
        String payload = record.value();
        OrderCreatedEvent event;
        try {
            event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        } catch (IOException e) {
            log.error("Невозможно десериализовать сообщение: {}", e.getMessage());
            // Отправляем в DLQ, если сообщение нечитаемо
            kafkaTemplate.send("order-created.DLT", record.key(), payload);
            return;
        }

        // 1. Проверка дубликата
        if (processedEventRepository.existsByEventId(event.getEventId())) {
            log.info("Дубликат события {}, пропускаем", event.getEventId());
            return; // offset будет зафиксирован после выхода из метода
        }

        try {
            // 2. Выполнение бизнес-логики
            businessLogicService.processOrder(event);

            // 3. Запись факта успешной обработки
            processedEventRepository.save(new ProcessedEvent(event.getEventId()));

        } catch (TransientException e) {
            log.warn("Временная ошибка, будет повторная попытка: {}", e.getMessage());
            // Бросаем исключение, чтобы контейнер Kafka не фиксировал смещение
            throw e;

        } catch (NonRecoverableException e) {
            log.error("Фатальная ошибка, перенос в DLQ: {}", e.getMessage());
            // Отправляем в Dead Letter Queue и фиксируем факт обработки, чтобы не зацикливалось
            kafkaTemplate.send("order-created.DLT", record.key(), payload);
            processedEventRepository.save(new ProcessedEvent(event.getEventId()));
            // offset будет закоммичен, партиция продолжит обработку
        }
    }
}
