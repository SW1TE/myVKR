package ru.mephi.kafka.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.mephi.kafka.example.model.OrderCreatedEvent;

import java.util.UUID;

/**
 * Транзакционный публикатор событий в Apache Kafka.
 * <p>
 * Использует {@code KafkaTemplate} с включённой идемпотентностью
 * (enable.idempotence=true) и транзакционным режимом (transactional-id-prefix).
 * <p>
 * Отправка происходит атомарно в рамках транзакции Spring:
 * если транзакция откатывается — сообщение не будет зафиксировано в Kafka
 * (уровень изоляции read_committed на стороне потребителя гарантирует,
 * что сообщения отменённых транзакций не будут видны).
 *
 * @see KafkaProducerConfig
 * @see OrderEventConsumer
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TransactionalEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Публикует событие создания заказа в топик "order-created".
     * <p>
     * Метод выполняется в контексте текущей транзакции Spring,
     * управляемой аннотацией {@link Transactional} на вызывающем сервисе.
     *
     * @param event объект события, содержащий уникальный идентификатор
     *              и данные заказа
     * @throws RuntimeException если сериализация события в JSON не удалась
     */
    public void publishOrderCreatedEvent(OrderCreatedEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Ошибка сериализации события {}: {}", event.getEventId(), e.getMessage());
            throw new RuntimeException("Не удалось сериализовать событие", e);
        }

        // Отправка с ключом заказа для сохранения порядка событий
        // одного заказа в одной партиции
        kafkaTemplate.send("order-created", event.getOrderId(), payload)
                .addCallback(
                        result -> {
                            log.debug("Событие {} успешно отправлено: offset={}, partition={}",
                                    event.getEventId(),
                                    result.getRecordMetadata().offset(),
                                    result.getRecordMetadata().partition());
                        },
                        failure -> {
                            log.error("Ошибка отправки события {}: {}",
                                    event.getEventId(), failure.getMessage());
                        }
                );
    }

    /**
     * Публикует несколько событий атомарно в рамках одной транзакции Kafka.
     * <p>
     * Используется для сценариев, где изменение состояния системы
     * порождает несколько событий, которые должны быть зафиксированы
     * или отменены как единое целое (например, "заказ создан" +
     * "платёж инициирован" + "склад зарезервирован").
     *
     * @param events массив событий для атомарной публикации
     */
    public void publishMultipleEvents(OrderCreatedEvent... events) {
        for (OrderCreatedEvent event : events) {
            publishOrderCreatedEvent(event);
        }
        // Все сообщения будут зафиксированы или отменены
        // вместе с завершением транзакции Spring
    }

    /**
     * Вспомогательный метод для создания события с уникальным
     * идентификатором из объекта заказа.
     *
     * @param orderId  идентификатор заказа
     * @param product  наименование товара
     * @param amount   сумма заказа
     * @return готовое событие с заполненным UUID
     */
    public static OrderCreatedEvent createEvent(String orderId, String product,
                                                java.math.BigDecimal amount) {
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                orderId,
                product,
                amount
        );
    }
}
