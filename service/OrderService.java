package ru.mephi.kafka.example.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.mephi.kafka.example.model.Order;
import ru.mephi.kafka.example.model.OrderCreatedEvent;
import ru.mephi.kafka.example.model.OutboxMessage;
import ru.mephi.kafka.example.repository.OrderRepository;
import ru.mephi.kafka.example.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional("transactionManager")
    public void createOrder(Order order) {
        // 1. Сохраняем заказ в БД
        orderRepository.save(order);

        // 2. Создаём событие с уникальным идентификатором
        String eventId = UUID.randomUUID().toString();
        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                order.getId(),
                order.getProduct(),
                order.getAmount()
        );

        // 3. Сохраняем запись в таблицу Outbox
        try {
            OutboxMessage outbox = new OutboxMessage();
            outbox.setTopic("order-created");
            outbox.setKey(order.getId());
            outbox.setPayload(objectMapper.writeValueAsString(event));
            outbox.setCreatedAt(LocalDateTime.now());
            outbox.setPublished(false);
            outboxRepository.save(outbox);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка сериализации события", e);
        }
    }
}
