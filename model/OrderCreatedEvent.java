package ru.mephi.kafka.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {
    private String eventId;      // UUID для дедупликации
    private String orderId;
    private String product;
    private BigDecimal amount;
}
