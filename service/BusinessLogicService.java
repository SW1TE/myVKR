package ru.mephi.kafka.example.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.mephi.kafka.example.exception.NonRecoverableException;
import ru.mephi.kafka.example.exception.TransientException;
import ru.mephi.kafka.example.model.Order;
import ru.mephi.kafka.example.model.OrderCreatedEvent;
import ru.mephi.kafka.example.repository.OrderRepository;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class BusinessLogicService {

    private final OrderRepository orderRepository;

    @Transactional("transactionManager")
    public void processOrder(OrderCreatedEvent event) {
        Optional<Order> opt = orderRepository.findById(event.getOrderId());
        if (opt.isEmpty()) {
            throw new NonRecoverableException("Заказ " + event.getOrderId() + " не найден");
        }
        Order order = opt.get();
        // Пример бизнес-логики
        if ("test-fail".equals(order.getProduct())) {
            throw new TransientException("Эмуляция временной ошибки для теста");
        }
        order.setStatus("PROCESSED");
        orderRepository.save(order);
    }
}
