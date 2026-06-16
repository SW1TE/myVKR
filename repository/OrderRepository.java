package ru.mephi.kafka.example.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.mephi.kafka.example.model.Order;

public interface OrderRepository extends JpaRepository<Order, String> {}
