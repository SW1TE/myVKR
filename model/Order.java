package ru.mephi.kafka.example.model;

import javax.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "orders")
public class Order {
    @Id
    private String id;
    private String product;
    private BigDecimal amount;
    private String status;

    // геттеры и сеттеры
}
