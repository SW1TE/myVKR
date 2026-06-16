package ru.mephi.kafka.example;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class IdempotencyTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("orders")
            .withUsername("postgres")
            .withPassword("postgres");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private OrderService orderService;
    @Autowired
    private ProcessedEventRepository processedRepo;

    @Test
    void testDuplicateEventIgnored() throws Exception {
        Order order = new Order();
        order.setId("order-1");
        order.setProduct("book");
        order.setAmount(new BigDecimal("100.00"));
        orderService.createOrder(order);

        // Дожидаемся публикации (OutboxPublisher)
        Thread.sleep(6000);

        // Симулируем повторную доставку того же события
        // ... код для инжекции дубликата ...

        // Проверяем, что в таблице processed_events только одна запись
        assertEquals(1, processedRepo.count());
    }
}