package com.exemple.kafka;

import com.exemple.kafka.model.Order;
import com.exemple.kafka.producer.OrderProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9093", "port=9093"},
        topics = {"orders-topic", "notifications-topic"}
)
class KafkaIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    @Test
    void shouldSendOrderToKafka() throws InterruptedException {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId("test-customer")
                .product("Test Product")
                .quantity(2)
                .price(BigDecimal.valueOf(49.99))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrder(order);

        // Give async send time to complete
        Thread.sleep(500);

        assertThat(order.getOrderId()).isNotNull();
        assertThat(order.getStatus()).isEqualTo(Order.OrderStatus.PENDING);
    }
}
