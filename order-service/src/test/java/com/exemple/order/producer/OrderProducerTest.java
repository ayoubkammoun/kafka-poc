package com.exemple.order.producer;

import com.exemple.common.model.Order;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private OrderProducer orderProducer;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(orderProducer, "ordersTopic", "orders-topic");
    }

    @Test
    void sendOrder_shouldPublishToOrdersTopic() {
        Order order = buildOrder();
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<>());

        orderProducer.sendOrder(order);

        verify(kafkaTemplate).send(eq("orders-topic"), anyString(), eq(order));
    }

    @Test
    void sendOrder_shouldUseOrderIdAsPartitionKey() {
        Order order = buildOrder();
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(new CompletableFuture<>());

        orderProducer.sendOrder(order);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), keyCaptor.capture(), any());
        assertThat(keyCaptor.getValue()).isEqualTo(order.getOrderId());
    }

    @Test
    void sendOrderAsync_shouldReturnKafkaFuture() {
        Order order = buildOrder();
        CompletableFuture<SendResult<String, Object>> expectedFuture = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(expectedFuture);

        CompletableFuture<SendResult<String, Object>> result = orderProducer.sendOrderAsync(order);

        assertThat(result).isSameAs(expectedFuture);
    }

    private Order buildOrder() {
        return Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId("cust-123")
                .product("Widget")
                .quantity(1)
                .price(BigDecimal.valueOf(9.99))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();
    }
}
