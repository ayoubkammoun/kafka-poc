package com.exemple.order.controller;

import com.exemple.common.model.Order;
import com.exemple.order.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        if (order.getOrderId() == null) {
            order = Order.builder()
                    .orderId(UUID.randomUUID().toString())
                    .customerId(order.getCustomerId())
                    .product(order.getProduct())
                    .quantity(order.getQuantity())
                    .price(order.getPrice())
                    .status(order.getStatus() != null ? order.getStatus() : Order.OrderStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();
        }

        log.info("Publishing order to Kafka: {}", order.getOrderId());
        orderProducer.sendOrder(order);

        return ResponseEntity.accepted().body(order);
    }

    @PostMapping("/bad")
    public ResponseEntity<Order> createBadOrder() {
        // customerId intentionally null → notification-service will fail → routed to DLT
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId(null)
                .product("Bad Product")
                .quantity(1)
                .price(BigDecimal.valueOf(0))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        log.warn("Publishing intentionally bad order: {}", order.getOrderId());
        orderProducer.sendOrder(order);
        return ResponseEntity.accepted().body(order);
    }

    @PostMapping("/sample")
    public ResponseEntity<Order> createSampleOrder() {
        Order order = Order.builder()
                .orderId(UUID.randomUUID().toString())
                .customerId("customer-" + UUID.randomUUID().toString().substring(0, 8))
                .product("Laptop")
                .quantity(1)
                .price(BigDecimal.valueOf(999.99))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        orderProducer.sendOrder(order);
        return ResponseEntity.accepted().body(order);
    }
}
