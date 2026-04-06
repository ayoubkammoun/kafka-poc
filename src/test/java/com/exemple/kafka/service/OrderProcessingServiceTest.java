package com.exemple.kafka.service;

import com.exemple.kafka.model.Notification;
import com.exemple.kafka.model.Order;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class OrderProcessingServiceTest {

    private final OrderProcessingService service = new OrderProcessingService();

    @Test
    void shouldUseCustomerIdAsRecipient() {
        Order order = buildOrder("cust-42", "ord-1");
        Notification notification = service.buildNotification(order);
        assertThat(notification.getRecipient()).isEqualTo("cust-42");
    }

    @Test
    void shouldBuildEmailTypeNotification() {
        Order order = buildOrder("cust-1", "ord-1");
        Notification notification = service.buildNotification(order);
        assertThat(notification.getType()).isEqualTo(Notification.NotificationType.EMAIL);
    }

    @Test
    void shouldIncludeOrderIdInMessage() {
        Order order = buildOrder("cust-1", "ord-99");
        Notification notification = service.buildNotification(order);
        assertThat(notification.getMessage()).contains("ord-99");
    }

    @Test
    void shouldIncludeOrderStatusInMessage() {
        Order order = buildOrder("cust-1", "ord-1");
        Notification notification = service.buildNotification(order);
        assertThat(notification.getMessage()).contains(Order.OrderStatus.PENDING.name());
    }

    @Test
    void shouldGenerateUniqueNotificationIds() {
        Order order = buildOrder("cust-1", "ord-1");
        Notification n1 = service.buildNotification(order);
        Notification n2 = service.buildNotification(order);
        assertThat(n1.getNotificationId()).isNotEqualTo(n2.getNotificationId());
    }

    @Test
    void shouldGenerateNonNullNotificationId() {
        Order order = buildOrder("cust-1", "ord-1");
        Notification notification = service.buildNotification(order);
        assertThat(notification.getNotificationId()).isNotNull().isNotBlank();
    }

    private Order buildOrder(String customerId, String orderId) {
        return Order.builder()
                .orderId(orderId)
                .customerId(customerId)
                .product("Product")
                .quantity(1)
                .price(BigDecimal.valueOf(10.0))
                .status(Order.OrderStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();
    }
}
