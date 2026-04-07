package com.exemple.notification.consumer;

import com.exemple.common.model.Notification;
import com.exemple.common.model.Order;
import com.exemple.notification.producer.NotificationProducer;
import com.exemple.notification.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    @Mock
    private NotificationProducer notificationProducer;

    @Mock
    private OrderProcessingService orderProcessingService;

    @InjectMocks
    private OrderConsumer orderConsumer;

    @Test
    void processOrder_shouldDelegateToServiceAndPublishNotification() {
        Order order = buildOrder();
        Notification notification = buildNotification(order);
        when(orderProcessingService.buildNotification(order)).thenReturn(notification);

        orderConsumer.processOrder(order);

        verify(orderProcessingService).buildNotification(order);
        verify(notificationProducer).sendNotification(notification);
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

    private Notification buildNotification(Order order) {
        return Notification.builder()
                .notificationId(UUID.randomUUID().toString())
                .recipient(order.getCustomerId())
                .message("Your order " + order.getOrderId() + " has been received")
                .type(Notification.NotificationType.EMAIL)
                .build();
    }
}
