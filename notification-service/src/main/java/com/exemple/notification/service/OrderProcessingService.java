package com.exemple.notification.service;

import com.exemple.common.model.Notification;
import com.exemple.common.model.Order;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderProcessingService {

    public Notification buildNotification(Order order) {
        if (order.getCustomerId() == null || order.getCustomerId().isBlank()) {
            throw new IllegalArgumentException(
                    "Cannot build notification: customerId is missing for orderId=" + order.getOrderId());
        }
        return Notification.builder()
                .notificationId(UUID.randomUUID().toString())
                .recipient(order.getCustomerId())
                .message("Your order " + order.getOrderId() + " has been received with status: " + order.getStatus())
                .type(Notification.NotificationType.EMAIL)
                .build();
    }
}
