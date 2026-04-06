package com.exemple.kafka.service;

import com.exemple.kafka.model.Notification;
import com.exemple.kafka.model.Order;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrderProcessingService {

    public Notification buildNotification(Order order) {
        return Notification.builder()
                .notificationId(UUID.randomUUID().toString())
                .recipient(order.getCustomerId())
                .message("Your order " + order.getOrderId() + " has been received with status: " + order.getStatus())
                .type(Notification.NotificationType.EMAIL)
                .build();
    }
}
