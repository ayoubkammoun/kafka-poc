package com.exemple.kafka.consumer;

import com.exemple.kafka.model.Notification;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class NotificationConsumerTest {

    private final NotificationConsumer consumer = new NotificationConsumer();

    @Test
    void dispatch_shouldHandleEmailNotification() {
        Notification notification = buildNotification("user@example.com", Notification.NotificationType.EMAIL);
        assertDoesNotThrow(() -> consumer.dispatch(notification));
    }

    @Test
    void dispatch_shouldHandleSmsNotification() {
        Notification notification = buildNotification("+1234567890", Notification.NotificationType.SMS);
        assertDoesNotThrow(() -> consumer.dispatch(notification));
    }

    @Test
    void dispatch_shouldHandlePushNotification() {
        Notification notification = buildNotification("device-token-abc", Notification.NotificationType.PUSH);
        assertDoesNotThrow(() -> consumer.dispatch(notification));
    }

    private Notification buildNotification(String recipient, Notification.NotificationType type) {
        return Notification.builder()
                .notificationId("notif-" + type.name().toLowerCase())
                .recipient(recipient)
                .message("Hello, this is a test notification")
                .type(type)
                .build();
    }
}
