package com.exemple.notification.consumer;

import com.exemple.common.model.Notification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationConsumer {

    @KafkaListener(
            topics = "${kafka.topics.notifications}",
            groupId = "notification-group",
            containerFactory = "notificationContainerFactory"
    )
    public void consumeNotification(
            @Payload Notification notification,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Received notification: id={}, type={}, recipient={}, partition={}, offset={}",
                notification.getNotificationId(),
                notification.getType(),
                notification.getRecipient(),
                partition,
                offset);

        dispatch(notification);
    }

    void dispatch(Notification notification) {
        switch (notification.getType()) {
            case EMAIL -> log.info("Sending EMAIL to {}: {}", notification.getRecipient(), notification.getMessage());
            case SMS   -> log.info("Sending SMS to {}: {}", notification.getRecipient(), notification.getMessage());
            case PUSH  -> log.info("Sending PUSH to {}: {}", notification.getRecipient(), notification.getMessage());
        }
    }
}
