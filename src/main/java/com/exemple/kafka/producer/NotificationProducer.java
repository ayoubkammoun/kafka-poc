package com.exemple.kafka.producer;

import com.exemple.kafka.model.Notification;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationProducer {

    private final KafkaTemplate<String, Notification> kafkaTemplate;

    @Value("${kafka.topics.notifications}")
    private String notificationsTopic;

    public void sendNotification(Notification notification) {
        CompletableFuture<SendResult<String, Notification>> future =
                kafkaTemplate.send(notificationsTopic, notification.getNotificationId(), notification);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Notification sent: id={}, partition={}, offset={}",
                        notification.getNotificationId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send notification: id={}, error={}",
                        notification.getNotificationId(), ex.getMessage(), ex);
            }
        });
    }
}
