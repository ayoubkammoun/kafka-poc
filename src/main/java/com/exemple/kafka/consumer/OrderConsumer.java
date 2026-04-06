package com.exemple.kafka.consumer;

import com.exemple.kafka.model.Notification;
import com.exemple.kafka.model.Order;
import com.exemple.kafka.producer.NotificationProducer;
import com.exemple.kafka.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderConsumer {

    private final NotificationProducer notificationProducer;
    private final OrderProcessingService orderProcessingService;

    @KafkaListener(
            topics = "${kafka.topics.orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )

    public void consumeOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.info("Received order: orderId={}, topic={}, partition={}, offset={}",
                order.getOrderId(), topic, partition, offset);

        processOrder(order);
    }

    @KafkaListener(
            topics = "${kafka.topics.orders}",
            groupId = "order-audit-group",
            containerFactory = "orderAuditContainerFactory"
    )
    public void auditOrder(ConsumerRecord<String, Order> record) {
        log.debug("Audit - key={}, value={}, timestamp={}",
                record.key(), record.value(), record.timestamp());
    }

    void processOrder(Order order) {
        log.info("Processing order: {}", order.getOrderId());

        Notification notification = orderProcessingService.buildNotification(order);
        notificationProducer.sendNotification(notification);
    }
}
