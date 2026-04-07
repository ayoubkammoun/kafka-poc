package com.exemple.order.producer;

import com.exemple.common.model.Order;
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
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    public void sendOrder(Order order) {
        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(ordersTopic, order.getOrderId(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent: orderId={}, partition={}, offset={}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: orderId={}, error={}",
                        order.getOrderId(), ex.getMessage(), ex);
            }
        });
    }

    public CompletableFuture<SendResult<String, Object>> sendOrderAsync(Order order) {
        log.debug("Sending order async: orderId={}", order.getOrderId());
        return kafkaTemplate.send(ordersTopic, order.getOrderId(), order);
    }
}
