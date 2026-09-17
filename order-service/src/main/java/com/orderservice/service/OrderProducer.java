package com.orderservice.service;

import com.orderservice.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public Mono<Void> sendOrderEvent(OrderEvent event) {
        if (event.getEventType() == null || event.getEventType().isBlank()) {
            event.setEventType("order.created");
        }

        return Mono.fromFuture(() -> kafkaTemplate.send(
                "storex-order-events",
                event.getOrderId(),
                event
        )).doOnSuccess(result ->
                log.info("Successfully sent order event to Kafka topic 'storex-order-events': orderId={}, partition={}, offset={}",
                        event.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset())
        ).doOnError(ex ->
                log.error("Failed to send order event to Kafka: orderId={}", event.getOrderId(), ex)
        ).then();
    }
}