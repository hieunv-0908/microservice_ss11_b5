package com.loyaltyservice.consumer;

import com.loyaltyservice.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class LoyaltyEventListener {

    @KafkaListener(topics = "storex-order-events")
    public void consume(OrderEvent event) {
        log.info("Loyalty-Service received order.created - orderId={}", event.getOrderId());
    }
}
