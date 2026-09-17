package com.inventoryservice.consumer;

import com.inventoryservice.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class InventoryConsumer {

    @KafkaListener(topics = "storex-order-events", groupId = "storex-inventory")
    public void consume(OrderEvent event) {
        log.info("Inventory-Service received order.created - orderId={}", event.getOrderId());

        if (event.getProductId() == null) {
            log.warn("productId không được null cho orderId={}. Ném Exception...", event.getOrderId());
            throw new IllegalArgumentException("productId không được null");
        }

        log.info("Inventory-Service đã xử lý đơn hàng thành công - orderId={}", event.getOrderId());
    }
}
