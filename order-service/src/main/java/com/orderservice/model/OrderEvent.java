package com.orderservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    @Builder.Default
    private String eventType = "order.created";

    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
}