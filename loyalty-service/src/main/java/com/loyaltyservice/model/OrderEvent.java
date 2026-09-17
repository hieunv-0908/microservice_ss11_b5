package com.loyaltyservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    private String eventType;
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
}
