package com.orderservice.controller;

import com.orderservice.model.OrderEvent;
import com.orderservice.service.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    @PostMapping
    public ResponseEntity<Mono<String>> createOrder(@RequestBody OrderEvent event) {
        if (event.getOrderId() == null || event.getOrderId().isBlank()) {
            return ResponseEntity.badRequest().body(Mono.just("orderId is required"));
        }

        Mono<String> responseBody = orderProducer.sendOrderEvent(event)
                .thenReturn(event.getOrderId());

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(responseBody);
    }
}