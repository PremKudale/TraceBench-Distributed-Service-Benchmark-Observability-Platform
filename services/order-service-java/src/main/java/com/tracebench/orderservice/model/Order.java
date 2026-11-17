package com.tracebench.orderservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String customerId;
    private double totalAmount;
    private String currency;
    private OrderStatus status;
    private Instant createdAt;
    private List<OrderItem> items;
    private Map<String, String> metadata;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderItem {
        private String productId;
        private int quantity;
        private double unitPrice;
    }
}
