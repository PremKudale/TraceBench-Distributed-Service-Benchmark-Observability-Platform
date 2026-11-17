package com.tracebench.orderservice;

import com.tracebench.orderservice.model.Order;
import com.tracebench.orderservice.model.OrderStatus;
import com.tracebench.orderservice.service.OrderProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class OrderServiceApplicationTests {

    @Autowired
    private OrderProcessingService orderService;

    @Test
    void contextLoads() {
        assertNotNull(orderService);
    }

    @Test
    void testProcessOrderSuccess() {
        Order request = Order.builder()
                .customerId("CUST-1001")
                .totalAmount(250.75)
                .currency("USD")
                .items(List.of(
                        Order.OrderItem.builder().productId("PROD-1").quantity(2).unitPrice(100.0).build(),
                        Order.OrderItem.builder().productId("PROD-2").quantity(1).unitPrice(50.75).build()
                ))
                .build();

        Order processed = orderService.processOrder(request);

        assertNotNull(processed.getOrderId());
        assertEquals(OrderStatus.COMPLETED, processed.getStatus());
        assertEquals(250.75, processed.getTotalAmount());

        Order retrieved = orderService.getOrder(processed.getOrderId());
        assertNotNull(retrieved);
        assertEquals(processed.getOrderId(), retrieved.getOrderId());
    }
}

