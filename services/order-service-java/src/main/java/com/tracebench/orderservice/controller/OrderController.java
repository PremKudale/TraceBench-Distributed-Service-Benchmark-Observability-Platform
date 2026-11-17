package com.tracebench.orderservice.controller;

import com.tracebench.orderservice.model.Order;
import com.tracebench.orderservice.service.OrderProcessingService;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderProcessingService orderService;
    private final PrometheusMeterRegistry prometheusRegistry;

    public OrderController(OrderProcessingService orderService, PrometheusMeterRegistry prometheusRegistry) {
        this.orderService = orderService;
        this.prometheusRegistry = prometheusRegistry;
    }

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        Order processed = orderService.processOrder(order);
        return ResponseEntity.status(HttpStatus.CREATED).body(processed);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrder(@PathVariable("id") String id) {
        Order order = orderService.getOrder(id);
        if (order == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(order);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "order-service-java",
                "runtime", "Java 21"
        ));
    }

    @GetMapping(value = "/metrics", produces = "text/plain")
    public String metrics() {
        return prometheusRegistry.scrape();
    }
}
