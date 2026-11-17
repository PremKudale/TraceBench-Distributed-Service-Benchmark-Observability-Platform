package com.tracebench.orderservice.service;

import com.tracebench.orderservice.model.Order;
import com.tracebench.orderservice.model.OrderStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class OrderProcessingService {

    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter ordersCreatedCounter;
    private final Counter ordersFailedCounter;
    private final Timer orderProcessingTimer;
    private final RestTemplate restTemplate;

    @Value("${ingest.collector.url:http://ingest-collector:8081/api/v1/logs/ingest}")
    private String ingestCollectorUrl;

    private final Map<String, Order> orderStore = new ConcurrentHashMap<>();

    public OrderProcessingService(Tracer tracer, MeterRegistry meterRegistry) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        this.ordersCreatedCounter = Counter.builder("tracebench_orders_created_total")
                .description("Total number of orders successfully processed")
                .register(meterRegistry);

        this.ordersFailedCounter = Counter.builder("tracebench_orders_failed_total")
                .description("Total number of orders that failed during processing")
                .register(meterRegistry);

        this.orderProcessingTimer = Timer.builder("tracebench_orders_processing_duration_seconds")
                .description("Order end-to-end execution latency")
                .publishPercentiles(0.5, 0.9, 0.95, 0.99)
                .register(meterRegistry);

        this.restTemplate = new RestTemplate();
    }

    public Order processOrder(Order request) {
        Timer.Sample sample = Timer.start(meterRegistry);
        Span parentSpan = tracer.spanBuilder("OrderProcessingService.processOrder")
                .setAttribute("customer.id", request.getCustomerId() != null ? request.getCustomerId() : "anon")
                .setAttribute("order.amount", request.getTotalAmount())
                .startSpan();

        try (Scope scope = parentSpan.makeCurrent()) {
            String traceId = parentSpan.getSpanContext().getTraceId();
            String spanId = parentSpan.getSpanContext().getSpanId();
            MDC.put("trace_id", traceId);
            MDC.put("span_id", spanId);

            String orderId = request.getOrderId() != null ? request.getOrderId() : "ORD-" + UUID.randomUUID().toString().substring(0, 8);
            request.setOrderId(orderId);
            request.setCreatedAt(Instant.now());
            request.setStatus(OrderStatus.PROCESSING);

            log.info("Starting order processing for orderId: {}, amount: {}", orderId, request.getTotalAmount());

            // 1. Child Span: Inventory Check
            checkInventory(orderId, request);

            // 2. Child Span: Payment Authorization
            authorizePayment(orderId, request);

            request.setStatus(OrderStatus.COMPLETED);
            orderStore.put(orderId, request);

            ordersCreatedCounter.increment();
            parentSpan.setStatus(StatusCode.OK);

            log.info("Order successfully completed and persisted: orderId: {}", orderId);

            // Forward structured log to TraceBench Ingestion engine
            shipLogToCollector(traceId, spanId, "INFO", "Order processed successfully", orderId, request.getTotalAmount());

            return request;
        } catch (Exception e) {
            parentSpan.recordException(e);
            parentSpan.setStatus(StatusCode.ERROR, e.getMessage());
            ordersFailedCounter.increment();
            log.error("Order processing encountered an error: {}", e.getMessage(), e);

            String traceId = parentSpan.getSpanContext().getTraceId();
            String spanId = parentSpan.getSpanContext().getSpanId();
            shipLogToCollector(traceId, spanId, "ERROR", "Order failed: " + e.getMessage(), request.getOrderId(), request.getTotalAmount());
            throw e;
        } finally {
            sample.stop(orderProcessingTimer);
            parentSpan.end();
            MDC.clear();
        }
    }

    private void checkInventory(String orderId, Order order) {
        Span span = tracer.spanBuilder("inventory.reserve")
                .setAttribute("order.id", orderId)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Simulate minimal non-blocking database latency
            Thread.sleep(2);
            span.addEvent("inventory_reserved");
            log.debug("Inventory reserved for order: {}", orderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            span.end();
        }
    }

    private void authorizePayment(String orderId, Order order) {
        Span span = tracer.spanBuilder("payment.authorize")
                .setAttribute("order.id", orderId)
                .setAttribute("payment.amount", order.getTotalAmount())
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Simulate payment gateway roundtrip
            Thread.sleep(3);
            span.addEvent("payment_authorized");
            log.debug("Payment authorized for order: {}", orderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            span.end();
        }
    }

    public Order getOrder(String orderId) {
        return orderStore.get(orderId);
    }

    @Async
    public void shipLogToCollector(String traceId, String spanId, String level, String message, String orderId, double amount) {
        try {
            Map<String, Object> logPayload = new HashMap<>();
            logPayload.put("timestamp", Instant.now().toString());
            logPayload.put("service_name", "order-service-java");
            logPayload.put("level", level);
            logPayload.put("message", message);
            logPayload.put("trace_id", traceId);
            logPayload.put("span_id", spanId);

            Map<String, String> attributes = new HashMap<>();
            if (orderId != null) attributes.put("order_id", orderId);
            attributes.put("environment", "production");
            logPayload.put("attributes", attributes);

            Map<String, Double> metrics = new HashMap<>();
            metrics.put("amount", amount);
            logPayload.put("metrics", metrics);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(logPayload, headers);

            restTemplate.postForEntity(ingestCollectorUrl, entity, String.class);
        } catch (Exception ex) {
            // Ingestion error shouldn't crash main transaction path
            log.trace("Failed to ship log to collector: {}", ex.getMessage());
        }
    }
}

