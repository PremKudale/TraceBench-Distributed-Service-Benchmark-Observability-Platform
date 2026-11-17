package com.tracebench.orderservice.config;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class OpenTelemetryConfig {

    @Value("${otel.exporter.otlp.endpoint:http://otel-collector:4317}")
    private String otelEndpoint;

    @Value("${spring.application.name:order-service-java}")
    private String serviceName;

    @Bean
    public OpenTelemetry openTelemetry() {
        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAME, serviceName)
                .put(ResourceAttributes.SERVICE_VERSION, "1.0.0")
                .put(ResourceAttributes.DEPLOYMENT_ENVIRONMENT, "production")
                .build();

        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(otelEndpoint)
                .setTimeout(Duration.ofSeconds(2))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        return sdk;
    }

    @Bean
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer(serviceName, "1.0.0");
    }
}
