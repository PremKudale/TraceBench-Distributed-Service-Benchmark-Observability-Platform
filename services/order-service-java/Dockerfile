# Stage 1: Build JAR using Maven with Eclipse Temurin Java 21
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app
COPY pom.xml .
# Download dependencies for layer caching
RUN mvn dependency:go-offline -B || true

COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Minimal Distroless / JRE Runtime
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN apk --no-cache add curl tzdata

# Add non-root user
RUN addgroup -S tracebench && adduser -S tracebench -G tracebench
USER tracebench

COPY --from=builder /app/target/*.jar /app/order-service.jar

EXPOSE 8082

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8082/api/v1/orders/health || exit 1

ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/order-service.jar"]
