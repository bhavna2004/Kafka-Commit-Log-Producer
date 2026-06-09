# ─────────────────────────────────────────────
# Stage 1: Build the fat JAR using Gradle
# ─────────────────────────────────────────────
FROM gradle:8.7-jdk17 AS builder

# Set working directory inside the build container
WORKDIR /app

# Copy Gradle wrapper and build definition first
# (separate layer so dependency downloads are cached
#  unless build.gradle changes)
COPY build.gradle .

# Copy source code
COPY src/ src/

# Build the fat JAR, skip tests (nothing to test here)
RUN gradle jar --no-daemon -x test

# ─────────────────────────────────────────────
# Stage 2: Minimal runtime image
# ─────────────────────────────────────────────
FROM eclipse-temurin:17-jre-jammy

# Label for Docker Hub identification
LABEL maintainer="commit-log-producer"
LABEL description="Kafka Commit Log Producer - generates synthetic WAL events"
LABEL version="1.0.0"

WORKDIR /app

# Copy only the fat JAR from the builder stage
COPY --from=builder /app/build/libs/commit-log-producer.jar .

# Default entrypoint — flags are passed at runtime via docker-compose
# or docker run command
ENTRYPOINT ["java", "-jar", "commit-log-producer.jar"]

# Default CMD — can be overridden in docker-compose.yml
# This runs 1000 messages against the default bootstrap server
CMD ["--count", "1000", "--bootstrap-server", "primary-kafka:9092"]