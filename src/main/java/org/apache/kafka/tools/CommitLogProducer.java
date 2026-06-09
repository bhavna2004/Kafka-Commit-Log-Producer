package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * CLI application that generates synthetic JSON events and produces them
 * to the primary Kafka cluster's commit-log topic.
 *
 * Usage: java -jar commit-log-producer.jar --count <N> [--bootstrap-server <host:port>]
 */
public class CommitLogProducer {

    private static final String DEFAULT_BOOTSTRAP_SERVER = "localhost:9092";
    private static final String DEFAULT_TOPIC = "commit-log";

    // Possible operation types for realistic WAL events
    private static final String[] OP_TYPES = {"INSERT", "UPDATE", "DELETE"};

    // Possible key prefixes to simulate different entity types
    private static final String[] KEY_PREFIXES = {"doc", "user", "order", "product", "session"};

    public static void main(String[] args) {
        // --- Parse CLI arguments ---
        int count = -1;
        String bootstrapServer = DEFAULT_BOOTSTRAP_SERVER;
        String topic = DEFAULT_TOPIC;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count":
                    if (i + 1 < args.length) {
                        try {
                            count = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("ERROR: --count must be a valid integer. Got: " + args[i]);
                            System.exit(1);
                        }
                    }
                    break;
                case "--bootstrap-server":
                    if (i + 1 < args.length) {
                        bootstrapServer = args[++i];
                    }
                    break;
                case "--topic":
                    if (i + 1 < args.length) {
                        topic = args[++i];
                    }
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    System.err.println("WARNING: Unknown argument: " + args[i]);
            }
        }

        // Validate required arguments
        if (count <= 0) {
            System.err.println("ERROR: --count N is required and must be a positive integer.");
            printUsage();
            System.exit(1);
        }

        System.out.printf("Starting CommitLogProducer: bootstrap=%s, topic=%s, count=%d%n",
                bootstrapServer, topic, count);

        // --- Configure and create the Kafka producer ---
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Reliability settings: wait for all in-sync replicas to ack
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Retry on transient failures
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Identify this producer in Kafka logs
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "commit-log-producer");

        int successCount = 0;
        int failureCount = 0;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < count; i++) {
                String keyPrefix = KEY_PREFIXES[(int) (Math.random() * KEY_PREFIXES.length)];
                String keyHex = UUID.randomUUID().toString().substring(0, 4); // short hex suffix like "8f7b"
                String key = keyPrefix + ":" + keyHex;
                String eventJson = generateEvent(key);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, eventJson);

                try {
                    RecordMetadata metadata = producer.send(record).get(); // synchronous send for ordering
                    successCount++;

                    if (successCount % 100 == 0 || successCount == count) {
                        System.out.printf("Progress: %d/%d messages sent. Last offset: %d, partition: %d%n",
                                successCount, count, metadata.offset(), metadata.partition());
                    }

                } catch (ExecutionException e) {
                    failureCount++;
                    System.err.printf("ERROR: Failed to send message %d: %s%n", i + 1, e.getCause().getMessage());
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("ERROR: Producer interrupted: " + e.getMessage());
            System.exit(1);
        }

        // --- Final summary ---
        System.out.printf("%nProduction complete. Success: %d, Failed: %d, Total: %d%n",
                successCount, failureCount, count);

        if (failureCount > 0) {
            System.exit(1); // Signal partial failure to the caller (e.g., run_challenge.sh)
        }
    }

    /**
     * Generates a single JSON event matching the project's event schema:
     * {
     *   "event_id": "<uuid>",
     *   "timestamp": <unix_epoch_seconds>,
     *   "op_type": "<INSERT|UPDATE|DELETE>",
     *   "key": "<prefix>:<short_hex>",
     *   "value": { "status": "<word>" }
     * }
     *
     * Intentionally using manual JSON string building to avoid adding
     * external dependencies (Jackson, Gson) to the project.
     */
    private static String generateEvent(String key) {
        String eventId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();
        String opType = OP_TYPES[(int) (Math.random() * OP_TYPES.length)];
        String status = generateStatus(opType);

        return String.format(
                "{\"event_id\":\"%s\",\"timestamp\":%d,\"op_type\":\"%s\",\"key\":\"%s\",\"value\":{\"status\":\"%s\"}}",
                eventId, timestamp, opType, key, status
        );
    }

    /**
     * Returns a contextually appropriate status for the given operation type.
     */
    private static String generateStatus(String opType) {
        switch (opType) {
            case "INSERT": return "created";
            case "UPDATE": return "archived";
            case "DELETE": return "deleted";
            default:       return "unknown";
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar commit-log-producer.jar --count <N> [OPTIONS]");
        System.out.println();
        System.out.println("Required:");
        System.out.println("  --count N                 Number of messages to produce");
        System.out.println();
        System.out.println("Optional:");
        System.out.println("  --bootstrap-server HOST   Kafka bootstrap server (default: localhost:9092)");
        System.out.println("  --topic TOPIC             Target topic (default: commit-log)");
        System.out.println("  --help                    Show this help message");
    }
}