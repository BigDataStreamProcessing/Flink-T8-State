package org.example.flink.connectors;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;

import org.example.flink.model.Challenge;
import org.example.flink.model.HouseEvent;

/**
 * Źródła Kafka z formatem JSON.
 *
 * Czytają temat od początku (earliest), chyba że zadanie startuje z punktu kontrolnego albo
 * punktu zachowania - wtedy pozycje w partycjach (offsety) pochodzą ze stanu źródła.
 * Znacznik czasu każdego elementu to znacznik rekordu Kafki (czas zdarzenia z generatora).
 */
public final class KafkaSources {

    public static final String BOOTSTRAP = KafkaSinks.BOOTSTRAP;

    private KafkaSources() {}

    /** zdarzenia HouseStats z tematu kafka-input */
    public static KafkaSource<HouseEvent> events(String groupId) {
        return KafkaSource.<HouseEvent>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("kafka-input")
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(HouseEvent.class))
                .build();
    }

    /** wyzwania dla domów z tematu house-challenge */
    public static KafkaSource<Challenge> challenges(String groupId) {
        return KafkaSource.<Challenge>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("house-challenge")
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Challenge.class))
                .build();
    }
}
