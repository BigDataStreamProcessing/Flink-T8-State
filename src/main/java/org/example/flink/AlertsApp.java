package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.example.flink.connectors.KafkaSinks;
import org.example.flink.connectors.KafkaSources;
import org.example.flink.functions.NewCharacters;
import org.example.flink.functions.Series;
import org.example.flink.functions.SilenceAlert;
import org.example.flink.model.Alert;
import org.example.flink.model.HouseEvent;

import java.time.Duration;

/**
 * Szkolenie Apache Flink, temat T8 - funkcje ze stanem klucza i timerami (część A warsztatu w8).
 *
 * Źródło: temat kafka-input, czas zdarzenia ze znacznika rekordu Kafki, watermark z zapasem 3 s
 * (generator uruchamiamy z opóźnieniem do 2 s). Ze strumienia wychodzą cztery funkcje ze stanem,
 * a ich komunikaty trafiają do tematu house-alerts:
 * - SilenceAlert    - dom milczy dłużej niż 10 s (stan + timer czasu zdarzenia),
 * - Series          - komunikat co 100 punktów domu (dwa stany),
 * - NewCharacters   - pierwsze pojawienie się postaci, bez TTL i z TTL 60 s.
 */
public class AlertsApp {

    /** zapas watermarku: generator opóźnia zdarzenia o 0-2 s */
    static final WatermarkStrategy<HouseEvent> WATERMARKS =
            WatermarkStrategy.<HouseEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withIdleness(Duration.ofMinutes(1));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(checkpointing("alerts-app"));

        DataStream<HouseEvent> events = env
                .fromSource(KafkaSources.events("flink-t8-alerts"), WATERMARKS, "kafka-input")
                .uid("house-events-source");

        alerts(events)
                .sinkTo(KafkaSinks.<Alert>json("house-alerts"))
                .name("house-alerts")
                .uid("house-alerts-sink");

        env.execute("alerts-app");
    }

    /** cztery funkcje ze stanem; ich komunikaty w jednym strumieniu */
    static DataStream<Alert> alerts(DataStream<HouseEvent> events) {
        KeyedStream<HouseEvent, String> byHouse = events.keyBy(e -> e.house);
        KeyedStream<HouseEvent, String> byCharacter = events.keyBy(e -> e.character);

        DataStream<Alert> silence = byHouse.process(new SilenceAlert()).name("cisza");
        DataStream<Alert> series = byHouse.process(new Series()).name("seria");
        DataStream<Alert> newChars = byCharacter
                .process(new NewCharacters(null)).name("nowe-postacie");
        DataStream<Alert> newCharsTtl = byCharacter
                .process(new NewCharacters(Duration.ofSeconds(60))).name("nowe-postacie-ttl");

        return silence.union(series, newChars, newCharsTtl);
    }

    /** punkty kontrolne co 10 s w MinIO, zachowywane po anulowaniu zadania */
    static Configuration checkpointing(String app) {
        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://flink-checkpoints/" + app);
        conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        return conf;
    }
}
