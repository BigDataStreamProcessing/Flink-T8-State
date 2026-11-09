package org.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.example.flink.connectors.KafkaSinks;
import org.example.flink.connectors.KafkaSources;
import org.example.flink.functions.HouseChallenge;
import org.example.flink.functions.NewCharacters;
import org.example.flink.functions.Series;
import org.example.flink.functions.SilenceAlert;
import org.example.flink.model.Alert;
import org.example.flink.model.Challenge;
import org.example.flink.model.HouseEvent;

import java.time.Duration;

/**
 * Szkolenie Apache Flink, temat T8 - nowa wersja programu (część B warsztatu w8).
 *
 * To samo co AlertsApp plus wyzwania dla domów: drugi strumień (temat house-challenge)
 * połączony ze zdarzeniami przez connect i KeyedCoProcessFunction (HouseChallenge).
 * Tę wersję uruchamiamy z punktu zachowania zrobionego przez AlertsApp.
 */
public class ChallengeApp {

    /** źródło wyzwań: wiadomości rzadkie, więc źródło szybko staje się bezczynne */
    static final WatermarkStrategy<Challenge> CHALLENGE_WATERMARKS =
            WatermarkStrategy.<Challenge>forMonotonousTimestamps()
                    .withIdleness(Duration.ofSeconds(10));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(AlertsApp.checkpointing("challenge-app"));

        DataStream<HouseEvent> events = env
                .fromSource(KafkaSources.events("flink-t8-alerts"), AlertsApp.WATERMARKS, "kafka-input")
                .uid("house-events-source");
        DataStream<Challenge> challenges = env
                .fromSource(KafkaSources.challenges("flink-t8-challenges"), CHALLENGE_WATERMARKS,
                        "house-challenge")
                .uid("house-challenge-source");

        alerts(events, challenges)
                .sinkTo(KafkaSinks.<Alert>json("house-alerts"))
                .name("house-alerts")
                .uid("house-alerts-sink");

        env.execute("challenge-app");
    }

    static DataStream<Alert> alerts(DataStream<HouseEvent> events, DataStream<Challenge> challenges) {
        KeyedStream<HouseEvent, String> byHouse = events.keyBy(e -> e.house);
        KeyedStream<HouseEvent, String> byCharacter = events.keyBy(e -> e.character);

        // TODO 9: nadaj stały uid operatorom ze stanem - czterem funkcjom i wyzwaniom
        //         (np. "silence", "series", "new-chars", "new-chars-ttl", "house-challenge")
        DataStream<Alert> silence = byHouse.process(new SilenceAlert())
                .name("cisza");
        DataStream<Alert> series = byHouse.process(new Series())
                .name("seria");
        DataStream<Alert> newChars = byCharacter.process(new NewCharacters(null))
                .name("nowe-postacie");
        DataStream<Alert> newCharsTtl = byCharacter.process(new NewCharacters(Duration.ofSeconds(60)))
                .name("nowe-postacie-ttl");

        DataStream<Alert> challengeAlerts = events.keyBy(e -> e.house)
                .connect(challenges.keyBy(c -> c.house))
                .process(new HouseChallenge())
                .name("wyzwania");

        return silence.union(series, newChars, newCharsTtl, challengeAlerts);
    }
}
