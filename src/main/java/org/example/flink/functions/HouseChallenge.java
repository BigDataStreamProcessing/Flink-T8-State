package org.example.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import org.example.flink.model.Alert;
import org.example.flink.model.Challenge;
import org.example.flink.model.HouseEvent;

/**
 * Wyzwania dla domów: "zdobądź target punktów w seconds sekund".
 *
 * Dwa wejścia podzielone po domu, jeden stan klucza - bieżące wyzwanie domu:
 * - wejście 2 (wyzwania) - START: zapisuje wyzwanie w stanie i rejestruje timer na termin;
 *   nowe wyzwanie zastępuje bieżące (challenge-replaced),
 * - wejście 1 (punkty)   - dolicza punkty ze zdarzeń z przedziału [start, termin];
 *   po osiągnięciu celu SUKCES,
 * - timer czasu zdarzenia - PORAŻKA, gdy watermark dojdzie do terminu przed osiągnięciem celu.
 *
 * Bilans: START = SUKCES + PORAŻKA + ZASTĄPIONE (+ wyzwania jeszcze trwające).
 */
public class HouseChallenge extends KeyedCoProcessFunction<String, HouseEvent, Challenge, Alert> {

    /** bieżące wyzwanie domu: cel, start, termin, zebrane punkty; null - brak wyzwania */
    private transient ValueState<Challenge> active;

    @Override
    public void open(OpenContext openContext) {
        // TODO 8a: uchwyt stanu active - ValueStateDescriptor "active" typu Challenge.class
    }

    @Override                                                 // wejście 2: wyzwania
    public void processElement2(Challenge c, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 8b: jeśli w stanie jest bieżące wyzwanie - usuń jego timer i wyślij
        //          alert("challenge-replaced", stare, jego termin, watermark, czas przetwarzania);
        //          potem nowe: start = znacznik wyzwania, deadline = start + seconds * 1000,
        //          points = 0; zapisz je w stanie, zarejestruj timer czasu zdarzenia na deadline
        //          i wyślij alert("challenge-start", ...)
    }

    @Override                                                 // wejście 1: punkty
    public void processElement1(HouseEvent e, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 8c: pomiń zdarzenie, gdy nie ma wyzwania albo znacznik zdarzenia jest poza
        //          [start, deadline]; inaczej dolicz punkty; poniżej celu - zapisz stan,
        //          po osiągnięciu celu - alert("challenge-success", ..., znacznik zdarzenia, ...),
        //          usuń timer i wyczyść stan
    }

    @Override                                                 // timer: termin minął
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // TODO 8d: wyślij alert("challenge-failure", wyzwanie ze stanu, timestamp, ...)
        //          i wyczyść stan
    }

    /** komunikat o wyzwaniu: start w sinceTs, zebrane punkty w amount, cel w target */
    static Alert alert(String kind, Challenge c, long atTs, long wm, long emittedAt) {
        Alert a = Alert.of(kind, c.house, atTs, wm, emittedAt);
        a.sinceTs = c.start;
        a.amount = c.points;
        a.target = c.target;
        return a;
    }
}
