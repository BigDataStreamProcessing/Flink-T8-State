package org.example.flink.functions;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.example.flink.model.Alert;
import org.example.flink.model.HouseEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Seria punktów - wersja NAIWNA ("Jeśli masz czas"): ta sama logika co Series, ale stan
 * trzymamy w zwykłych polach obiektu, a nie w stanie zarządzanym przez Flinka.
 *
 * Działa, dopóki nic się nie psuje. Pola nie trafiają do punktów kontrolnych, więc po awarii
 * i odtworzeniu jednostka zadania zaczyna od pustych map. Mapa po domu jest potrzebna, bo
 * jedna instancja funkcji obsługuje wiele kluczy.
 */
public class SeriesNaive extends KeyedProcessFunction<String, HouseEvent, Alert> {

    static final long THRESHOLD = 100;

    private final Map<String, Long> sum = new HashMap<>();
    private final Map<String, Long> total = new HashMap<>();

    @Override
    public void processElement(HouseEvent e, Context ctx, Collector<Alert> out) {
        String house = ctx.getCurrentKey();
        long s = sum.merge(house, (long) e.score, Long::sum);
        long t = total.merge(house, (long) e.score, Long::sum);
        if (s >= THRESHOLD) {
            Alert a = Alert.of("series-naive", house, ctx.timestamp(),
                    ctx.timerService().currentWatermark(), ctx.timerService().currentProcessingTime());
            a.amount = s;
            a.total = t;
            out.collect(a);
            sum.remove(house);
        }
    }
}
