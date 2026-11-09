package org.example.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.example.flink.model.Alert;
import org.example.flink.model.HouseEvent;

import java.time.Duration;

/**
 * Alert bezczynności ("Jeśli masz czas"): do domu nie DOTARŁO żadne zdarzenie przez GAP czasu
 * PRZETWARZANIA. Timer czasu przetwarzania strzela według zegara TaskManagera, więc działa
 * także wtedy, gdy dane przestały płynąć i watermark stoi.
 */
public class IdleAlert extends KeyedProcessFunction<String, HouseEvent, Alert> {

    static final long GAP = Duration.ofSeconds(10).toMillis();

    /** termin bieżącego timera czasu przetwarzania */
    private transient ValueState<Long> deadline;

    @Override
    public void open(OpenContext openContext) {
        // TODO 10a: uchwyt stanu deadline - ValueStateDescriptor "idle-deadline" typu Types.LONG
    }

    @Override
    public void processElement(HouseEvent e, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 10b: usuń poprzedni timer czasu PRZETWARZANIA (jeśli jest), zarejestruj nowy na
        //           bieżący czas przetwarzania + GAP i zapisz termin w stanie
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // TODO 10c: wyślij Alert.of("idle", klucz, timestamp, watermark, czas przetwarzania)
        //           z sinceTs = timestamp - GAP i wyczyść stan
    }
}
