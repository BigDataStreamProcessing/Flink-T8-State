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
 * Alert ciszy: dom nie dostał punktów przez GAP czasu zdarzenia.
 *
 * Stan klucza (dom): termin bieżącego timera. Każde zdarzenie przesuwa termin na
 * (znacznik zdarzenia + GAP); jeśli watermark dojdzie do terminu, zanim przyjdzie nowsze
 * zdarzenie domu, timer strzela i wysyłamy alert.
 */
public class SilenceAlert extends KeyedProcessFunction<String, HouseEvent, Alert> {

    static final long GAP = Duration.ofSeconds(10).toMillis();

    /** termin bieżącego timera (epoch ms); null - dom jeszcze nie miał zdarzeń */
    private transient ValueState<Long> deadline;

    @Override
    public void open(OpenContext openContext) {
        // TODO 1: uchwyt stanu deadline - deskryptor ValueStateDescriptor o nazwie "deadline"
        //         i typie Types.LONG, uchwyt z getRuntimeContext().getState(...)
    }

    @Override
    public void processElement(HouseEvent e, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 2: nowy termin = znacznik zdarzenia (ctx.timestamp()) + GAP;
        //         - jeśli nowy termin nie jest późniejszy od zapisanego - nic nie rób (opóźnione),
        //         - jeśli zdarzenie przyszło PRZED zapisanym terminem - usuń stary timer,
        //         - zarejestruj timer czasu zdarzenia na nowy termin i zapisz go w stanie
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // TODO 3: wyślij Alert.of("silence", klucz, timestamp, watermark, czas przetwarzania)
        //         z sinceTs = timestamp - GAP; jeśli to był termin zapisany w stanie - wyczyść stan
    }
}
