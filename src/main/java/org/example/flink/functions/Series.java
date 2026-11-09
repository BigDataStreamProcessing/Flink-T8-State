package org.example.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.example.flink.model.Alert;
import org.example.flink.model.HouseEvent;

/**
 * Seria punktów: komunikat za każde zebrane THRESHOLD punktów domu.
 *
 * Dwa stany klucza (dom):
 * - sum   - punkty bieżącej serii (ReducingState: dodawanie przyrostowe, jak reduce),
 *           czyszczone po każdym komunikacie,
 * - total - suma narastająca od początku przetwarzania (ValueState), nigdy nie czyszczona.
 */
public class Series extends KeyedProcessFunction<String, HouseEvent, Alert> {

    static final long THRESHOLD = 100;

    private transient ReducingState<Long> sum;
    private transient ValueState<Long> total;

    @Override
    public void open(OpenContext openContext) {
        // TODO 4: uchwyty obu stanów:
        //         - sum: ReducingStateDescriptor o nazwie "sum", funkcja Long::sum, typ Types.LONG,
        //           uchwyt z getRuntimeContext().getReducingState(...),
        //         - total: ValueStateDescriptor o nazwie "total", typ Types.LONG
    }

    @Override
    public void processElement(HouseEvent e, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 5: dodaj punkty zdarzenia do sum i do total;
        //         gdy sum >= THRESHOLD - wyślij Alert.of("series", klucz, znacznik zdarzenia,
        //         watermark, czas przetwarzania) z amount = sum i total = total, potem wyczyść sum
    }
}
