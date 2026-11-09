package org.example.flink.functions;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.example.flink.model.Alert;
import org.example.flink.model.HouseEvent;

import java.time.Duration;

/**
 * Nowa postać: komunikat, gdy postać (klucz) pojawia się po raz pierwszy.
 *
 * Stan klucza: znacznik "już była". Bez TTL stan każdej postaci zostaje na zawsze - to
 * deduplikacja jak w SQL (i jak ChangelogNormalize). Z TTL stan wygasa po ttl czasu
 * PRZETWARZANIA od ostatniego zapisu, więc postać, która długo milczała, jest znów "nowa".
 *
 * ttl = null - stan bez TTL (kind = new-char), inaczej kind = new-char-ttl.
 */
public class NewCharacters extends KeyedProcessFunction<String, HouseEvent, Alert> {

    private final Duration ttl;
    private transient ValueState<Boolean> seen;

    public NewCharacters(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(OpenContext openContext) {
        // TODO 6: deskryptor ValueStateDescriptor "seen" typu Types.BOOLEAN; jeśli ttl != null -
        //         włącz na nim TTL (enableTimeToLive z StateTtlConfig.newBuilder(ttl).build());
        //         dopiero potem pobierz uchwyt stanu
    }

    @Override
    public void processElement(HouseEvent e, Context ctx, Collector<Alert> out) throws Exception {
        // TODO 7: jeśli stanu nie ma (null) - wyślij Alert.of(ttl == null ? "new-char" : "new-char-ttl",
        //         dom zdarzenia, znacznik zdarzenia, watermark, czas przetwarzania) z character
        //         postaci; w każdym przypadku zapisz w stanie true
    }
}
