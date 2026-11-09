package org.example.flink.model;

/**
 * Jeden komunikat funkcji ze stanem - zapisywany jako JSON do tematu house-alerts.
 *
 * kind       - kto go wysłał: silence, series, new-char, new-char-ttl, challenge-start,
 *              challenge-success, challenge-failure, challenge-replaced (oraz idle
 *              i series-naive w części "Jeśli masz czas")
 * house      - dom (dla new-char i new-char-ttl: dom z bieżącego zdarzenia)
 * character  - postać (tylko new-char i new-char-ttl)
 * sinceTs    - silence: czas zdarzenia ostatniego zdarzenia domu przed ciszą;
 *              challenge-*: początek wyzwania;
 *              idle: czas przetwarzania, w którym dotarło ostatnie zdarzenie (epoch ms)
 * atTs       - silence, idle, challenge-start, challenge-failure, challenge-replaced: termin
 *              timera; pozostałe: znacznik zdarzenia, które wywołało komunikat (epoch ms)
 * amount     - series: suma punktów zamkniętej serii; challenge-*: punkty zebrane w wyzwaniu
 * total      - series: suma narastająca punktów domu od początku przetwarzania
 * target     - challenge-*: cel wyzwania (punkty)
 * wm         - watermark operatora w chwili emisji (epoch ms)
 * emittedAt  - czas przetwarzania w chwili emisji (epoch ms)
 *
 * Nazwy pól omijają słowa zarezerwowane Flink SQL (value, at, watermark), bo wyniki czytamy
 * w SQL Client.
 */
public class Alert {
    public String kind;
    public String house;
    public String character;
    public long sinceTs;
    public long atTs;
    public long amount;
    public long total;
    public long target;
    public long wm;
    public long emittedAt;

    public Alert() {}

    public static Alert of(String kind, String house, long atTs, long wm, long emittedAt) {
        Alert a = new Alert();
        a.kind = kind;
        a.house = house;
        a.atTs = atTs;
        a.wm = wm;
        a.emittedAt = emittedAt;
        return a;
    }

    @Override
    public String toString() {
        return "Alert{" + kind + ", " + house + (character == null ? "" : ", " + character)
                + ", since=" + sinceTs + ", at=" + atTs + ", amount=" + amount + ", total=" + total + ", target=" + target
                + ", wm=" + wm + ", emittedAt=" + emittedAt + "}";
    }
}
