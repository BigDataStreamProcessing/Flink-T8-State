package org.example.flink.model;

/**
 * Zdarzenie HouseStats - POJO (publiczne pola, konstruktor bez argumentów).
 *
 * Pola z JSON-a wysyłanego przez KafkaFakerProducer. Czas zdarzenia NIE jest polem obiektu:
 * KafkaSource bierze go ze znacznika rekordu Kafki (generator zapisuje tam ten sam czas co w
 * polu ts), a w funkcjach odczytujemy go przez ctx.timestamp().
 */
public class HouseEvent {
    public String house;
    public String character;
    public int score;
    public String ts;

    public HouseEvent() {}

    @Override
    public String toString() {
        return "HouseEvent{" + house + ", " + character + ", " + score + ", ts=" + ts + "}";
    }
}
