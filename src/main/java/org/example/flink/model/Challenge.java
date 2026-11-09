package org.example.flink.model;

/**
 * Wyzwanie dla domu - wiadomość z tematu house-challenge, np.
 * {"house": "Gryffindor", "target": 50, "seconds": 60}
 * czyli: zdobądź 50 punktów w 60 sekund (czasu zdarzenia) od chwili wyzwania.
 *
 * Pola start, deadline i points nie przychodzą w JSON - wylicza je HouseChallenge i trzyma
 * w stanie klucza razem z resztą wyzwania.
 */
public class Challenge {
    public String house;
    public long target;
    public long seconds;

    /** początek wyzwania - znacznik czasu rekordu z wyzwaniem (epoch ms) */
    public long start;
    /** termin: start + seconds (epoch ms) */
    public long deadline;
    /** punkty zebrane od startu */
    public long points;

    public Challenge() {}

    @Override
    public String toString() {
        return "Challenge{" + house + ", " + points + "/" + target + " w " + seconds + " s, start="
                + start + ", deadline=" + deadline + "}";
    }
}
