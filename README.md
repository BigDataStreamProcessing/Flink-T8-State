# Flink-T8-State

Projekt startowy do warsztatu **T8 - zaawansowana obsługa stanu** ze szkolenia
"Zaawansowane przetwarzanie strumieni danych w Apache Flink" (scenariusz *HouseStats*,
środowisko *BigData26*, Apache Flink 2.3.0, *DataStream API* v1).

Zawartość (`src/main/java/org/example/flink`):

- programy (klasy z `main`, uruchamiane przez `flink run -c`):
  - `AlertsApp` - część A: źródło `kafka-input` (watermark z zapasem 3 s), cztery funkcje ze
    stanem klucza i komunikaty do tematu `house-alerts`; punkty kontrolne co 10 s w
    `s3://flink-checkpoints/alerts-app`,
  - `ChallengeApp` - część B: to samo plus wyzwania dla domów z tematu `house-challenge`
    (`connect`); uruchamiany z punktu zachowania `AlertsApp`,
- `model` - klasy danych: `HouseEvent` (zdarzenie), `Challenge` (wyzwanie dla domu), `Alert`
  (komunikat funkcji ze stanem),
- `functions` - funkcje ze stanem:
  - `SilenceAlert` - cisza domu dłuższa niż 10 s czasu zdarzenia (stan + timer),
  - `Series` - komunikat co 100 punktów domu (`ReducingState` i `ValueState`),
  - `NewCharacters` - pierwsze pojawienie się postaci (bez TTL i z TTL),
  - `HouseChallenge` - wyzwania z drugiego strumienia (`KeyedCoProcessFunction`),
  - `IdleAlert` i `SeriesNaive` - do części "Jeśli masz czas",
- `connectors` - złącza: `KafkaSources` (źródła Kafka z JSON), `KafkaSinks` (ujście Kafka z JSON).

Miejsca do uzupełnienia oznaczone są komentarzami `TODO`.

## Wymagania

- Java 17 lub 21, Maven (IntelliJ IDEA ma wbudowany),
- działające środowisko BigData26 (profil `flink`; tematy `kafka-input`, `house-challenge`,
  `house-alerts`; bucket `flink-checkpoints` w MinIO - przygotowanie w warsztacie),
- projekt sklonowany do `~/BigData26/shared_workspace` - katalog jest widoczny w kontenerze
  `flink-jobmanager` jako `/opt/workspace`.

## Budowa i uruchomienie

1. Otwórz katalog projektu w IntelliJ IDEA (*File -> Open*), zaufaj projektowi Maven.
2. Zbuduj jar: panel *Maven* -> *Lifecycle* -> `clean`, potem `package`
   (powstaje `target/flink-t8-state.jar`).
3. Wyślij wybrany program na klaster (jar nie wskazuje klasy głównej - podajemy ją opcją `-c`):

   ```bash
   cd ~/BigData26
   docker compose exec flink-jobmanager ./bin/flink run -d -p 2 \
     -c org.example.flink.AlertsApp /opt/workspace/Flink-T8-State/target/flink-t8-state.jar
   ```

Wszystkie zależności (w tym złącze Kafki i format JSON) mają zakres `provided` - klaster ma je w
`/opt/flink/lib`. Programy korzystają z adresów z sieci kontenerów (`kafka-1..3`, `minio`), więc
są przeznaczone do uruchamiania na klastrze.

Treść zadań: warsztat T8 (MkDocs, `apache-flink/w8`).
