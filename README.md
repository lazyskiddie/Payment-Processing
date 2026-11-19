# Event-Driven Payment Processing Engine

A Spring Boot service that accepts payment requests over REST, persists them idempotently in PostgreSQL,
and processes them asynchronously through Apache Kafka.

> Single Spring Boot module: the REST gateway (producer) and the processor (Kafka consumer) live in the
> same app. They only talk through Kafka, so they can be split into two deployables later.

## Flow

1. `POST /api/v1/payments` validates the request and the `Idempotency-Key` header.
2. The payment is saved as `PENDING` (unique constraint on the key = DB-level idempotency).
3. After the row is committed, a `PaymentEvent` is published to `payment-events`. Client gets `202 Accepted` + JSON.
4. The consumer **claims** the payment with an atomic `UPDATE ... WHERE status='PENDING'` (-> `PROCESSING`).
   Only one consumer can win the claim, so duplicate events never cause a second bank call.
5. After the (simulated) bank call it moves to `COMPLETED` or `FAILED`.
6. `GET /api/v1/payments/{id}` returns the current status.

Safety nets:
- **Reconciler** (`@Scheduled`): republishes `PENDING` rows older than 60s (lost Kafka publish) and resets
  `PROCESSING` rows older than 5 min (consumer crashed mid-flight).
- **Dead-letter topic** `payment-events.DLT`: poison messages are retried 3x, then parked.
- **Fail-fast producer**: `max.block.ms=2000`, so a Kafka outage returns quickly instead of hanging requests;
  the payment stays `PENDING` and the reconciler publishes it once Kafka is back.

## Idempotency behaviour

| Situation | Result |
|---|---|
| New key | `202`, payment saved + published |
| Same key, same body | `202`, the original payment is returned (no new charge) |
| Same key, different body | `422 Unprocessable Entity` |
| Invalid key / same source & destination / bad amount | `400` |

## Tech stack

Java 17, Spring Boot 4.x, Spring Data JPA, Spring for Apache Kafka 4 (Jackson 3 serializers),
PostgreSQL 15, Kafka (KRaft, no Zookeeper), Docker Compose, JUnit 5 + Mockito, k6.

## Getting started

```bash
docker compose up -d          # Postgres :5432 + Kafka :9092
./mvnw spring-boot:run        # app on http://localhost:8080
```

Config is read from environment variables with local-dev defaults that match `docker-compose.yml`:
`DB_URL`, `DB_USER`, `DB_PASSWORD`, `KAFKA_BOOTSTRAP`, `DDL_AUTO`. Never commit real credentials.

```bash
curl -i -X POST http://localhost:8080/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-12345" \
  -d '{"sourceAccount":"ACC-9999","destinationAccount":"ACC-1111","amount":250.50}'
# HTTP/1.1 202  {"paymentId":1,"status":"PENDING","failureReason":null}

curl http://localhost:8080/api/v1/payments/1     # -> COMPLETED after ~50ms
```

Re-sending the exact same request returns `202` with the **same** `paymentId`; changing the amount under the
same key returns `422`.

## Tests

```bash
./mvnw test        # unit tests, no Docker needed
```

Load test (needs [k6](https://k6.io)): `k6 run -e BASE_URL=http://localhost:8080 load-test/k6-payment-load.js`

## Known limitations / next steps

- No authentication, and payment IDs are sequential. Add auth and ownership checks before exposing it.
- `ddl-auto=update` is for development; use Flyway or Liquibase for real deployments.
- The bank is simulated. A real integration must pass the idempotency key downstream.
- No correlation-ID propagation or metrics yet (Actuator + Micrometer tracing would be the next step).
- Add integration tests with Testcontainers (Postgres + Kafka) to cover the full flow.
