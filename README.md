# Kafka POC

A Spring Boot 3 multi-module project demonstrating microservice communication via Apache Kafka, with TDD, Dead Letter Topics (DLT), and a visual Kafka UI.

## Architecture

```
POST /api/orders
      │
      ▼
┌─────────────────────┐
│    order-service    │  port 8080
│  ─────────────────  │
│  OrderController    │
│  OrderProducer      │──────────────────────► orders-topic
└─────────────────────┘                              │
                                                     ▼
                                        ┌────────────────────────┐
                                        │  notification-service  │  port 8081
                                        │  ──────────────────── │
                                        │  OrderConsumer         │
                                        │  OrderProcessingService│
                                        │  NotificationProducer  │──► notifications-topic
                                        │  NotificationConsumer  │◄── notifications-topic
                                        └────────────────────────┘
                                                     │
                                          (on error after 3 retries)
                                                     ▼
                                              orders-topic.DLT
```

## Project Structure

```
kafka-microservices/
├── common/                         # Shared models (Order, Notification)
├── order-service/                  # REST API + Kafka producer (port 8080)
│   ├── controller/OrderController
│   └── producer/OrderProducer
├── notification-service/           # Kafka consumers + notification producer (port 8081)
│   ├── consumer/OrderConsumer
│   ├── consumer/NotificationConsumer
│   ├── producer/NotificationProducer
│   └── service/OrderProcessingService
└── docker-compose.yml              # Kafka + Kafka UI
```

## Prerequisites

- Java 17+
- Maven (or use `./mvnw`)
- Docker

---

## Running Locally

### Step 1 — Build all modules

```bash
./mvnw install -DskipTests
```

### Step 2 — Start Kafka

```bash
docker compose up -d
```

| Service   | URL                      |
|-----------|--------------------------|
| Kafka     | `localhost:9092`         |
| Kafka UI  | http://localhost:8090    |

### Step 3 — Start order-service

```bash
./mvnw spring-boot:run -pl order-service
```

Wait for: `Started OrderServiceApplication`

### Step 4 — Start notification-service

```bash
./mvnw spring-boot:run -pl notification-service
```

Wait for: `Started NotificationServiceApplication`

---

## API Endpoints

### Create an order (auto-generates ID and sets status to PENDING)

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","product":"Laptop","quantity":1,"price":999.99}' | jq
```

### Create a sample order (no body needed)

```bash
curl -s -X POST http://localhost:8080/api/orders/sample | jq
```

### Create a bad order (triggers DLT error flow)

```bash
curl -s -X POST http://localhost:8080/api/orders/bad | jq
```

---

## Dead Letter Topic (DLT)

When `notification-service` fails to process a message (e.g. missing `customerId`), it retries **3 times** with a **1 second** delay, then routes the failed message to `orders-topic.DLT`.

**To observe in Kafka UI:**
1. Send a bad order: `POST /api/orders/bad`
2. Open http://localhost:8090
3. Go to **Topics** → `orders-topic.DLT`
4. Inspect the message — headers include:
   - `kafka_dlt-exception-message` — why it failed
   - `kafka_dlt-original-topic` — `orders-topic`
   - `kafka_dlt-original-offset` — original offset

---

## Observing Kafka

### Watch messages on a topic

```bash
# orders
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-topic \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" → "

# notifications
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic notifications-topic \
  --from-beginning

# failed messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders-topic.DLT \
  --from-beginning
```

### List topics

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### Check consumer group lag

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group notification-demo-group
```

---

## Running Tests

```bash
# all modules
./mvnw test

# single module
./mvnw test -pl notification-service
./mvnw test -pl order-service
```

### Test coverage

| Module                | Tests | What is covered |
|-----------------------|-------|-----------------|
| `order-service`       | 8     | OrderProducer (topic, key), OrderController (HTTP 202, ID generation, default status, ID preservation, sample endpoint) |
| `notification-service`| 11    | OrderProcessingService (recipient, type, message, unique IDs, null/blank customerId → exception), OrderConsumer (delegates to service + sends notification), NotificationConsumer (EMAIL/SMS/PUSH dispatch) |

---

## Kafka Topics

| Topic                  | Producer             | Consumer                    |
|------------------------|----------------------|-----------------------------|
| `orders-topic`         | order-service        | notification-service        |
| `notifications-topic`  | notification-service | notification-service        |
| `orders-topic.DLT`     | notification-service | (inspect manually / Kafka UI) |

---

## Stopping

```bash
docker compose down
```
