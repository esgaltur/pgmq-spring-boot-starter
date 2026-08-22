# Requirements

This document outlines the system, software, and dependency requirements for using or contributing to the `pgmq-spring-boot-starter`.

## 🖥️ System & Infrastructure Requirements

### PostgreSQL
- **Version:** PostgreSQL 14 or higher is recommended.
- **Extension:** The [`pgmq`](https://github.com/tembo-io/pgmq) extension must be installed on your PostgreSQL server. 
  - *Note:* If you are using a managed database provider (like AWS RDS, GCP Cloud SQL, or Azure), ensure they support installing custom extensions or provide `pgmq` natively (e.g., Tembo Cloud).
  - PGMQ 1.10 or newer is required for the complementary throttled `LISTEN/NOTIFY` wake-up mode. PGMQ remains the durable queue in this mode; older versions automatically fall back to polling.

### Java Development Kit (JDK)
- **Version:** Java 17 or higher. 
- *Why?* The starter targets Spring Boot 4 while retaining Java 17 as its source baseline.

---

## 📦 Application Dependencies

To use this starter in your application, your project must meet the following dependency baselines:

- **Spring Boot:** `4.0.0` or higher.
- **Spring Data / JDBC:** The starter relies on `spring-boot-starter-jdbc` to interact with the database.
- **Jackson:** Used for serializing and deserializing message payloads to and from JSONB.

### Optional Dependencies
- **Micrometer (`micrometer-core`):** If a `MeterRegistry` is present, the starter registers queue, processing, wake-up, reconnect, recovery, and empty-read metrics.
- **Spring Boot health:** When present, contributes `pgmqListener` health details. The core runtime and status API do not require Actuator or Micrometer.
- **Lombok:** If you are contributing to the starter's source code, Lombok is required to compile the project.

---

## 🛠️ Development & Testing Requirements

If you wish to contribute to the source code, you will need the following tools:

- **Maven:** `3.8.x` or higher for building the project.
- **Docker:** Required for running the integration test suite. 
  - The project uses **Testcontainers** to spin up PostgreSQL with PGMQ from `ghcr.io/pgmq/pg18-pgmq:v1.10.0` during the `mvn test` phase.

### LISTEN connection

- Notification mode reserves one session-scoped PostgreSQL connection per application instance.
- PgBouncer transaction pooling is not compatible with `LISTEN`; use a direct endpoint or session pooling.
- A separate datasource can be supplied with `@PgmqNotificationDataSource` when the main datasource is transaction-pooled or tightly sized.

---

## 🚀 Deployment Requirements (Serverless / AOT)

- **GraalVM (Optional):** The starter contributes a `RuntimeHintsRegistrar` hook. Applications must still validate their listener methods and payload types with Spring's native test/build workflow.
