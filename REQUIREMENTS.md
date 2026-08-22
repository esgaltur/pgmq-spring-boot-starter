# Requirements

This document outlines the system, software, and dependency requirements for using or contributing to the `pgmq-spring-boot-starter`.

## 🖥️ System & Infrastructure Requirements

### PostgreSQL
- **Version:** PostgreSQL 14 or higher is recommended.
- **Extension:** The [`pgmq`](https://github.com/tembo-io/pgmq) extension must be installed on your PostgreSQL server. 
  - *Note:* If you are using a managed database provider (like AWS RDS, GCP Cloud SQL, or Azure), ensure they support installing custom extensions or provide `pgmq` natively (e.g., Tembo Cloud).
  - PGMQ 1.10 or newer is required for throttled `LISTEN/NOTIFY` mode. Older versions automatically fall back to polling.

### Java Development Kit (JDK)
- **Version:** Java 17 or higher. 
- *Why?* The starter is built on top of Spring Boot 3.x, which mandates Java 17 as the baseline.

---

## 📦 Application Dependencies

To use this starter in your application, your project must meet the following dependency baselines:

- **Spring Boot:** `3.2.0` or higher.
- **Spring Data / JDBC:** The starter relies on `spring-boot-starter-jdbc` to interact with the database.
- **Jackson:** Used for serializing and deserializing message payloads to and from JSONB.

### Optional Dependencies
- **Micrometer (`micrometer-core`):** If present on the classpath, the starter will automatically register metrics for queue depth, processing latency, and throughput.
- **Lombok:** If you are contributing to the starter's source code, Lombok is required to compile the project.

---

## 🛠️ Development & Testing Requirements

If you wish to contribute to the source code, you will need the following tools:

- **Maven:** `3.8.x` or higher for building the project.
- **Docker:** Required for running the integration test suite. 
  - The project uses **Testcontainers** to spin up a real PostgreSQL database with the `pgmq` extension installed (`quay.io/tembo/pgmq-pg:latest`) during the `mvn test` phase.

---

## 🚀 Deployment Requirements (Serverless / AOT)

- **GraalVM (Optional):** If you intend to deploy your application as a Native Image (e.g., for AWS Lambda to reduce cold starts), you must use GraalVM 22.3+. The starter includes a `RuntimeHintsRegistrar` to ensure compatibility.
