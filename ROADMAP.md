# PGMQ Spring Boot Starter Roadmap

This document outlines the planned features, enhancements, and milestones for the `pgmq-spring-boot-starter` project. The roadmap changes with feedback and adoption.

## 🎯 Current Status: v0.3.0

Released on GitHub Packages: 0.1.0, 0.2.0 and 0.3.0. In production use by [MonoPath](https://monopath.app/).
- ✅ `@PgmqListener` for declarative message consumption.
- ✅ `PgmqTemplate` for message production and synchronous consumption.
- ✅ Transactional Outbox built-in (Spring `@Transactional` integration).
- ✅ Transactional processed-message deduplication.
- ✅ High throughput batching.
- ✅ Poison pill handling & Dead Letter Queues (DLQ).
- ✅ Exponential backoff for retries.
- ✅ Micrometer observability integration.
- ✅ Spring AOT runtime-hint integration hook.
- ✅ Spring Boot health and runtime status (`PgmqListenerStatus`).
- ✅ Payloads through the application's Jackson 3 or Jackson 2 mapper (`PgmqPayloadCodec`, 0.1.0).
- ✅ `@PgmqListener(transactional = false)` for long-running handlers (0.2.0).
- ✅ Startup fails with an explanation when the PGMQ extension is unavailable (0.3.0).

### Known follow-ups from production
- [ ] **Quieter expected retries:** every retry is logged at ERROR with a stack trace, so expected
  temporary failures (an external API answering 503) look like application errors.

---

## 🛣️ Upcoming Milestones

### Milestone 1: Stabilization & Observability
*Focus: Improve visibility into queue health and make it easier to manage in production.*
- [x] **Spring Boot Health & Runtime Status:** Expose queue modes, LISTEN state,
  fallback reasons, reconnects, recovery scans, and read counters through
  `PgmqListenerStatus`, Actuator health, and Micrometer.
- [ ] **Advanced DLQ Management:** Provide utilities via `PgmqTemplate` to inspect, replay, or clear messages from the Dead Letter Queue.
- [ ] **Distributed Tracing:** Integrate with Micrometer Tracing (OpenTelemetry/Zipkin) to trace message flows across microservices.
- [ ] **Dynamic Queue Management:** API methods to create and drop queues dynamically at runtime without restarting the application.

### Milestone 2: Advanced Messaging Patterns
*Focus: Extend the core PGMQ capabilities to support complex enterprise messaging patterns.*
- [ ] **Priority Queues:** Introduce a priority mechanism (likely by routing messages to different underlying PGMQ queues based on a priority header).
- [ ] **Topic Exchanges (Pub/Sub):** Provide a fan-out mechanism to route a single message to multiple consumer queues.
- [ ] **Content-Based Routing:** Filter or route messages based on JSONB payload contents or custom headers.
- [ ] **Message Compression:** Optional GZIP or Snappy compression for large payloads to reduce database I/O.

### Milestone 3: Ecosystem Integrations
*Focus: Play nicely with the broader Spring and Java ecosystem.*
- [ ] **Spring Cloud Stream Binder:** A dedicated binder for Spring Cloud Stream (`spring-cloud-stream-binder-pgmq`).
- [ ] **Spring Integration Channel Adapters:** Inbound and Outbound channel adapters for Spring Integration flows.
- [ ] **Test Utilities:** Release a separate `pgmq-spring-boot-starter-test` module to simplify integration testing without boilerplate Testcontainers setup.

### Milestone 4: Production Readiness (v1.0.0)
*Focus: Battle-testing and performance guarantees.*
- [ ] **Performance Benchmarks:** Publish comprehensive benchmarks comparing latency and throughput against Kafka and RabbitMQ on similar hardware.
- [ ] **Performance Tuning Guide:** Documentation on how to tune PostgreSQL (e.g., `work_mem`, `shared_buffers`) and Spring Boot thread pools for optimal PGMQ performance.
- [ ] **Production Case Studies:** The first is MonoPath (reminders and background generation on one small VM; see the README). Measured numbers will follow after its first weeks; more case studies, especially at higher scale, are welcome.

---

## 💡 How to Contribute
If you would like to tackle any of these items, please check the GitHub Issues page or open a discussion!
