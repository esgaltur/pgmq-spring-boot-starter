# PGMQ Spring Boot Starter brand guide

## Positioning

**Durable queues. Instant wake-ups. Spring-native.**

PGMQ Spring Boot Starter brings durable PostgreSQL messaging into the Spring
programming model. The identity should feel dependable and technical without
looking heavyweight: PostgreSQL provides durable queue state, Spring provides
the application experience, and `LISTEN/NOTIFY` provides responsive wake-ups.

## Mark

The elephant represents the PostgreSQL foundation. Its trunk becomes a sequence
of queue nodes and ends in a notification spark. The green leaf-shaped ear
signals Spring integration without reproducing the official Spring mark.

| Asset | Intended use |
|---|---|
| [`pgmq-spring-mark.png`](assets/pgmq-spring-mark.png) | Repository avatar, square thumbnail, presentation icon |
| [`pgmq-spring-banner.png`](assets/pgmq-spring-banner.png) | README masthead, GitHub social preview, announcement header |

Keep generous clear space around the mark. Do not stretch, rotate, recolor, add
effects, or place it over visually busy artwork. The current square asset uses
an intentional off-white canvas rather than simulated transparency.

## Palette

| Role | Color | Hex |
|---|---|---|
| Foundation | Deep navy | `#13233A` |
| PostgreSQL and queue paths | Database blue | `#336791` |
| Spring and notification accents | Fresh green | `#6DB33F` |
| Light canvas and primary inverse text | Off-white | `#F7F9FC` |

Use navy as the dominant dark color, blue for durable data paths, and green
sparingly for framework integration, healthy status, or wake-up accents.

## Voice

Brand language should be concise, practical, and technically honest:

- Lead with durable messaging and Spring ergonomics.
- Describe `LISTEN/NOTIFY` as a complementary wake-up mechanism, not as the
  durable queue or a replacement for PGMQ.
- Prefer measurable claims over superlatives.
- Keep the early-beta status visible until the project has production evidence.
- Avoid claiming universal replacement of Kafka, RabbitMQ, or managed queues;
  explain the workload where PostgreSQL-backed messaging is a good fit.

## Naming

Use **PGMQ Spring Boot Starter** in headings and prose. Use
`pgmq-spring-boot-starter` only for the Maven artifact ID, repository slug,
commands, and file paths.

The project mark is an original community identity. PostgreSQL, Spring, and
PGMQ names and trademarks belong to their respective owners.
