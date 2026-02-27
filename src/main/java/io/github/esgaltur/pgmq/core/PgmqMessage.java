package io.github.esgaltur.pgmq.core;

import lombok.Builder;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
@Builder
public class PgmqMessage<T> {
    private long msgId;
    private int readCount;
    private OffsetDateTime enqueuedAt;
    private OffsetDateTime vt;
    private T payload;
}
