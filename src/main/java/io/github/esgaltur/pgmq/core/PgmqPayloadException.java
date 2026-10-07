package io.github.esgaltur.pgmq.core;

/** A payload could not be written to or read from JSON, whichever Jackson version is in use. */
public class PgmqPayloadException extends RuntimeException {

    public PgmqPayloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
