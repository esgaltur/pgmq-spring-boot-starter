package io.github.esgaltur.pgmq.listener;

import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns the dedicated JDBC connection used for PostgreSQL LISTEN and translates
 * database notifications into local wake-up signals for consumer workers.
 */
@Slf4j
final class PgmqNotificationCoordinator implements AutoCloseable {

    private static final int NOTIFICATION_WAIT_MILLIS = 1_000;

    private final DataSource dataSource;
    private final Duration reconnectInterval;
    private final Map<String, PgmqQueueSignal> signalsByChannel = new ConcurrentHashMap<>();
    private final AtomicReference<Connection> activeConnection = new AtomicReference<>();

    private volatile boolean running;
    private Thread listenerThread;

    PgmqNotificationCoordinator(DataSource dataSource, Duration reconnectInterval) {
        this.dataSource = dataSource;
        this.reconnectInterval = reconnectInterval;
    }

    PgmqQueueSignal register(String channel) {
        if (running) {
            throw new IllegalStateException("Notification channels must be registered before starting");
        }
        return signalsByChannel.computeIfAbsent(channel, key -> {
            log.debug("Registering PostgreSQL notification channel {}.", key);
            return new PgmqQueueSignal();
        });
    }

    void start() throws SQLException {
        if (running) {
            return;
        }

        Connection initialConnection = openListeningConnection();
        activeConnection.set(initialConnection);
        running = true;
        listenerThread = new Thread(() -> listenLoop(initialConnection), "pgmq-notification-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
        signalAll();
        log.info("PGMQ LISTEN connection started for {} channel(s).", signalsByChannel.size());
    }

    private void listenLoop(Connection initialConnection) {
        Connection connection = initialConnection;

        while (running) {
            try {
                if (connection == null || connection.isClosed()) {
                    connection = openListeningConnection();
                    activeConnection.set(connection);
                    signalAll();
                    log.info("PGMQ LISTEN connection re-established for {} channel(s).", signalsByChannel.size());
                }

                PGConnection pgConnection = connection.unwrap(PGConnection.class);
                PGNotification[] notifications = pgConnection.getNotifications(NOTIFICATION_WAIT_MILLIS);
                if (notifications == null) {
                    continue;
                }

                for (PGNotification notification : notifications) {
                    PgmqQueueSignal signal = signalsByChannel.get(notification.getName());
                    if (signal != null) {
                        log.trace("Received PostgreSQL notification on channel {} from backend {}.",
                                notification.getName(), notification.getPID());
                        signal.signal();
                    } else {
                        log.debug("Ignoring notification received on unregistered channel {}.",
                                notification.getName());
                    }
                }
            } catch (SQLException e) {
                if (running) {
                    log.warn("PGMQ LISTEN connection failed; retrying in {}.", reconnectInterval, e);
                }
                closeConnection(connection);
                activeConnection.compareAndSet(connection, null);
                connection = null;
                signalAll();
                waitBeforeReconnect();
            }
        }

        closeConnection(connection);
        activeConnection.compareAndSet(connection, null);
        log.debug("PGMQ notification listener thread stopped.");
    }

    private Connection openListeningConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        boolean success = false;
        try {
            connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                for (String channel : signalsByChannel.keySet()) {
                    statement.execute("LISTEN " + quoteIdentifier(channel));
                }
            }
            success = true;
            return connection;
        } finally {
            if (!success) {
                closeConnection(connection);
            }
        }
    }

    private static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private void waitBeforeReconnect() {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.max(1L, reconnectInterval.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void signalAll() {
        signalsByChannel.values().forEach(PgmqQueueSignal::signal);
    }

    private static void closeConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute("UNLISTEN *");
        } catch (SQLException e) {
            // The connection may already be broken. Closing it is still required.
            log.debug("Could not execute UNLISTEN while closing the PGMQ notification connection.", e);
        }
        try {
            connection.close();
        } catch (SQLException e) {
            // Nothing else can be done while shutting down or reconnecting.
            log.debug("Could not close the PGMQ notification connection cleanly.", e);
        }
    }

    @Override
    public void close() {
        if (!running && activeConnection.get() == null) {
            return;
        }

        log.debug("Stopping the PGMQ notification listener.");
        running = false;
        signalAll();

        Thread thread = listenerThread;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(NOTIFICATION_WAIT_MILLIS + 500L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        Connection connection = activeConnection.getAndSet(null);
        closeConnection(connection);
        log.info("PGMQ LISTEN connection stopped.");
    }

}
