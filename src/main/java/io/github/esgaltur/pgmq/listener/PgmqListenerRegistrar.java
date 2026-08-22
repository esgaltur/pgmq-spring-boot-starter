package io.github.esgaltur.pgmq.listener;

import io.github.esgaltur.pgmq.annotation.PgmqListener;
import io.github.esgaltur.pgmq.core.PgmqMessage;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.ResolvableType;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

import java.util.ArrayList;
import java.util.List;

/** Discovers and validates {@link PgmqListener} methods during bean creation. */
@Slf4j
@NullMarked
public final class PgmqListenerRegistrar implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final List<PgmqListenerMetadata> listeners = new ArrayList<>();
    private @Nullable StringValueResolver resolver;

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        ReflectionUtils.doWithMethods(bean.getClass(), method -> {
            PgmqListener annotation = method.getAnnotation(PgmqListener.class);
            if (annotation == null) {
                return;
            }
            if (method.getParameterCount() != 1) {
                throw new IllegalArgumentException("@PgmqListener method must have exactly one parameter");
            }
            validateOptions(annotation, method.toGenericString());

            ResolvableType parameterType = ResolvableType.forMethodParameter(method, 0);
            boolean batch = List.class.isAssignableFrom(parameterType.resolve(Object.class));
            ResolvableType itemType = batch ? parameterType.getGeneric(0) : parameterType;
            boolean messageWrapped = PgmqMessage.class.isAssignableFrom(itemType.resolve(Object.class));
            Class<?> payloadType = messageWrapped
                    ? itemType.getGeneric(0).resolve(Object.class)
                    : itemType.resolve(Object.class);

            String queue = resolve(annotation.queue());
            String deadLetterQueue = resolve(annotation.deadLetterQueue());
            int concurrency = Integer.parseInt(resolve(annotation.concurrency()));
            if (queue.isBlank()) {
                throw new IllegalArgumentException("@PgmqListener queue must not be blank");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("@PgmqListener concurrency must be greater than zero");
            }
            if (!deadLetterQueue.isEmpty() && deadLetterQueue.equals(queue)) {
                throw new IllegalArgumentException("@PgmqListener dead-letter queue must differ from its source queue");
            }
            ReflectionUtils.makeAccessible(method);

            listeners.add(new PgmqListenerMetadata(
                    bean, method, annotation, payloadType, messageWrapped, batch,
                    queue, deadLetterQueue, concurrency));
            log.info("Registered PGMQ listener on method {} for queue {} " +
                            "(Batch: {}, Type: {}, Concurrency: {}, Mode: {})",
                    method.getName(), queue, batch, payloadType.getSimpleName(), concurrency,
                    annotation.mode());
        });
        return bean;
    }

    List<PgmqListenerMetadata> listeners() {
        return List.copyOf(listeners);
    }

    private String resolve(String value) {
        @Nullable String resolved = resolver == null ? value : resolver.resolveStringValue(value);
        if (resolved == null) {
            throw new IllegalArgumentException("Could not resolve @PgmqListener value: " + value);
        }
        return resolved;
    }

    private static void validateOptions(PgmqListener annotation, String method) {
        if (annotation.vt() <= 0) {
            throw invalid(method, "vt must be greater than zero");
        }
        if (annotation.qty() <= 0) {
            throw invalid(method, "qty must be greater than zero");
        }
        if (annotation.pollInterval() <= 0L) {
            throw invalid(method, "pollInterval must be greater than zero");
        }
        if (!Double.isFinite(annotation.backoffMultiplier()) || annotation.backoffMultiplier() < 1.0) {
            throw invalid(method, "backoffMultiplier must be finite and at least 1.0");
        }
        if (annotation.maxBackoff() <= 0) {
            throw invalid(method, "maxBackoff must be greater than zero");
        }
        if (annotation.backoffMultiplier() > 1.0 && annotation.maxBackoff() < annotation.vt()) {
            throw invalid(method, "maxBackoff must not be shorter than vt when backoff is enabled");
        }
    }

    private static IllegalArgumentException invalid(String method, String reason) {
        return new IllegalArgumentException("Invalid @PgmqListener on " + method + ": " + reason);
    }
}
