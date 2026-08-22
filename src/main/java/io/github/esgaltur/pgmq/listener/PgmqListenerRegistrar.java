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

            listeners.add(new PgmqListenerMetadata(
                    bean, method, annotation, payloadType, messageWrapped, batch,
                    queue, deadLetterQueue, concurrency));
            log.info("Registered PGMQ listener on method {} for queue {} " +
                            "(Batch: {}, Type: {}, Concurrency: {})",
                    method.getName(), queue, batch, payloadType.getSimpleName(), concurrency);
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
}
