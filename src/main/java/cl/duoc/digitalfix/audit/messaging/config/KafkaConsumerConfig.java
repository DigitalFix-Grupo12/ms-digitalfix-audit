package cl.duoc.digitalfix.audit.messaging.config;

import cl.duoc.digitalfix.audit.messaging.event.AuditTimelineEvent;
import cl.duoc.digitalfix.audit.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.audit.messaging.support.InvalidEventException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuracion del cliente Kafka del timeline de auditoria.
 *
 * <pre>
 *  workorders.events --(grupo audit-timeline, 3 hilos)--> WorkOrderEventsListener --+
 *  audit.timeline    --(grupo audit-timeline, 3 hilos)--> AuditTimelineListener   --+--> AuditTimelineService
 *
 *  Error en un registro (DefaultErrorHandler):
 *    JSON ilegible (DeserializationException) ----------------> DLT sin reintentar (bytes originales)
 *    Payload invalido (InvalidEventException) ----------------> DLT sin reintentar (JSON del evento)
 *    Falla transitoria (ej. BD caida) -> 3 reintentos 1s/2s/4s -> DLT
 *    DLT = &lt;topico&gt;.audit-timeline.DLT, misma particion; headers kafka_dlt-* con la causa
 *  El registro fallido nunca bloquea la particion: tras enviarse a la DLT se confirma su offset.
 * </pre>
 *
 * Las propiedades comunes (bootstrap, group, offsets, assignor, timeouts) vienen de
 * {@code spring.kafka.*}; aqui solo se agregan los deserializadores tipados por topico.
 */
@Configuration
@EnableConfigurationProperties(AuditKafkaProperties.class)
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    private final AuditKafkaProperties props;
    private final ObjectMapper objectMapper;

    public KafkaConsumerConfig(AuditKafkaProperties props, ObjectMapper objectMapper) {
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkOrderEvent> workOrderEventListenerFactory(
            ConsumerFactory<Object, Object> baseConsumerFactory, DefaultErrorHandler auditErrorHandler) {
        return typedFactory(WorkOrderEvent.class, baseConsumerFactory, auditErrorHandler);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AuditTimelineEvent> auditTimelineListenerFactory(
            ConsumerFactory<Object, Object> baseConsumerFactory, DefaultErrorHandler auditErrorHandler) {
        return typedFactory(AuditTimelineEvent.class, baseConsumerFactory, auditErrorHandler);
    }

    /**
     * Fabrica por tipo de evento: JSON -> DTO con el tipo fijo del topico (se ignoran
     * los headers de tipo del productor, que contienen clases de otro microservicio),
     * envuelto en ErrorHandlingDeserializer para que un mensaje ilegible no detenga
     * el consumidor (poison pill).
     */
    private <T> ConcurrentKafkaListenerContainerFactory<String, T> typedFactory(
            Class<T> type, ConsumerFactory<Object, Object> base, DefaultErrorHandler errorHandler) {

        JsonDeserializer<T> json = new JsonDeserializer<>(type, objectMapper, false);
        ConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(
                base.getConfigurationProperties(),
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(json));

        ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.concurrency());
        factory.setCommonErrorHandler(errorHandler);
        // Commit del offset tras procesar cada registro: at-least-once + idempotencia por eventId
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        return factory;
    }

    @Bean
    public DefaultErrorHandler auditErrorHandler(DeadLetterPublishingRecoverer auditDltRecoverer) {
        AuditKafkaProperties.Retry retry = props.retry();
        ExponentialBackOff backOff = new ExponentialBackOff(retry.initialInterval().toMillis(), retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());
        backOff.setMaxAttempts(retry.maxAttempts());

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            log.error("Enviando a DLT {}-{}@{} -> {}: {}", record.topic(), record.partition(), record.offset(),
                    props.dltFor(record.topic()), rootMessage(ex));
            // En la DLT queda la causa real (ej. InvalidEventException con el detalle de
            // cada campo) y no el envoltorio ListenerExecutionFailedException de Spring
            auditDltRecoverer.accept(record, unwrap(ex));
        }, backOff);

        // Reintentar un payload invalido daria el mismo error: directo a la DLT
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Fallo procesando {}-{}@{} (intento {}): {}", record.topic(), record.partition(),
                        record.offset(), attempt, rootMessage(ex)));
        return handler;
    }

    /**
     * Publica el registro fallido en {@code <topico>.<grupo>.DLT}, en la misma
     * particion. Si el valor no se pudo deserializar se reenvian los bytes
     * originales; si no, el evento como JSON.
     */
    @Bean
    public DeadLetterPublishingRecoverer auditDltRecoverer(KafkaTemplate<String, Object> dltJsonTemplate,
                                                           KafkaTemplate<String, byte[]> dltBytesTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, dltBytesTemplate);
        templates.put(Object.class, dltJsonTemplate);
        return new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(props.dltFor(record.topic()), record.partition()));
    }

    @Bean
    public KafkaTemplate<String, Object> dltJsonTemplate(ProducerFactory<Object, Object> baseProducerFactory) {
        JsonSerializer<Object> serializer = new JsonSerializer<Object>(objectMapper).noTypeInfo();
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                baseProducerFactory.getConfigurationProperties(), new StringSerializer(), serializer));
    }

    @Bean
    public KafkaTemplate<String, byte[]> dltBytesTemplate(ProducerFactory<Object, Object> baseProducerFactory) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                baseProducerFactory.getConfigurationProperties(), new StringSerializer(), new ByteArraySerializer()));
    }

    private static Exception unwrap(Exception ex) {
        if (ex instanceof ListenerExecutionFailedException && ex.getCause() instanceof Exception cause) {
            return cause;
        }
        return ex;
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
