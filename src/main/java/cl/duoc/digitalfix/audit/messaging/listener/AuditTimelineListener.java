package cl.duoc.digitalfix.audit.messaging.listener;

import cl.duoc.digitalfix.audit.messaging.event.AuditTimelineEvent;
import cl.duoc.digitalfix.audit.messaging.support.EventValidator;
import cl.duoc.digitalfix.audit.messaging.support.KafkaLogContext;
import cl.duoc.digitalfix.audit.service.AuditTimelineService;
import cl.duoc.digitalfix.audit.service.EventOrigin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

/**
 * Consumidor de {@code audit.timeline} (quien / que / cuando / desde donde).
 * El topico es compact,delete: un valor nulo es un tombstone legitimo de la
 * compactacion y se ignora; cualquier otro payload se valida antes de registrarlo.
 */
@Component
public class AuditTimelineListener {

    private static final Logger log = LoggerFactory.getLogger(AuditTimelineListener.class);

    private final EventValidator validator;
    private final AuditTimelineService timelineService;

    public AuditTimelineListener(EventValidator validator, AuditTimelineService timelineService) {
        this.validator = validator;
        this.timelineService = timelineService;
    }

    @KafkaListener(
            id = "audit-timeline-events",
            topics = "${digitalfix.kafka.topics.audit-timeline}",
            groupId = "${digitalfix.kafka.consumer-group}",
            containerFactory = "auditTimelineListenerFactory",
            autoStartup = "${digitalfix.kafka.listeners-enabled:true}")
    public void onTimelineEvent(ConsumerRecord<String, AuditTimelineEvent> record) {
        AuditTimelineEvent event = record.value();
        try (var ignored = KafkaLogContext.open(record,
                event != null ? event.eventId() : null,
                event != null ? event.traceId() : null)) {
            if (event == null && !hasDeserializationError(record)) {
                log.info("Tombstone de compactacion (key={}) ignorado", record.key());
                return;
            }
            validator.requireValid(event, AuditTimelineEvent.class);
            timelineService.registerTimelineEvent(event,
                    EventOrigin.kafka(record.topic(), record.partition(), record.offset()));
        }
    }

    /** ErrorHandlingDeserializer deja el valor nulo y marca el error en este header. */
    private static boolean hasDeserializationError(ConsumerRecord<?, ?> record) {
        return record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER) != null;
    }
}
