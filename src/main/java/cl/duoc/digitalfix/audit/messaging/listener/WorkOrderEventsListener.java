package cl.duoc.digitalfix.audit.messaging.listener;

import cl.duoc.digitalfix.audit.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.audit.messaging.support.EventValidator;
import cl.duoc.digitalfix.audit.messaging.support.KafkaLogContext;
import cl.duoc.digitalfix.audit.service.AuditTimelineService;
import cl.duoc.digitalfix.audit.service.EventOrigin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de {@code workorders.events} en el grupo {@code audit-timeline}.
 * Comparte el topico con el grupo {@code report-kpis} (fan-out: cada grupo lee
 * todos los eventos a su ritmo) y reparte sus 3 particiones entre 3 hilos.
 * Solo adapta el registro: valida y delega en el servicio de dominio; si algo
 * falla, la excepcion sube al DefaultErrorHandler (reintentos / DLT).
 */
@Component
public class WorkOrderEventsListener {

    private final EventValidator validator;
    private final AuditTimelineService timelineService;

    public WorkOrderEventsListener(EventValidator validator, AuditTimelineService timelineService) {
        this.validator = validator;
        this.timelineService = timelineService;
    }

    @KafkaListener(
            id = "audit-workorder-events",
            topics = "${digitalfix.kafka.topics.workorder-events}",
            groupId = "${digitalfix.kafka.consumer-group}",
            containerFactory = "workOrderEventListenerFactory",
            autoStartup = "${digitalfix.kafka.listeners-enabled:true}")
    public void onWorkOrderEvent(ConsumerRecord<String, WorkOrderEvent> record) {
        WorkOrderEvent event = record.value();
        try (var ignored = KafkaLogContext.open(record,
                event != null ? event.eventId() : null,
                event != null ? event.traceId() : null)) {
            validator.requireValid(event, WorkOrderEvent.class);
            timelineService.registerWorkOrderEvent(event,
                    EventOrigin.kafka(record.topic(), record.partition(), record.offset()));
        }
    }
}
