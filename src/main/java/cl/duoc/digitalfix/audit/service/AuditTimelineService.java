package cl.duoc.digitalfix.audit.service;

import cl.duoc.digitalfix.audit.entity.AuditEvent;
import cl.duoc.digitalfix.audit.messaging.event.AuditOutcome;
import cl.duoc.digitalfix.audit.messaging.event.AuditTimelineEvent;
import cl.duoc.digitalfix.audit.messaging.event.WorkOrderEvent;
import cl.duoc.digitalfix.audit.repository.AuditEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Logica de dominio del timeline. No conoce Kafka: recibe eventos ya
 * deserializados y validados, los traduce a un registro de auditoria y los
 * persiste una sola vez por eventId (el consumo es at-least-once, por lo que un
 * mismo evento puede llegar dos veces tras un rebalanceo o un reintento).
 */
@Service
public class AuditTimelineService {

    private static final Logger log = LoggerFactory.getLogger(AuditTimelineService.class);

    private final AuditEventRepository repository;

    public AuditTimelineService(AuditEventRepository repository) {
        this.repository = repository;
    }

    /** @return true si se registro; false si era un duplicado. */
    public boolean registerWorkOrderEvent(WorkOrderEvent event, EventOrigin origin) {
        String referencia = "orden#" + event.workOrderId();
        AuditEvent record = AuditEvent.fromEvent(
                event.eventId(),
                "WORKORDER_" + event.eventType().name(),
                event.actor().username(),
                event.actor().role(),
                describe(event),
                event.source(),
                referencia,
                null,
                AuditOutcome.SUCCESS.name(),
                event.occurredAt(),
                origin);
        return saveOnce(record);
    }

    /** @return true si se registro; false si era un duplicado. */
    public boolean registerTimelineEvent(AuditTimelineEvent event, EventOrigin origin) {
        String referencia = event.resourceId() == null || event.resourceId().isBlank()
                ? event.resourceType().toLowerCase()
                : event.resourceType().toLowerCase() + "#" + event.resourceId();
        AuditEvent record = AuditEvent.fromEvent(
                event.eventId(),
                event.action(),
                event.actor().username(),
                event.actor().role(),
                describe(event),
                event.source(),
                referencia,
                event.sourceIp(),
                event.outcome().name(),
                event.occurredAt(),
                origin);
        return saveOnce(record);
    }

    private boolean saveOnce(AuditEvent record) {
        if (repository.existsByEventId(record.getEventId())) {
            log.info("Evento duplicado ignorado (ya registrado en el timeline)");
            return false;
        }
        try {
            repository.save(record);
        } catch (DataIntegrityViolationException e) {
            // Carrera entre dos entregas del mismo evento: la restriccion unica protege el timeline
            log.info("Evento duplicado ignorado (restriccion unica event_id)");
            return false;
        }
        log.info("Timeline: {} | {} | {}", record.getReferencia(), record.getUsuario(), record.getAccion());
        return true;
    }

    private static String describe(WorkOrderEvent e) {
        String orden = "orden #" + e.workOrderId();
        return switch (e.eventType()) {
            case CREATED -> "CREO " + orden + (e.serviceCode() != null ? " (servicio " + e.serviceCode() + ")" : "");
            case ASSIGNED -> "ASIGNO " + orden + " a " + e.technician();
            case STATUS_CHANGED -> "CAMBIO ESTADO " + orden + ": " + e.previousStatus() + " -> " + e.status();
            case CLOSED -> "CERRO " + orden;
            case CANCELLED -> "CANCELO " + orden;
        };
    }

    private static String describe(AuditTimelineEvent e) {
        StringBuilder sb = new StringBuilder(e.action()).append(' ').append(e.resourceType());
        if (e.resourceId() != null && !e.resourceId().isBlank()) {
            sb.append(" #").append(e.resourceId());
        }
        if (e.outcome() != AuditOutcome.SUCCESS) {
            sb.append(" [").append(e.outcome()).append(']');
        }
        if (e.detail() != null && !e.detail().isBlank()) {
            sb.append(" - ").append(e.detail());
        }
        return sb.toString();
    }
}
