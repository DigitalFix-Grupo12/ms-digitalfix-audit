package cl.duoc.digitalfix.audit.controller;

import cl.duoc.digitalfix.audit.dto.AuditEventRequest;
import cl.duoc.digitalfix.audit.entity.AuditEvent;
import cl.duoc.digitalfix.audit.repository.AuditEventRepository;
import jakarta.validation.Valid;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Timeline de auditoria. API interna (solo la consumen el BFF y los demas
 * microservicios por localhost; el puerto no se expone publicamente).
 *  - GET  /api/audit           -> ultimos N eventos (lectura para el BFF)
 *  - POST /api/audit           -> ingesta de eventos desde otros servicios
 * En la evaluacion de streaming el POST se reemplaza por un consumer Kafka.
 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private static final int MAX_LIMIT = 500;
    private final AuditEventRepository repository;

    public AuditController(AuditEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<AuditEvent> timeline(
            @RequestParam(required = false) String referencia,
            @RequestParam(defaultValue = "100") int limit) {
        if (referencia != null && !referencia.isBlank()) {
            return repository.findByReferenciaOrderByFechaDesc(referencia);
        }
        int size = Math.max(1, Math.min(limit, MAX_LIMIT));
        return repository.findAllByOrderByFechaDesc(PageRequest.of(0, size));
    }

    @PostMapping
    public ResponseEntity<AuditEvent> record(@Valid @RequestBody AuditEventRequest req) {
        AuditEvent saved = repository.save(
            new AuditEvent(req.usuario(), req.accion(), req.servicio(), req.referencia()));
        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }
}
