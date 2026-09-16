package cl.duoc.digitalfix.audit.repository;

import cl.duoc.digitalfix.audit.entity.AuditEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {
    List<AuditEvent> findAllByOrderByFechaDesc(Pageable pageable);
    List<AuditEvent> findByReferenciaOrderByFechaDesc(String referencia);
}
