package cl.duoc.digitalfix.audit.entity;

import jakarta.persistence.*;
import java.time.Instant;

/** Evento inmutable de auditoria (append-only: no hay update ni delete). */
@Entity
@Table(name = "audit_events", indexes = @Index(name = "idx_audit_fecha", columnList = "fecha"))
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String usuario;

    @Column(nullable = false, length = 500)
    private String accion;

    /** Microservicio que origino el evento. */
    @Column(nullable = false, length = 100)
    private String servicio;

    /** Recurso afectado, ej. "orden#12". */
    @Column(length = 100)
    private String referencia;

    @Column(nullable = false, updatable = false)
    private Instant fecha;

    protected AuditEvent() {}

    public AuditEvent(String usuario, String accion, String servicio, String referencia) {
        this.usuario = usuario;
        this.accion = accion;
        this.servicio = servicio;
        this.referencia = referencia;
    }

    @PrePersist
    void onCreate() {
        if (fecha == null) fecha = Instant.now();
    }

    public Long getId() { return id; }
    public String getUsuario() { return usuario; }
    public String getAccion() { return accion; }
    public String getServicio() { return servicio; }
    public String getReferencia() { return referencia; }
    public Instant getFecha() { return fecha; }
}
