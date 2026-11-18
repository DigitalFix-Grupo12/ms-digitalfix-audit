package cl.duoc.digitalfix.audit.entity;

import cl.duoc.digitalfix.audit.service.EventOrigin;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Evento inmutable de auditoria (append-only: no hay update ni delete).
 * Los registros que llegan por Kafka guardan ademas su eventId (idempotencia) y
 * su posicion en el log (topico, particion, offset) para trazarlos de vuelta.
 */
@Entity
@Table(name = "audit_events",
       indexes = {
           @Index(name = "idx_audit_fecha", columnList = "fecha"),
           @Index(name = "idx_audit_referencia", columnList = "referencia")
       },
       uniqueConstraints = @UniqueConstraint(name = "uk_audit_event_id", columnNames = "event_id"))
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

    /** Fecha del hecho (occurredAt del evento), no la de su lectura. */
    @Column(nullable = false, updatable = false)
    private Instant fecha;

    /** Id unico del evento de origen; null en registros REST antiguos. */
    @Column(name = "event_id", length = 64, updatable = false)
    private String eventId;

    /** Tipo de evento, ej. WORKORDER_STATUS_CHANGED o una accion del BFF. */
    @Column(name = "tipo_evento", length = 60, updatable = false)
    private String tipoEvento;

    @Column(name = "rol_usuario", length = 30, updatable = false)
    private String rolUsuario;

    @Column(name = "ip_origen", length = 45, updatable = false)
    private String ipOrigen;

    /** SUCCESS, DENIED o ERROR. */
    @Column(length = 10, updatable = false)
    private String resultado;

    /** KAFKA o REST. */
    @Column(length = 10, updatable = false)
    private String canal;

    @Column(length = 120, updatable = false)
    private String topico;

    @Column(updatable = false)
    private Integer particion;

    @Column(name = "offset_kafka", updatable = false)
    private Long offsetKafka;

    protected AuditEvent() {}

    /** Ingesta sincrona por REST (canal de compatibilidad). */
    public AuditEvent(String usuario, String accion, String servicio, String referencia) {
        this.usuario = usuario;
        this.accion = accion;
        this.servicio = servicio;
        this.referencia = referencia;
        applyOrigin(EventOrigin.rest());
    }

    /** Registro construido a partir de un evento de streaming. */
    public static AuditEvent fromEvent(String eventId, String tipoEvento, String usuario, String rolUsuario,
                                       String accion, String servicio, String referencia, String ipOrigen,
                                       String resultado, Instant fecha, EventOrigin origin) {
        AuditEvent e = new AuditEvent();
        e.eventId = eventId;
        e.tipoEvento = tipoEvento;
        e.usuario = usuario;
        e.rolUsuario = rolUsuario;
        e.accion = accion.length() > 500 ? accion.substring(0, 497) + "..." : accion;
        e.servicio = servicio;
        e.referencia = referencia;
        e.ipOrigen = ipOrigen;
        e.resultado = resultado;
        e.fecha = fecha;
        e.applyOrigin(origin);
        return e;
    }

    private void applyOrigin(EventOrigin origin) {
        this.canal = origin.canal();
        this.topico = origin.topico();
        this.particion = origin.particion();
        this.offsetKafka = origin.offset();
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
    public String getEventId() { return eventId; }
    public String getTipoEvento() { return tipoEvento; }
    public String getRolUsuario() { return rolUsuario; }
    public String getIpOrigen() { return ipOrigen; }
    public String getResultado() { return resultado; }
    public String getCanal() { return canal; }
    public String getTopico() { return topico; }
    public Integer getParticion() { return particion; }
    public Long getOffsetKafka() { return offsetKafka; }
}
