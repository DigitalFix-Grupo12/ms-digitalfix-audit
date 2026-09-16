package cl.duoc.digitalfix.audit.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record AuditEventRequest(
    @NotBlank String usuario,
    @NotBlank @Size(max = 500) String accion,
    @NotBlank @Size(max = 100) String servicio,
    @Size(max = 100) String referencia
) {}
