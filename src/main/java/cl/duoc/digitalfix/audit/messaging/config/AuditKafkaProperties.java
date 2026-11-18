package cl.duoc.digitalfix.audit.messaging.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Parametros de mensajeria Kafka del microservicio ({@code digitalfix.kafka.*}).
 * Unica fuente de nombres de topicos, consumer group, sufijo DLT y politica de reintentos.
 */
@Validated
@ConfigurationProperties(prefix = "digitalfix.kafka")
public record AuditKafkaProperties(
        boolean listenersEnabled,
        @NotBlank String consumerGroup,
        @Min(1) @Max(12) int concurrency,
        @Valid @NotNull Topics topics,
        @NotBlank String dltSuffix,
        @Valid @NotNull Retry retry) {

    public record Topics(@NotBlank String workorderEvents, @NotBlank String auditTimeline) {}

    /** Reintentos con backoff exponencial antes de enviar el registro a su DLT. */
    public record Retry(
            @Min(0) @Max(10) int maxAttempts,
            @NotNull Duration initialInterval,
            @DecimalMin("1.0") double multiplier,
            @NotNull Duration maxInterval) {}

    /** DLT de este consumer group para un topico: {@code <topico>.<grupo>.DLT}. */
    public String dltFor(String topic) {
        return topic + "." + consumerGroup + dltSuffix;
    }
}
