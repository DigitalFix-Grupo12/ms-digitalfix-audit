package cl.duoc.digitalfix.audit.messaging;

import cl.duoc.digitalfix.audit.entity.AuditEvent;
import cl.duoc.digitalfix.audit.repository.AuditEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Prueba de punta a punta del consumidor contra un broker Kafka embebido con los
 * mismos topicos (3 particiones) y DLT que digitalfix-infra/kafka.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "digitalfix.kafka.retry.initial-interval=100ms",
        "digitalfix.kafka.retry.max-interval=200ms",
        "digitalfix.kafka.retry.max-attempts=1"
})
@EmbeddedKafka(partitions = 3, topics = {
        AuditKafkaIntegrationTest.WORKORDERS,
        AuditKafkaIntegrationTest.TIMELINE,
        AuditKafkaIntegrationTest.WORKORDERS_DLT,
        AuditKafkaIntegrationTest.TIMELINE_DLT
})
class AuditKafkaIntegrationTest {

    static final String WORKORDERS = "workorders.events";
    static final String TIMELINE = "audit.timeline";
    static final String WORKORDERS_DLT = "workorders.events.audit-timeline.DLT";
    static final String TIMELINE_DLT = "audit.timeline.audit-timeline.DLT";

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private AuditEventRepository repository;

    private Producer<String, String> producer;

    @BeforeEach
    void setUp() {
        Map<String, Object> config = KafkaTestUtils.producerProps(broker);
        producer = new DefaultKafkaProducerFactory<>(config, new StringSerializer(), new StringSerializer())
                .createProducer();
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void eventoDeOrdenSeRegistraConSuPosicionEnElLog() {
        String eventId = UUID.randomUUID().toString();
        send(WORKORDERS, "41", workOrderEvent(eventId, 41, "STATUS_CHANGED",
                "\"previousStatus\":\"ASIGNADA\",\"status\":\"EN_DESPLAZAMIENTO\""));

        AuditEvent saved = awaitEvent(eventId);
        assertThat(saved.getReferencia()).isEqualTo("orden#41");
        assertThat(saved.getAccion()).isEqualTo("CAMBIO ESTADO orden #41: ASIGNADA -> EN_DESPLAZAMIENTO");
        assertThat(saved.getUsuario()).isEqualTo("supervisor.demo@digitalfix.test");
        assertThat(saved.getTipoEvento()).isEqualTo("WORKORDER_STATUS_CHANGED");
        assertThat(saved.getCanal()).isEqualTo("KAFKA");
        assertThat(saved.getTopico()).isEqualTo(WORKORDERS);
        assertThat(saved.getParticion()).isBetween(0, 2);
        assertThat(saved.getOffsetKafka()).isNotNull();
    }

    @Test
    void eventoRepetidoSeRegistraUnaSolaVez() {
        String eventId = UUID.randomUUID().toString();
        String payload = workOrderEvent(eventId, 42, "CREATED", "\"serviceCode\":\"ELEC-01\"");
        send(WORKORDERS, "42", payload);
        send(WORKORDERS, "42", payload);
        // Misma clave -> misma particion -> el marcador se procesa despues de ambos
        String marker = UUID.randomUUID().toString();
        send(WORKORDERS, "42", workOrderEvent(marker, 42, "CLOSED", ""));

        awaitEvent(marker);
        assertThat(repository.countByEventId(eventId)).isEqualTo(1);
    }

    @Test
    void payloadInvalidoVaALaDltSinReintentos() {
        String eventId = UUID.randomUUID().toString();
        // STATUS_CHANGED sin estados y sin actor: viola el contrato
        String invalid = """
                {"eventId":"%s","eventType":"STATUS_CHANGED","schemaVersion":1,
                 "occurredAt":"2026-11-18T12:00:00Z","source":"ms-digitalfix-workorders","workOrderId":43}
                """.formatted(eventId);
        send(WORKORDERS, "43", invalid);

        ConsumerRecord<String, String> dlt = readOne(WORKORDERS_DLT);
        assertThat(dlt.key()).isEqualTo("43");
        assertThat(dlt.value()).contains(eventId);
        assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN)).endsWith("InvalidEventException");
        assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("actor").contains("status");
        assertThat(header(dlt, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(WORKORDERS);
        assertThat(repository.existsByEventId(eventId)).isFalse();
    }

    @Test
    void jsonIlegibleVaALaDltYElConsumidorSigue() {
        send(TIMELINE, "orden-44", "{esto no es json");
        String eventId = UUID.randomUUID().toString();
        send(TIMELINE, "orden-44", timelineEvent(eventId, "44"));

        ConsumerRecord<String, String> dlt = readOne(TIMELINE_DLT);
        assertThat(dlt.value()).isEqualTo("{esto no es json");
        assertThat(header(dlt, KafkaHeaders.DLT_EXCEPTION_FQCN) + header(dlt, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("DeserializationException");

        AuditEvent saved = awaitEvent(eventId);
        assertThat(saved.getIpOrigen()).isEqualTo("200.1.2.3");
        assertThat(saved.getResultado()).isEqualTo("DENIED");
        assertThat(saved.getReferencia()).isEqualTo("workorder#44");
        assertThat(saved.getAccion()).isEqualTo("WORKORDER_STATUS_UPDATE WORKORDER #44 [DENIED] - rol Cliente sin permiso");
    }

    @Test
    void tombstoneDeCompactacionSeIgnora() {
        producer.send(new ProducerRecord<>(TIMELINE, "orden-45", null));
        String eventId = UUID.randomUUID().toString();
        send(TIMELINE, "orden-45", timelineEvent(eventId, "45"));

        awaitEvent(eventId);
    }

    // ---------------------------------------------------------------- helpers

    private void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
        producer.flush();
    }

    private AuditEvent awaitEvent(String eventId) {
        await().atMost(TIMEOUT).until(() -> repository.existsByEventId(eventId));
        return repository.findByEventId(eventId).orElseThrow();
    }

    private ConsumerRecord<String, String> readOne(String topic) {
        Map<String, Object> config = KafkaTestUtils.consumerProps("test-dlt-" + UUID.randomUUID(), "false", broker);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(config,
                new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, topic);
            return KafkaTestUtils.getSingleRecord(consumer, topic, TIMEOUT);
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String workOrderEvent(String eventId, long workOrderId, String type, String extraFields) {
        String extra = extraFields.isBlank() ? "" : "," + extraFields;
        return """
                {"eventId":"%s","eventType":"%s","schemaVersion":1,"occurredAt":"2026-11-18T12:00:00Z",
                 "traceId":"trace-%s","source":"ms-digitalfix-workorders","workOrderId":%d,
                 "actor":{"id":"oid-sup","username":"supervisor.demo@digitalfix.test","role":"Supervisor"}%s}
                """.formatted(eventId, type, eventId.substring(0, 8), workOrderId, extra);
    }

    private static String timelineEvent(String eventId, String resourceId) {
        return """
                {"eventId":"%s","schemaVersion":1,"occurredAt":"2026-11-18T12:05:00Z","traceId":"t-1",
                 "source":"ms-digitalfix-bff",
                 "actor":{"id":"oid-cli","username":"cliente.demo@digitalfix.test","role":"Cliente"},
                 "action":"WORKORDER_STATUS_UPDATE","resourceType":"WORKORDER","resourceId":"%s",
                 "sourceIp":"200.1.2.3","outcome":"DENIED","detail":"rol Cliente sin permiso"}
                """.formatted(eventId, resourceId);
    }
}
