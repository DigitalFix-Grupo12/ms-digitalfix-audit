package cl.duoc.digitalfix.audit.service;

/**
 * Procedencia de un registro de auditoria: canal (KAFKA o REST) y, si vino por
 * streaming, su posicion exacta en el log (permite volver a leerlo en Kafka UI).
 */
public record EventOrigin(String canal, String topico, Integer particion, Long offset) {

    public static EventOrigin kafka(String topic, int partition, long offset) {
        return new EventOrigin("KAFKA", topic, partition, offset);
    }

    public static EventOrigin rest() {
        return new EventOrigin("REST", null, null, null);
    }
}
