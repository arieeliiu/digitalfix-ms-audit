package cl.digitalfix.audit.messaging.kafka;

// Error PERMANENTE: el evento nunca se podrá procesar (JSON roto, faltan campos, tipo desconocido).
// Reintentarlo no sirve, así que el manejador de errores lo manda directo al DLT
// (ver KafkaConsumerConfig.manejadorErrores).
public class EventoInvalido extends RuntimeException {
    public EventoInvalido(String motivo) { super(motivo); }
    public EventoInvalido(String motivo, Throwable causa) { super(motivo, causa); }
}
