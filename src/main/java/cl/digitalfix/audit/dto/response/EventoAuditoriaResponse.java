package cl.digitalfix.audit.dto.response;

import java.time.Instant;

import cl.digitalfix.audit.entity.EventoAuditoria;

// Lo que ve el Auditor de cada evento (no expone el id interno de la tabla).
public record EventoAuditoriaResponse(
        String eventId,
        String tipo,
        Long ordenId,
        String accion,
        String estadoAnterior,
        String estadoNuevo,
        String tecnicoId,
        String actorId,
        String actorRoles,
        Instant fecha,
        Instant recibidoEn,
        String correlationId,
        Integer kafkaParticion,
        Long kafkaOffset,
        String kafkaTopico) {

    public static EventoAuditoriaResponse desde(EventoAuditoria e) {
        return new EventoAuditoriaResponse(e.getEventId(), e.getTipo(), e.getOrdenId(), e.getAccion(),
                e.getEstadoAnterior(), e.getEstadoNuevo(), e.getTecnicoId(), e.getActorId(), e.getActorRoles(),
                e.getFecha(), e.getRecibidoEn(), e.getCorrelationId(), e.getKafkaParticion(), e.getKafkaOffset(), e.getKafkaTopico());
    }
}
