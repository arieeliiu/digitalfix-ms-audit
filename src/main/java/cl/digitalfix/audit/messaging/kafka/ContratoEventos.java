package cl.digitalfix.audit.messaging.kafka;

import java.time.Instant;
import java.util.UUID;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

// Contrato de los eventos que publica Workorders en workorders.events.
// Es una COPIA del contrato del productor (digitalfix-ms-workorders/docs/MENSAJERIA.md):
// si Workorders cambia el evento, hay que actualizar esto primero.
public final class ContratoEventos {
    private ContratoEventos() {}

    // Sobre común (el mismo formato que usa RabbitMQ)
    public record SobreEvento(
            String schemaVersion,
            @NotBlank @Pattern(regexp = "ORDEN_CREADA|ORDEN_ESTADO_CAMBIADO|ORDEN_ELIMINADA") String type,
            @NotNull UUID eventId,          // único por evento: clave de la idempotencia
            Instant timestamp,
            UUID traceId,
            @Size(max = 100) String correlationId,
            @NotNull @Valid EventoOrden payload) {}

    // Payload: quién hizo qué, sobre qué orden y cuándo
    public record EventoOrden(
            @NotNull @Positive Long ordenId,
            @NotBlank @Pattern(regexp = "CREACION|CAMBIO_ESTADO|ELIMINACION") String accion,
            @Size(max = 30) String estadoAnterior,
            @Size(max = 30) String estadoNuevo,
            @Size(max = 100) String tecnicoId,
            @NotBlank @Size(max = 100) String actorId,
            @Size(max = 100) String actorRoles,
            @NotNull Instant fecha) {}
}
