package cl.digitalfix.audit.messaging.kafka;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import jakarta.validation.Validator;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import cl.digitalfix.audit.messaging.kafka.ContratoEventos.SobreEvento;
import cl.digitalfix.audit.service.AuditoriaService;

// Consumidor de workorders.events (grupo "audit").
//
// Recorrido de cada mensaje:
//   1. Pone el correlationId en el MDC: todas las líneas de log de este evento lo muestran.
//   2. Convierte el JSON a SobreEvento y lo valida.
//        - Si no se puede (JSON roto, faltan campos) lanza EventoInvalido: va DIRECTO al DLT.
//   3. Lo guarda en Oracle con AuditoriaService.registrar (idempotente por eventId).
//        - Si Oracle falla, la excepción sale de aquí: se reintenta y, si sigue fallando, va al DLT.
//   4. Si el método termina sin excepción, el contenedor hace commit del offset.
//      Por eso el commit ocurre DESPUÉS de guardar: si ms-audit se cae a la mitad,
//      al volver relee el evento y la idempotencia evita el duplicado.
@Component
public class ConsumidorEventosOrden {
    private static final Logger log = LoggerFactory.getLogger(ConsumidorEventosOrden.class);

    private final AuditoriaService servicio;
    private final Validator validator;
    private final JsonMapper json = JsonMapper.builder().build();

    public ConsumidorEventosOrden(AuditoriaService servicio, Validator validator) {
        this.servicio = servicio;
        this.validator = validator;
    }

    // Compatibilidad para llamadas internas que representaban el tópico original.
    public void recibir(String mensaje, int particion, long offset, String key, byte[] correlationId) {
        recibir(mensaje, particion, offset, key, correlationId, "workorders.events");
    }

    @KafkaListener(
            id = "auditoria-eventos-orden",
            topics = {"${digitalfix.kafka.topico}", "${digitalfix.kafka.topico-ediciones:audit.timeline}"},
            groupId = "${digitalfix.kafka.group-id}")
    public void recibir(
            @Payload String mensaje,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(name = "correlationId", required = false) byte[] correlationId,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topico) {

        MDC.put("correlationId", correlationId == null ? "-" : new String(correlationId, StandardCharsets.UTF_8));
        try {
            log.info("evento recibido particion={} offset={} key={}", particion, offset, key);

            SobreEvento sobre = convertir(mensaje);
            validarCoherencia(sobre, topico, key, correlationId);
            boolean nuevo = servicio.registrar(sobre, topico, particion, offset);

            if (nuevo)
                log.info("evento guardado tipo={} eventId={} ordenId={}",
                        sobre.type(), sobre.eventId(), sobre.payload().ordenId());
            else
                log.info("evento duplicado ignorado eventId={}", sobre.eventId());
        } finally {
            // El hilo del consumidor se reutiliza: limpiar para no mezclar eventos en los logs
            MDC.remove("correlationId");
        }
    }

    // JSON -> SobreEvento + validación. Cualquier problema aquí es permanente (EventoInvalido).
    SobreEvento convertir(String mensaje) {
        if (mensaje == null || mensaje.isBlank()) throw new EventoInvalido("mensaje_vacio");
        SobreEvento sobre;
        try {
            sobre = json.readValue(mensaje, SobreEvento.class);
        } catch (JacksonException e) {
            throw new EventoInvalido("json_invalido", e);
        }
        if (sobre == null) throw new EventoInvalido("evento_nulo");
        var errores = validator.validate(sobre);
        if (!errores.isEmpty()) {
            String detalle = errores.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new EventoInvalido("evento_invalido: " + detalle);
        }
        return sobre;
    }

    private void validarCoherencia(SobreEvento sobre, String topico, String key, byte[] header) {
        var e = sobre.payload();
        String esperado = switch (e.accion()) {
            case "CREACION" -> "ORDEN_CREADA";
            case "CAMBIO_ESTADO" -> "ORDEN_ESTADO_CAMBIADO";
            case "ELIMINACION" -> "ORDEN_ELIMINADA";
            case "EDICION_DATOS" -> "ORDEN_DATOS_ACTUALIZADOS";
            case "EDICION_CONTACTOS" -> "ORDEN_CONTACTOS_ACTUALIZADOS";
            case "EDICION_REPUESTOS" -> "ORDEN_REPUESTOS_ACTUALIZADOS";
            default -> throw new EventoInvalido("accion_desconocida");
        };
        boolean edicion = e.accion().startsWith("EDICION_");
        if (!esperado.equals(sobre.type()) || !(edicion ? "audit.timeline" : "workorders.events").equals(topico))
            throw new EventoInvalido("tipo_accion_o_topico_incoherente");
        String orden = e.ordenId().toString();
        if (!orden.equals(key) || !orden.equals(sobre.correlationId()) || !sobre.timestamp().equals(e.fecha())
                || (header != null && !orden.equals(new String(header, StandardCharsets.UTF_8))))
            throw new EventoInvalido("identidad_o_fecha_incoherente");
        var estados = java.util.Set.of("CREADA", "ASIGNADA", "EN_DESPLAZAMIENTO", "EN_EJECUCION", "CERRADA", "CANCELADA");
        boolean anteriorValido = e.estadoAnterior() != null && estados.contains(e.estadoAnterior());
        boolean nuevoValido = e.estadoNuevo() != null && estados.contains(e.estadoNuevo());
        boolean valido = switch (e.accion()) {
            case "CREACION" -> e.estadoAnterior() == null && "CREADA".equals(e.estadoNuevo());
            case "ELIMINACION" -> ("CREADA".equals(e.estadoAnterior()) || "CANCELADA".equals(e.estadoAnterior())) && e.estadoNuevo() == null;
            case "CAMBIO_ESTADO" -> anteriorValido && nuevoValido && !e.estadoAnterior().equals(e.estadoNuevo());
            default -> anteriorValido && nuevoValido && e.estadoAnterior().equals(e.estadoNuevo());
        };
        if (!valido) throw new EventoInvalido("estados_incoherentes");
    }
}
