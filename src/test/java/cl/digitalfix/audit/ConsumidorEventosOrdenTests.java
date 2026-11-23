package cl.digitalfix.audit;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import cl.digitalfix.audit.messaging.kafka.ConsumidorEventosOrden;
import cl.digitalfix.audit.messaging.kafka.EventoInvalido;
import cl.digitalfix.audit.repository.EventoAuditoriaRepository;
import cl.digitalfix.audit.service.AuditoriaService;

import static org.junit.jupiter.api.Assertions.*;

// El consumidor real con H2, llamado directamente (sin broker): así se prueba
// lo que pasa con cada mensaje, sin depender de Kafka.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ConsumidorEventosOrdenTests {
    @Autowired ConsumidorEventosOrden consumidor;
    @Autowired AuditoriaService servicio;
    @Autowired EventoAuditoriaRepository repositorio;

    @BeforeEach void limpiar() { repositorio.deleteAll(); }

    // Mismo formato que publica Workorders (docs/MENSAJERIA.md)
    static String evento(UUID eventId, String tipo, long ordenId, String accion, String anterior,
                         String nuevo, String actorId, String fecha) {
        return """
            {"schemaVersion":"1.0","type":"%s","eventId":"%s","timestamp":"%s",
             "traceId":"%s","correlationId":"%d",
             "payload":{"ordenId":%d,"accion":"%s","estadoAnterior":%s,"estadoNuevo":%s,
                        "tecnicoId":null,"actorId":"%s","actorRoles":"Admin","fecha":"%s"}}
            """.formatted(tipo, eventId, fecha, UUID.randomUUID(), ordenId, ordenId, accion,
                anterior == null ? "null" : "\"" + anterior + "\"",
                nuevo == null ? "null" : "\"" + nuevo + "\"", actorId, fecha);
    }

    static String creada(UUID id, long orden, String fecha) {
        return evento(id, "ORDEN_CREADA", orden, "CREACION", null, "CREADA", "oid-cliente", fecha);
    }

    void recibir(String mensaje, long offset) {
        consumidor.recibir(mensaje, 0, offset, "22", "22".getBytes());
    }

    @Test void eventoValidoSeGuardaConParticionYOffset() {
        var id = UUID.randomUUID();
        recibir(creada(id, 22, "2026-11-23T03:40:49Z"), 5);

        var guardado = repositorio.findAll().getFirst();
        assertEquals(id.toString(), guardado.getEventId());
        assertEquals("ORDEN_CREADA", guardado.getTipo());
        assertEquals(22L, guardado.getOrdenId());
        assertEquals("CREADA", guardado.getEstadoNuevo());
        assertEquals("oid-cliente", guardado.getActorId());
        assertEquals(Instant.parse("2026-11-23T03:40:49Z"), guardado.getFecha());
        assertEquals(0, guardado.getKafkaParticion());
        assertEquals(5L, guardado.getKafkaOffset());
        assertNotNull(guardado.getRecibidoEn());
    }

    // Idempotencia: Kafka puede entregar el mismo mensaje dos veces (at-least-once)
    @Test void mismoEventIdDosVecesSeGuardaUnaSola() {
        var mensaje = creada(UUID.randomUUID(), 22, "2026-11-23T03:40:49Z");
        recibir(mensaje, 0);
        recibir(mensaje, 0);
        assertEquals(1, repositorio.count());
    }

    // Errores permanentes: deben ser EventoInvalido para ir directo al DLT
    @Test void jsonRotoEsEventoInvalido() {
        var e = assertThrows(EventoInvalido.class, () -> recibir("{esto no es json", 0));
        assertEquals("json_invalido", e.getMessage());
        assertEquals(0, repositorio.count());
    }

    @Test void mensajeVacioEsEventoInvalido() {
        assertThrows(EventoInvalido.class, () -> recibir("  ", 0));
    }

    @Test void faltaActorEsEventoInvalido() {
        var sinActor = evento(UUID.randomUUID(), "ORDEN_CREADA", 22, "CREACION", null, "CREADA", "", "2026-11-23T03:40:49Z");
        var e = assertThrows(EventoInvalido.class, () -> recibir(sinActor, 0));
        assertTrue(e.getMessage().contains("payload.actorId"), e.getMessage());
    }

    @Test void tipoDesconocidoEsEventoInvalido() {
        var raro = evento(UUID.randomUUID(), "ORDEN_PINTADA", 22, "CREACION", null, "CREADA", "oid", "2026-11-23T03:40:49Z");
        assertThrows(EventoInvalido.class, () -> recibir(raro, 0));
    }

    @Test void timelineEnOrdenCronologicoYBusquedaConFiltros() {
        // Llegan desordenados a propósito: el timeline ordena por fecha del evento
        recibir(evento(UUID.randomUUID(), "ORDEN_ESTADO_CAMBIADO", 22, "CAMBIO_ESTADO", "CREADA", "ASIGNADA",
                "oid-operador", "2026-11-23T03:42:49Z"), 1);
        recibir(creada(UUID.randomUUID(), 22, "2026-11-23T03:40:49Z"), 0);
        recibir(creada(UUID.randomUUID(), 23, "2026-11-23T04:00:00Z"), 2);

        var timeline = servicio.timeline(22L);
        assertEquals(2, timeline.size());
        assertEquals("CREADA", timeline.get(0).estadoNuevo());
        assertEquals("ASIGNADA", timeline.get(1).estadoNuevo());

        assertEquals(1, servicio.buscar(null, "oid-operador", null, null, null, 0, 20).total());
        assertEquals(2, servicio.buscar(null, null, "ORDEN_CREADA", null, null, 0, 20).total());
        assertEquals(1, servicio.buscar(null, null, null, Instant.parse("2026-11-23T03:50:00Z"), null, 0, 20).total());
        var todos = servicio.buscar(null, null, null, null, null, 0, 20);
        assertEquals(3, todos.total());
        assertEquals(23L, todos.contenido().getFirst().ordenId()); // el más reciente primero
    }
}
