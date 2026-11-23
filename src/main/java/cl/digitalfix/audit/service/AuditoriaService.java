package cl.digitalfix.audit.service;

import java.time.Instant;
import java.util.List;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import cl.digitalfix.audit.dto.response.EventoAuditoriaResponse;
import cl.digitalfix.audit.dto.response.PaginaResponse;
import cl.digitalfix.audit.entity.EventoAuditoria;
import cl.digitalfix.audit.messaging.kafka.ContratoEventos.SobreEvento;
import cl.digitalfix.audit.repository.EventoAuditoriaRepository;

@Service
public class AuditoriaService {

    public static final int TAMANO_MAXIMO = 100;

    private final EventoAuditoriaRepository repositorio;

    public AuditoriaService(EventoAuditoriaRepository repositorio) {
        this.repositorio = repositorio;
    }

    // Guarda un evento recibido de Kafka. Devuelve true si es nuevo y false si es un duplicado.
    //
    // Idempotencia en dos niveles ("check-then-act"):
    //  1. existsByEventId: el caso normal de un duplicado (ya estaba guardado).
    //  2. La restricción única de event_id: si dos consumidores intentan guardar el
    //     mismo evento a la vez, la base rechaza el segundo y también lo tratamos como duplicado.
    //
    // Sin @Transactional aquí: saveAndFlush tiene su propia transacción, así que el choque
    // con la restricción se detecta en esta misma línea y no al final de otra transacción.
    public boolean registrar(SobreEvento sobre, int particion, long offset) {
        return registrar(sobre, "workorders.events", particion, offset);
    }

    public boolean registrar(SobreEvento sobre, String topico, int particion, long offset) {
        String eventId = sobre.eventId().toString();
        if (repositorio.existsByEventId(eventId)) return false;

        var e = sobre.payload();
        var registro = new EventoAuditoria(eventId, sobre.type(), sobre.correlationId(),
                sobre.traceId() == null ? null : sobre.traceId().toString(),
                e.ordenId(), e.accion(), e.estadoAnterior(), e.estadoNuevo(), e.tecnicoId(),
                e.actorId(), e.actorRoles(), e.fecha(), particion, offset, topico);
        try {
            repositorio.saveAndFlush(registro);
            return true;
        } catch (DataIntegrityViolationException duplicado) {
            return false;
        }
    }

    // Timeline de una orden, del evento más antiguo al más reciente.
    public List<EventoAuditoriaResponse> timeline(Long ordenId) {
        return repositorio.findByOrdenIdOrderByFechaAscIdAsc(ordenId).stream()
                .map(EventoAuditoriaResponse::desde)
                .toList();
    }

    // Búsqueda paginada, del más reciente al más antiguo. Los filtros en null no se aplican.
    public PaginaResponse<EventoAuditoriaResponse> buscar(Long ordenId, String actorId, String tipo,
            Instant desde, Instant hasta, int pagina, int tamano) {
        var resultado = repositorio.buscar(ordenId, vacioANull(actorId), vacioANull(tipo), desde, hasta,
                PageRequest.of(pagina, tamano));
        return new PaginaResponse<>(
                resultado.getContent().stream().map(EventoAuditoriaResponse::desde).toList(),
                pagina, tamano, resultado.getTotalElements());
    }

    private static String vacioANull(String valor) {
        return valor == null || valor.isBlank() ? null : valor.trim();
    }
}
