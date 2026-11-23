package cl.digitalfix.audit.repository;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import cl.digitalfix.audit.entity.EventoAuditoria;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, Long> {

    // Idempotencia: ¿ya guardamos este evento?
    boolean existsByEventId(String eventId);

    // Timeline de una orden: del evento más antiguo al más reciente
    List<EventoAuditoria> findByOrdenIdOrderByFechaAscIdAsc(Long ordenId);

    // Búsqueda con filtros opcionales: un filtro en null no se aplica.
    @Query("""
            select e from EventoAuditoria e
            where (:ordenId is null or e.ordenId = :ordenId)
              and (:actorId is null or e.actorId = :actorId)
              and (:tipo is null or e.tipo = :tipo)
              and (:desde is null or e.fecha >= :desde)
              and (:hasta is null or e.fecha <= :hasta)
            order by e.fecha desc, e.id desc
            """)
    Page<EventoAuditoria> buscar(@Param("ordenId") Long ordenId,
                                 @Param("actorId") String actorId,
                                 @Param("tipo") String tipo,
                                 @Param("desde") Instant desde,
                                 @Param("hasta") Instant hasta,
                                 Pageable pagina);
}
