package cl.digitalfix.audit.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

// Un evento de una orden tal como llegó desde Kafka. Es la "línea de tiempo" que consulta el Auditor.
//
// - Inmutable: solo se inserta, nunca se actualiza ni se borra (no hay setters).
// - event_id es ÚNICO: es la base de la idempotencia. Si Kafka entrega el mismo
//   evento dos veces, el segundo insert choca con esta restricción y se ignora.
// - Guarda partición y offset de Kafka: permite rastrear de dónde salió cada fila.
@Entity
@Table(name = "auditoria_eventos",
        uniqueConstraints = @UniqueConstraint(name = "uk_auditoria_eventos_event_id", columnNames = "event_id"),
        indexes = {
                @Index(name = "ix_auditoria_eventos_orden", columnList = "orden_id"),
                @Index(name = "ix_auditoria_eventos_actor", columnList = "actor_id"),
                @Index(name = "ix_auditoria_eventos_fecha", columnList = "fecha")
        })
public class EventoAuditoria {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // --- Identidad del evento (del Sobre) ---
    @Column(name = "event_id", nullable = false, updatable = false, length = 36)
    private String eventId;

    // ORDEN_CREADA, ORDEN_ESTADO_CAMBIADO u ORDEN_ELIMINADA
    @Column(nullable = false, updatable = false, length = 40)
    private String tipo;

    @Column(name = "correlation_id", updatable = false, length = 100)
    private String correlationId;

    @Column(name = "trace_id", updatable = false, length = 36)
    private String traceId;

    // --- Qué pasó (del payload EventoOrden) ---
    @Column(name = "orden_id", nullable = false, updatable = false)
    private Long ordenId;

    // CREACION, CAMBIO_ESTADO o ELIMINACION
    @Column(nullable = false, updatable = false, length = 20)
    private String accion;

    @Column(name = "estado_anterior", updatable = false, length = 30)
    private String estadoAnterior;

    @Column(name = "estado_nuevo", updatable = false, length = 30)
    private String estadoNuevo;

    @Column(name = "tecnico_id", updatable = false, length = 100)
    private String tecnicoId;

    // --- Quién lo hizo ---
    // oid de Entra del usuario que ejecutó la acción
    @Column(name = "actor_id", nullable = false, updatable = false, length = 100)
    private String actorId;

    @Column(name = "actor_roles", updatable = false, length = 100)
    private String actorRoles;

    // --- Cuándo ---
    // Cuándo ocurrió en Workorders
    @Column(nullable = false, updatable = false)
    private Instant fecha;

    // Cuándo lo guardó ms-audit (la diferencia con "fecha" muestra el retraso del consumidor)
    @Column(name = "recibido_en", nullable = false, updatable = false)
    private Instant recibidoEn;

    // --- De dónde salió en Kafka ---
    // "offset" es palabra reservada en Oracle: por eso la columna es kafka_offset
    @Column(name = "kafka_particion", nullable = false, updatable = false)
    private Integer kafkaParticion;

    @Column(name = "kafka_offset", nullable = false, updatable = false)
    private Long kafkaOffset;

    // JPA necesita un constructor vacío
    protected EventoAuditoria() {}

    public EventoAuditoria(String eventId, String tipo, String correlationId, String traceId,
            Long ordenId, String accion, String estadoAnterior, String estadoNuevo, String tecnicoId,
            String actorId, String actorRoles, Instant fecha, int kafkaParticion, long kafkaOffset) {
        this.eventId = eventId;
        this.tipo = tipo;
        this.correlationId = correlationId;
        this.traceId = traceId;
        this.ordenId = ordenId;
        this.accion = accion;
        this.estadoAnterior = estadoAnterior;
        this.estadoNuevo = estadoNuevo;
        this.tecnicoId = tecnicoId;
        this.actorId = actorId;
        this.actorRoles = actorRoles;
        this.fecha = fecha;
        this.recibidoEn = Instant.now();
        this.kafkaParticion = kafkaParticion;
        this.kafkaOffset = kafkaOffset;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getTipo() { return tipo; }
    public String getCorrelationId() { return correlationId; }
    public String getTraceId() { return traceId; }
    public Long getOrdenId() { return ordenId; }
    public String getAccion() { return accion; }
    public String getEstadoAnterior() { return estadoAnterior; }
    public String getEstadoNuevo() { return estadoNuevo; }
    public String getTecnicoId() { return tecnicoId; }
    public String getActorId() { return actorId; }
    public String getActorRoles() { return actorRoles; }
    public Instant getFecha() { return fecha; }
    public Instant getRecibidoEn() { return recibidoEn; }
    public Integer getKafkaParticion() { return kafkaParticion; }
    public Long getKafkaOffset() { return kafkaOffset; }
}
