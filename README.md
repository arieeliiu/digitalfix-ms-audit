# digitalfix-ms-audit

Auditoría de las órdenes de trabajo de DigitalFix. Lee los eventos que publica Workorders en Kafka
(`workorders.events`), los guarda en Oracle y los expone por una API **de solo lectura** para los
roles **Admin** y **Auditor**.

```
Workorders ──▶ workorders.events ──(grupo "audit")──▶ ms-audit ──▶ Oracle (auditoria_eventos)
                                                          │            ▲
                                    falla permanente ─────┘            │
                                    o reintentos agotados              │
                                          ▼                            │
                              workorders.events.audit.DLT     GET /api/audit/... (Admin, Auditor)
```

Puerto **8083**. Java 21, Spring Boot 4.1.1.

## Cómo procesa cada evento

1. `ConsumidorEventosOrden` recibe el mensaje (grupo `audit`, `auto-offset-reset=earliest`).
2. Convierte el JSON a `SobreEvento` y lo valida. Si no se puede → `EventoInvalido` → **directo al DLT**.
3. `AuditoriaService.registrar` lo guarda. Si el `eventId` ya existe → **duplicado ignorado** (idempotencia).
4. Si Oracle falla → se **reintenta** 3 veces con 1 s de espera; si sigue fallando → **DLT**.
   Un error de integridad solo se ignora si el `eventId` ya está persistido; los demás fallos se propagan.
5. Solo si todo salió bien se hace **commit del offset** (`AckMode.RECORD`).

| Clase | Responsabilidad |
|---|---|
| `config/KafkaConsumerConfig` | Consumidor, productor del DLT, manejador de errores (reintentos + DLT) |
| `messaging/kafka/ConsumidorEventosOrden` | `@KafkaListener`, MDC con `correlationId`, conversión y validación |
| `messaging/kafka/ContratoEventos` | Copia del contrato de Workorders (`docs/MENSAJERIA.md` de Workorders) |
| `messaging/kafka/EventoInvalido` | Error permanente: no se reintenta |
| `service/AuditoriaService` | Guardar (idempotente), timeline y búsqueda |
| `entity/EventoAuditoria` | Tabla `auditoria_eventos`, `event_id` único |
| `controller/AuditoriaController` | `/api/audit/*`, roles Admin y Auditor |

## API (solo lectura)

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/audit/ordenes/{ordenId}/timeline` | Eventos de una orden, del más antiguo al más reciente |
| GET | `/api/audit/eventos?ordenId=&actorId=&tipo=&desde=&hasta=&pagina=0&tamano=20` | Búsqueda con filtros opcionales, del más reciente al más antiguo. `tamano` máximo 100; fechas ISO-8601 (`2026-11-23T00:00:00Z`) |

Sin token: **401**. Token sin rol `Admin` ni `Auditor`: **403**.

## Correr en local (IntelliJ)

1. Levantar Kafka: en `digitalfix-infra`, `docker compose -f kafka/compose.yml up -d`.
2. En *Run → Edit Configurations → Environment variables*, definir `DB_URL`, `DB_USERNAME` y `DB_PASSWORD`
   (la misma RDS que Workorders). Kafka usa `localhost:9092,9093,9094` por defecto.
3. Arrancar `AuditApplication`. Hibernate crea la tabla `auditoria_eventos` (`ddl-auto=update`).
4. Al ser un grupo nuevo, lee desde el primer evento que siga en el tópico (retención de 7 días).
   En el log: `evento recibido ...` y `evento guardado ...`, con `[cid:<ordenId>]`.
5. En Kafka UI (`localhost:8090`) → Consumers → `audit`: el **lag** debe quedar en 0.

Variables: ver [.env.example](.env.example).

## Tests

```powershell
.\mvnw.cmd -B test
```

H2 en modo Oracle y Kafka simulado (el listener no arranca en tests):
`ConsumidorEventosOrdenTests`, `AuditoriaControllerTests` y `KafkaConsumerConfigTests` (DLT y reintentos).
`IntegridadAuditoriaTests` comprueba restricciones H2 reales, reentrega y colisión concurrente
de identidad. No certifica esas transacciones contra Oracle ni el envío real a DLT.

## Limitaciones conocidas

- "Desde dónde" (IP u origen) no viaja en el evento de Workorders, así que no se registra.
- `audit.timeline` existe en el clúster pero no se usa: el historial completo vive en Oracle
  (`compact` conservaría solo el último evento por key).
- Reinyectar mensajes desde el DLT es manual por ahora (candidato para el microservicio administrador de Kafka).
