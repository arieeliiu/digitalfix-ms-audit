# digitalfix-ms-audit

Contexto global en `../CLAUDE.md`. Aquí va solo lo propio de este servicio.

Consumidor Kafka de `workorders.events` (grupo `audit`) que guarda los eventos en Oracle
(`auditoria_eventos`) y expone `/api/audit/*` de **solo lectura** para Admin y Auditor. Puerto **8083**.
Detalle de clases, API y uso local en `README.md`.

## Decisiones (23/11/2026)

- Consume **solo** `workorders.events`. `audit.timeline` queda creado en infra pero sin uso
  (`compact` conservaría solo el último evento por key). Republicar ahí es opcional si la pauta lo exige.
- Contrato: copia en `messaging/kafka/ContratoEventos` del que publica Workorders
  (`digitalfix-ms-workorders/docs/MENSAJERIA.md`). Si Workorders cambia el evento, actualizar aquí **antes**.
  Tipos aceptados: `ORDEN_CREADA`, `ORDEN_ESTADO_CAMBIADO`, `ORDEN_ELIMINADA`; otro tipo va al DLT.
- No guarda "desde dónde": ese dato no viaja en el evento.

## Invariantes (no romper)

- **Commit del offset después de guardar** (`enable.auto.commit=false` + `AckMode.RECORD`). Nunca antes.
- **Idempotencia por `eventId`**: `existsByEventId` + restricción única `uk_auditoria_eventos_event_id`.
  `AuditoriaService.registrar` no es `@Transactional` a propósito: `saveAndFlush` detecta el choque en esa línea.
- **Errores:** `EventoInvalido` (JSON roto, validación, tipo desconocido) va directo al DLT sin reintentos.
  Cualquier otra excepción se reintenta (`KAFKA_REINTENTOS`, `KAFKA_ESPERA_REINTENTO`) y luego va al DLT.
- **DLT propio:** `workorders.events.audit.DLT`, misma partición de origen (`KafkaConsumerConfig.destinoDlt`).
  No usar el `<tópico>.DLT` por defecto de Spring: lo compartiría con ms-report.
- Tabla inmutable: solo inserts. La API no tiene POST, PUT ni DELETE.
- Roles: igual que Workorders, se revisa el claim `roles` del JWT en el controller (401 sin token, 403 sin Admin/Auditor).
- `@EnableKafka` es necesario: el proyecto usa `spring-kafka` sin la autoconfiguración de Boot.

## Variables de entorno

**Credenciales locales:** igual que Catalog y Workorders, `src/main/resources/application.properties` tiene en el
working tree el endpoint, el usuario y la contraseña de la RDS como valores por defecto de `${DB_URL:...}`,
`${DB_USERNAME:...}` y `${DB_PASSWORD:...}`. **No se commitean**: la versión versionada usa `${DB_URL:}` (vacío).
Al commitear, deja fuera esas tres líneas y nunca copies la contraseña a otro archivo, log o respuesta.

`DB_URL`, `DB_USERNAME`, `DB_PASSWORD`,
`DIGITALFIX_KAFKA_ENABLED` (true por defecto), `KAFKA_BOOTSTRAP_SERVERS` (local `localhost:9092,9093,9094`),
`KAFKA_GROUP_ID` (`audit`), `KAFKA_REINTENTOS` (3) y `KAFKA_ESPERA_REINTENTO` (1s). Ver `.env.example`.

## Tests

`.\mvnw.cmd -B test`: H2 en modo Oracle, listener apagado (`digitalfix.kafka.enabled=false`) y el consumidor
llamado directamente. Suites: `ConsumidorEventosOrdenTests` (guardado, duplicados, inválidos, timeline y búsqueda),
`AuditoriaControllerTests` (roles y parámetros) y `KafkaConsumerConfigTests` (DLT y reintentos con `handleOne`).

## Pendientes

- Proxy en el BFF (`/api/audit/**`), rol `Auditor` en Entra y pantalla `/audit` en el frontend.
- Servicio `audit` en `digitalfix-infra/apps/compose.yml` (despliegue).
- Reinyección desde el DLT (microservicio administrador de Kafka).
