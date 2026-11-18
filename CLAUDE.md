# digitalfix-ms-audit

Contexto global en `../CLAUDE.md`.

**Estado: vacío** (solo README). Hay que construirlo desde cero.

## Qué pide el caso

- Consumir Kafka (`workorders.events` y/o `audit.timeline`) y persistir los eventos de cada orden en Oracle: quién, qué, cuándo y desde dónde.
- Exponer `/api/audit/*` **de solo lectura**: el timeline de una orden y una búsqueda con filtros por usuario, fechas y tipo de evento.
- Roles: Admin y Auditor (el rol Auditor aún no existe en Entra, BFF ni frontend).
- `audit.timeline`: 3 particiones, 3 réplicas, `compact,delete`, retención de 14 a 30 días. DLT por consumidor (`*.DLT`) tras N reintentos.

## Convenciones a seguir (copiar de los servicios existentes)

- Java 21, Spring Boot 4.1.1 y Maven wrapper, con el mismo Dockerfile multi-stage y usuario no root que `digitalfix-ms-workorders`.
- Paquete `cl.digitalfix.audit` con capas `controller → service → repository`, `dto` como records, `entity`, `messaging/kafka` y `config`.
- Resource server de Entra con el mismo `issuer-uri` y `audiences` que Workorders. El BFF hace de proxy y aplica los roles.
- Idempotencia por `eventId` (igual que Notify y Report): clave única en la tabla de eventos.
- Tests con H2 en modo Oracle y Kafka simulado. Un test "real" que se salte sin broker.
- Puerto propuesto: 8083 (confirmar con el usuario y registrarlo en `../CLAUDE.md` e infra).

## Antes de empezar

1. El contrato del evento lo define el productor (Workorders): acuérdalo y documéntalo en `docs/` de Workorders primero.
2. Infra necesita `kafka/compose.yml` (Zookeeper ×3, Kafka ×3 y Kafka UI).
3. Confirma con el usuario el alcance de la evaluación vigente (pauta).
