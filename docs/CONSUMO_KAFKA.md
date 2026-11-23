# Consumo Kafka de ciclo de vida y ediciones

Implementación de Audit #1, según el [contrato Infra #16](https://github.com/arieeliiu/digitalfix-infra/pull/16).

| Origen | Tipos | Grupo | DLT |
|---|---|---|---|
| `workorders.events` | `ORDEN_CREADA`, `ORDEN_ESTADO_CAMBIADO`, `ORDEN_ELIMINADA` | `audit` | `workorders.events.audit.DLT` |
| `audit.timeline` | `ORDEN_DATOS_ACTUALIZADOS`, `ORDEN_CONTACTOS_ACTUALIZADOS`, `ORDEN_REPUESTOS_ACTUALIZADOS` | `audit` | `audit.timeline.audit.DLT` |

Se valida sobre 1.0, UUID, fecha, identidad de orden, key/correlación y coherencia
entre tipo, acción, tópico y estados. Una edición conserva el estado; creación y
eliminación usan el extremo null correspondiente. No se reciben correos, direcciones
ni descripciones de la orden. Un contrato inválido va directamente al DLT.

Ambos flujos se guardan en `auditoria_eventos`, con unicidad global por `eventId`.
La API existente `/api/audit/ordenes/{id}/timeline` reúne ambos, mantiene los roles
Admin/Auditor y añade `kafkaTopico` al DTO. No se cambia la autorización para dar
acceso al propietario: eso debe resolverse explícitamente en BFF #23.

## Entrega, errores y persistencia

Auto-commit permanece desactivado. El offset se confirma después del procesamiento
o recuperación correcta; no antes de persistir. El DLT conserva partición, key y
valor original, y añade headers de diagnóstico. Se espera la confirmación de envío:
un fallo del DLT no se considera recuperación exitosa y bloquea el avance de ese registro.
Los errores temporales se reintentan tres veces antes de ir al DLT; los logs incluyen
tópico/partición/offset/intento y la clase del error, no el payload.

La columna nueva `kafka_topico` es nullable para compatibilidad con registros previos.
Los nuevos registros siempre conservan el origen. Se preparó `oracle-audit-topico.sql`
para añadir la columna y completar el origen histórico conocido, sin sobrescribir
valores no nulos. No se ejecutó ni se certificó contra Oracle. Aplicarlo solo tras
revisión en el entorno autorizado; el arranque sigue usando `ddl-auto=update` y
puede efectuar DDL, por lo que no debe usarse como prueba inocua contra RDS.

## Validación local

```powershell
.\mvnw.cmd -B test
# Solo contra un Kafka de prueba aislado: publica datos sintéticos en los tópicos del caso.
.\mvnw.cmd -B '-Dtest=KafkaAuditRealTests,KafkaDltFalloRealTests' '-Ddigitalfix.pruebas.kafka=true' test
```

`KafkaAuditRealTests` usa listener y brokers reales, persistencia H2 aislada, servidor
HTTP real y JWT RSA firmado/verificado localmente. Comprueba ambos orígenes,
duplicados, consulta HTTP 200/401/403, rechazo de firma inválida, DLT por origen,
tres reintentos ante una tabla de prueba indisponible y lag final cero. Los grupos
y órdenes de prueba son distintos por ejecución; no se borran mensajes anteriores.

`KafkaDltFalloRealTests` utiliza un grupo propio desde los offsets actuales y un
destino DLT temporal inicialmente ausente. Comprueba que el offset queda detenido;
al crear el DLT, verifica el mensaje y el avance del offset. Elimina exclusivamente
su DLT y grupo temporales. No altera los DLT del caso.

El verificador de Workorders publicó previamente eventos reales que este consumidor
leyó del backlog; los logs registraron las cinco entradas de esa orden. Esto comprueba
el contrato entre ambos servicios por Kafka, pero no su transacción sobre Oracle.

No se presenta H2 como Oracle, ni la identidad local como Microsoft Entra. Quedan
pendientes migraciones/Oracle autorizado, flujo con Entra real y validación conjunta
de RabbitMQ/SMTP/PDF. Integrar también el PR independiente de integridad Audit #4
antes de activar el conjunto; este cambio no incorpora esa corrección en su historial.

Referencia: [confirmación del DLT en Spring Kafka](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).
