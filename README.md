# ms-digitalfix-audit (puerto 8085)

[![CI](https://github.com/DigitalFix-Grupo12/ms-digitalfix-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalFix-Grupo12/ms-digitalfix-audit/actions/workflows/ci.yml)

Timeline de auditoria append-only (JPA + H2 / RDS PostgreSQL). Su canal principal de ingesta es **Kafka**.

| Metodo | Ruta | Descripcion |
|---|---|---|
| GET | /api/audit?limit=100&referencia= | Ultimos eventos (desc) |
| POST | /api/audit | Ingesta REST de compatibilidad (no expuesto por el BFF) |

## Streaming con Kafka

Consumer group **`audit-timeline`** (3 hilos = 3 particiones por topico):

| Topico | Evento (DTO) | Productor | Que registra |
|---|---|---|---|
| `workorders.events` | `WorkOrderEvent` | ms-digitalfix-workorders | Hechos de la orden: creada, asignada, cambio de estado, cerrada, cancelada |
| `audit.timeline` | `AuditTimelineEvent` | ms-digitalfix-bff | Quien / que / cuando / desde donde (IP, rol, resultado SUCCESS/DENIED/ERROR) |

`workorders.events` lo lee tambien el grupo `report-kpis` (fan-out): cada grupo avanza con sus propios offsets.

```
messaging/config    AuditKafkaProperties (digitalfix.kafka.*), KafkaConsumerConfig (fabricas tipadas, errores, DLT)
messaging/event     contratos: WorkOrderEvent, AuditTimelineEvent, Actor
messaging/listener  @KafkaListener por topico: validan y delegan (sin logica de negocio)
messaging/support   EventValidator, KafkaLogContext (MDC), InvalidEventException
service             AuditTimelineService: traduce el evento y lo guarda una sola vez por eventId
```

**Garantias**

- *At-least-once + idempotencia*: el offset se confirma despues de guardar (AckMode RECORD); un evento repetido se detecta por `eventId` (restriccion unica) y se ignora.
- *Orden por orden de trabajo*: la clave del mensaje es el id de la orden, asi que sus eventos van a la misma particion.
- *Errores*: JSON ilegible o payload invalido (Bean Validation) van directo a la DLT; fallas transitorias (ej. BD) se reintentan 3 veces con backoff 1s/2s/4s y luego van a la DLT. La DLT es `<topico>.audit-timeline.DLT`, en la misma particion, con headers `kafka_dlt-*` (causa, topico, particion y offset originales). Un mensaje defectuoso nunca bloquea la particion.
- *Trazabilidad*: cada registro guarda topico, particion y offset; cada linea de log lleva `eventId`, `traceId` y la posicion del registro.
- Un valor nulo en `audit.timeline` es un tombstone de compactacion y se ignora.

Configuracion: `KAFKA_BOOTSTRAP_SERVERS` (por defecto `localhost:9092,localhost:9093,localhost:9094`, el cluster de `digitalfix-infra/kafka`). Los tests usan un broker Kafka embebido, no requieren Docker.

Ejemplo de evento en `workorders.events` (clave `12`):

```json
{"eventId":"7f0c...","eventType":"STATUS_CHANGED","schemaVersion":1,"occurredAt":"2026-11-18T12:00:00Z",
 "traceId":"a1b2c3","source":"ms-digitalfix-workorders","workOrderId":12,
 "actor":{"id":"<oid>","username":"supervisor.demo@...","role":"Supervisor"},
 "previousStatus":"ASIGNADA","status":"EN_DESPLAZAMIENTO"}
```
## Arquitectura

```
Angular (MSAL) -> AWS API Gateway -> ms-digitalfix-bff :8080 (valida JWT Entra ID + rol)
                                         |-> ms-digitalfix-workorders :8082 -> audit
                                         |-> ms-digitalfix-catalog    :8083
                                         |-> ms-digitalfix-report     :8084 -> workorders
                                         |-> ms-digitalfix-audit      :8085
```

Los microservicios de dominio **no validan JWT**: solo escuchan en la red interna
del host (el Security Group expone unicamente el 8080 del BFF). El BFF propaga la
identidad del usuario en el header `X-User-Name`.

## Ejecutar

```
mvn clean package
java -jar target/ms-digitalfix-audit-0.0.1-SNAPSHOT.jar
```

Health: `GET /actuator/health`. Base de datos: H2 en memoria (se reinicia con el servicio).

## Perfiles de base de datos

| Perfil | Base de datos | Uso |
|---|---|---|
| (por defecto) | H2 en memoria | Desarrollo local y tests |
| `cloud` | Amazon RDS PostgreSQL, schema `audit` | EC2 (`SPRING_PROFILES_ACTIVE=cloud`) |

Variables del perfil `cloud`: `DB_HOST`, `DB_PORT` (5432), `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD`. En la EC2 se obtienen de SSM Parameter Store; nunca se guardan en el repo. El schema se crea al arrancar (`hibernate.hbm2ddl.create_namespaces`).

## Ejemplos

Directo al microservicio (local, sin token):

```bash
# últimos 20 eventos, del más reciente al más antiguo
curl "http://localhost:8085/api/audit?limit=20"

# historial de una orden (el # se codifica como %23)
curl "http://localhost:8085/api/audit?referencia=orden%231"

# registrar un evento (uso interno entre microservicios)
curl -X POST http://localhost:8085/api/audit \
  -H "Content-Type: application/json" \
  -d '{"usuario":"supervisor.demo","accion":"CREO orden #1","servicio":"ms-digitalfix-workorders","referencia":"orden#1"}'
```

Si falta `usuario`, `accion` o `servicio`, el POST responde 400.

A través del API Gateway (roles Admin o Auditor; el POST no está expuesto):

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://7s6qn2mb8h.execute-api.us-east-1.amazonaws.com/api/audit?referencia=orden%231"
```
