# ms-digitalfix-audit (puerto 8085)

[![CI](https://github.com/DigitalFix-Grupo12/ms-digitalfix-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/DigitalFix-Grupo12/ms-digitalfix-audit/actions/workflows/ci.yml)

Timeline de auditoria append-only (JPA + H2).

| Metodo | Ruta | Descripcion |
|---|---|---|
| GET | /api/audit?limit=100&referencia= | Ultimos eventos (desc) |
| POST | /api/audit | Ingesta interna desde otros microservicios (no expuesto por el BFF) |
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
