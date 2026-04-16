# CFL Backend

API REST del sistema Control de Fletes (CFL) de Greenvic. Gestiona el ciclo de vida completo de fletes, desde la ingesta de entregas SAP y registros de romana hasta la prefacturacion y generacion de planillas SAP.

## Stack tecnologico

| Componente | Tecnologia |
|---|---|
| Runtime | Node.js >= 20 |
| Framework | Express 4.21 |
| Base de datos | SQL Server (mssql 11) |
| Autenticacion | JWT HS256 (jsonwebtoken 9) |
| Validacion | Zod 4 |
| Logging | Pino 10 |
| Seguridad | Helmet 8, bcryptjs 2.4, express-rate-limit 8 |
| Exportacion | ExcelJS 4.4 (xlsx), PDFKit 0.18 (pdf) |

## Requisitos previos

- Node.js 20 o superior
- Acceso a una instancia SQL Server con el esquema `[cfl]` inicializado (scripts en `cfl-infra/database/modelo-datos/`)
- Archivo de entorno `cfl-infra/.env` configurado (este servicio no usa `.env` propio)

## Instalacion y ejecucion

### Desarrollo local

```bash
npm install
npm run dev
```

El flag `--watch` de Node.js reinicia el servidor automaticamente ante cambios en el codigo fuente. La API queda disponible en `http://localhost:4000`.

### Docker Compose (desde cfl-infra)

```bash
cd ../cfl-infra
make up
```

## Estructura del proyecto

```text
src/
  index.js                  Punto de entrada, arranque del servidor y graceful shutdown
  app.js                    Configuracion de Express, middlewares y montaje de rutas
  config.js                 Carga y validacion de variables de entorno
  db.js                     Pool de conexiones MSSQL
  authz.js                  Logica de autorizacion y resolucion de permisos
  audit.js                  Registro de auditoria en base de datos
  logger.js                 Instancia Pino (nivel INFO por defecto)
  helpers.js                Utilidades generales
  text-normalizer.js        Normalizacion de payloads JSON entrantes
  token-blocklist.js        Blocklist de tokens JWT revocados
  mantenedores-config.js    Configuracion de entidades CRUD maestras
  middleware/
    authn.middleware.js     Validacion de JWT en requests
    authz.middleware.js     Verificacion de permisos por ruta
    audit.middleware.js     Interceptor de auditoria por request
    rate-limit.middleware.js  Limitadores de tasa (lectura y escritura)
    validate.middleware.js  Validacion de schemas Zod
  routes/
    authn.routes.js         Login, logout, contexto de sesion
    dashboard.routes.js     Bandeja de fletes, resumen, acciones rapidas
    fletes.routes.js        CRUD de fletes, edicion, creacion manual
    fletes-sap-loads.routes.js   Carga de entregas SAP (por VBELN o rango de fechas)
    fletes-romana-loads.routes.js  Carga de registros de romana
    facturas.routes.js      Prefacturacion: movimientos elegibles, generacion, CRUD
    planillas-sap.routes.js Generacion y gestion de planillas SAP
    mantenedores.routes.js  CRUD generico para entidades maestras
    operaciones.routes.js   Operaciones sobre fletes
  schemas/
    authn.schemas.js        Schemas de validacion para autenticacion
    facturas.schemas.js     Schemas de validacion para facturas
    fletes.schemas.js       Schemas de validacion para fletes
    planillas-sap.schemas.js  Schemas de validacion para planillas
    usuarios.schemas.js     Schemas de validacion para usuarios
  services/
    factura-pdf.js          Generacion de PDFs de facturas
    factura-queries.js      Consultas SQL especializadas de facturacion
    planilla-sap-export.js  Exportacion de planillas SAP a Excel
  modules/
    cfl-sap-load/           ETL de entregas SAP (cliente, repositorio, servicio)
    cfl-romana-load/        ETL de registros de romana (cliente, repositorio, servicio)
    sap-sync/               Sincronizacion de catalogos SAP (productores, transporte)
  utils/
    lifecycle.js            Utilidades de ciclo de vida del proceso
    parse.js                Utilidades de parsing
```

## Variables de entorno

El backend lee sus variables desde `cfl-infra/.env`. No mantiene un archivo `.env` propio.

| Variable | Requerida | Default | Descripcion |
|---|---|---|---|
| `PORT` | No | `4000` | Puerto del servidor HTTP |
| `NODE_ENV` | No | `development` | Ambiente de ejecucion |
| `CORS_ORIGIN` | Si | -- | Origen permitido para CORS |
| `AUTHN_JWT_SECRET` | Si | -- | Secreto JWT HS256 (minimo 32 bytes) |
| `DB_HOST` | Si | -- | Host de SQL Server |
| `DB_PORT` | No | `1433` | Puerto de SQL Server |
| `DB_USER` | Si | -- | Usuario de SQL Server |
| `DB_PASSWORD` | Si | -- | Password de SQL Server |
| `DB_NAME` | Si | -- | Nombre de la base de datos |
| `SAP_ETL_BASE_URL` | No | -- | URL base del servicio Greenvic SAP ETL |
| `SAP_ETL_API_TOKEN` | No | -- | Bearer token para autenticacion con SAP ETL |
| `SAP_ETL_DEFAULT_DESTINATION` | No | `PRD` | Destino RFC por defecto |
| `SAP_ETL_REQUEST_TIMEOUT_MS` | No | `125000` | Timeout de requests hacia SAP ETL (ms) |
| `CFL_ETL_MAX_DATE_RANGE_DAYS` | No | `30` | Maximo de dias permitido en consultas por rango de fechas |

## Autenticacion y autorizacion

### Autenticacion

El sistema utiliza JWT con algoritmo HS256 y expiracion de 8 horas. El flujo es:

1. El cliente envia credenciales a `POST /api/authn/login`
2. El servidor valida contra la base de datos y retorna un token JWT
3. Las solicitudes posteriores incluyen el token en el header `Authorization: Bearer <token>`
4. Los tokens revocados se registran en una blocklist persistente

### Autorizacion

El middleware `requirePermission()` protege todas las rutas que requieren control de acceso. La resolucion de permisos sigue la cadena: `Usuario -> UsuarioRol -> Rol -> RolPermiso -> Permiso`.

| Rol | Permisos | Alcance |
|---|---|---|
| Administrador | 33 (todos) | Acceso total al sistema. Bypass automatico de chequeos via `isAdmin()` |
| Autorizador | 27 | Operaciones de fletes, facturacion y mantenedores parciales |
| Ingresador | 11 | Ingreso de fletes, lectura de facturas, planillas y mantenedores |

## Endpoints

### Autenticacion (`/api/authn`)

| Metodo | Ruta | Proteccion | Descripcion |
|---|---|---|---|
| `POST` | `/api/authn/login` | Publico (rate limited) | Inicio de sesion |
| `POST` | `/api/authn/logout` | JWT | Cierre de sesion e invalidacion de token |
| `GET` | `/api/authn/context` | JWT | Retorna roles y permisos del usuario autenticado |

### Dashboard y Bandeja (`/api/dashboard`)

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `GET` | `/api/dashboard/resumen` | `reportes.view` | Resumen de KPIs y metricas |
| `GET` | `/api/dashboard/fletes/no-ingresados` | `fletes.candidatos.view` | Fletes candidatos pendientes de ingreso |
| `GET` | `/api/dashboard/fletes/completados` | `fletes.candidatos.view` | Fletes en estado completado |
| `POST` | `/api/dashboard/fletes/:id/anular` | `fletes.anular` | Anulacion de un flete |
| `POST` | `/api/dashboard/fletes/no-ingresados/:id/crear` | `fletes.crear` | Crear flete desde candidato SAP |
| `POST` | `/api/dashboard/fletes/no-ingresados/:id/descartar` | `fletes.sap.descartar` | Descartar candidato SAP |

### Fletes (`/api/fletes`)

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `GET` | `/api/fletes/:id` | `fletes.candidatos.view` | Detalle de un flete |
| `POST` | `/api/fletes/manual` | `fletes.crear` | Creacion manual de flete |
| `PUT` | `/api/fletes/:id` | `fletes.editar` | Edicion de flete existente |

### Cargas SAP y Romana (`/api/fletes/cargas-sap`, `/api/fletes/cargas-romana`)

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `POST` | `/api/fletes/cargas-sap/vbeln` | `fletes.sap.etl.ejecutar` | Carga de entregas SAP por numero VBELN |
| `POST` | `/api/fletes/cargas-sap/rango-fechas` | `fletes.sap.etl.ejecutar` | Carga de entregas SAP por rango de fechas |
| `GET` | `/api/fletes/cargas-sap/jobs` | `fletes.sap.etl.ver` | Consulta de jobs de carga ejecutados |
| `POST` | `/api/fletes/cargas-romana/rango-fechas` | `fletes.sap.etl.ejecutar` | Carga de registros de romana por rango de fechas |

### Facturas (`/api/facturas`)

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `GET` | `/api/facturas` | `facturas.ver` | Listado de prefacturas |
| `POST` | `/api/facturas/generar` | `facturas.editar` | Generacion de prefactura desde movimientos elegibles |
| `POST` | `/api/facturas/:id/movimientos` | `facturas.editar` | Agregar movimientos a una factura |
| `PATCH` | `/api/facturas/:id/estado` | `facturas.editar` | Cambio de estado de factura |

### Planillas SAP (`/api/planillas-sap`)

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `GET` | `/api/planillas-sap` | `planillas.ver` | Listado de planillas SAP |
| `POST` | `/api/planillas-sap/generar` | `planillas.generar` | Generacion de planilla SAP |
| `PATCH` | `/api/planillas-sap/:id/estado` | `planillas.generar` | Cambio de estado de planilla |

### Mantenedores (`/api/mantenedores`)

CRUD generico para entidades maestras del sistema.

| Metodo | Ruta | Permiso | Descripcion |
|---|---|---|---|
| `GET` | `/api/mantenedores/:entity` | `mantenedores.view` | Listado de registros de la entidad |
| `POST` | `/api/mantenedores/:entity` | `mantenedores.edit.{entity}` | Creacion de registro |
| `PUT` | `/api/mantenedores/:entity/:id` | `mantenedores.edit.{entity}` | Edicion de registro |

Entidades disponibles: `temporadas`, `centros-costo`, `tipos-flete`, `tipos-camion`, `choferes`, `empresas-transporte`, `productores`, `nodos`, `rutas`, `tarifas`, `cuentas-mayor`, `usuarios`, `roles`, `permisos`, `camiones`, `detalles-viaje`, `especies`, `imputaciones-flete`.

### Health Check

| Metodo | Ruta | Proteccion | Descripcion |
|---|---|---|---|
| `GET` | `/health` | Publico | Verifica conectividad con la base de datos |

## Modelo de datos

Todas las tablas residen en el esquema `[cfl]` de SQL Server. Los scripts de creacion se encuentran en `cfl-infra/database/modelo-datos/`.

| Grupo | Tablas | Proposito |
|---|---|---|
| SAP Raw | `SapLikpRaw`, `SapLipsRaw`, `SapEntrega`, `SapEntregaPosicion` + historiales | Datos crudos ingestados desde SAP |
| Romana Raw | `RomanaCabeceraRaw`, `RomanaDetalleRaw`, `RomanaEntrega`, `RomanaEntregaPosicion` + historiales | Datos crudos de pesaje en romana |
| Staging | `StgLikp`, `StgLips`, `StgRomanaCabecera`, `StgRomanaDetalle` | Tablas intermedias de procesamiento ETL |
| Operacion | `CabeceraFlete`, `DetalleFlete`, `FleteEstadoHistorial`, `FleteSapEntrega`, `FleteRomanaEntrega` | Fletes gestionados por el sistema |
| Facturacion | `CabeceraFactura`, `PlanillaSap`, `PlanillaSapDocumento`, `PlanillaSapFactura`, `PlanillaSapLinea` | Prefacturacion y planillas SAP |
| Maestros | `Temporada`, `CentroCosto`, `CuentaMayor`, `TipoFlete`, `TipoCamion`, `Camion`, `EmpresaTransporte`, `Chofer`, `Movil`, `NodoLogistico`, `Ruta`, `Tarifa`, `ImputacionFlete`, `DetalleViaje`, `Especie`, `Productor` | Datos de referencia y configuracion |
| Seguridad | `Usuario`, `Rol`, `Permiso`, `UsuarioRol`, `RolPermiso`, `Auditoria`, `TokenBlocklist` | Control de acceso y trazabilidad |

## Docker

La imagen de produccion se construye desde `Dockerfile` con las siguientes caracteristicas:

- Imagen base: `node:20-slim`
- Instalacion de dependencias de produccion unicamente (`npm ci --omit=dev`)
- Ejecucion como usuario no-root (`app`)
- Puerto expuesto: 4000
- Comando: `node src/index.js`

## Verificacion

```bash
curl http://localhost:4000/health
```

Una respuesta `200 OK` confirma que el servidor esta activo y conectado a la base de datos.
