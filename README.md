# cfl-back

Backend Node.js/Express del sistema Control de Fletes (CFL) de Greenvic.

## Variables de entorno

`cfl-back` usa el archivo centralizado `cfl-infra/.env`.
No usa `.env` propio.

| Variable | Descripcion |
|---|---|
| `PORT` | Puerto del servidor (default: 4000) |
| `CORS_ORIGIN` | Origen permitido para CORS |
| `AUTHN_JWT_SECRET` | Secret JWT HS256 (min 32 bytes, obligatorio) |
| `DB_HOST` | Host SQL Server |
| `DB_PORT` | Puerto SQL Server (default: 1433) |
| `DB_USER` | Usuario SQL Server |
| `DB_PASSWORD` | Password SQL Server |
| `DB_NAME` | Nombre de la base de datos |
| `SAP_ETL_BASE_URL` | URL del servicio Greenvic SAP ETL |
| `SAP_ETL_API_TOKEN` | Bearer token para SAP ETL |
| `SAP_ETL_DEFAULT_DESTINATION` | Destino RFC por defecto (PRD) |
| `SAP_ETL_REQUEST_TIMEOUT_MS` | Timeout de requests SAP en ms |
| `CFL_ETL_MAX_DATE_RANGE_DAYS` | Max dias en consulta por rango de fechas |

## Levantar

```bash
# Local
npm install
npm run dev

# Docker Compose (desde cfl-infra)
cd ../cfl-infra
docker compose up
```

API disponible en `http://localhost:4000`.

## Autenticacion y autorizacion

- **Autenticacion:** JWT HS256 con 8h de expiracion. Login via `POST /api/authn/login`.
- **Autorizacion:** Middleware `requirePermission()` en todas las rutas protegidas. Resuelve permisos desde BD (`Usuario → UsuarioRol → Rol → RolPermiso → Permiso`).
- **Roles:** Administrador (acceso total), Autorizador (operaciones + mantenedores parcial), Ingresador (fletes + lectura).
- **Admin bypass:** El rol Administrador pasa todos los chequeos de permisos automaticamente via `isAdmin()` en `authz.js`.

## Endpoints

### Autenticacion
| Metodo | Ruta | Proteccion |
|---|---|---|
| `POST` | `/api/authn/login` | Publico (rate limited) |
| `POST` | `/api/authn/logout` | JWT |
| `GET` | `/api/authn/context` | JWT — devuelve roles y permisos del usuario |

### Dashboard / Bandeja
| Metodo | Ruta | Permiso |
|---|---|---|
| `GET` | `/api/dashboard/resumen` | `reportes.view` |
| `GET` | `/api/dashboard/fletes/no-ingresados` | `fletes.candidatos.view` |
| `GET` | `/api/dashboard/fletes/completados` | `fletes.candidatos.view` |
| `POST` | `/api/dashboard/fletes/:id/anular` | `fletes.anular` |
| `POST` | `/api/dashboard/fletes/no-ingresados/:id/crear` | `fletes.crear` |
| `POST` | `/api/dashboard/fletes/no-ingresados/:id/descartar` | `fletes.sap.descartar` |

### Fletes
| Metodo | Ruta | Permiso |
|---|---|---|
| `GET` | `/api/fletes/:id` | `fletes.candidatos.view` / `fletes.editar` |
| `POST` | `/api/fletes/manual` | `fletes.crear` |
| `PUT` | `/api/fletes/:id` | `fletes.editar` |

### Facturas
| Metodo | Ruta | Permiso |
|---|---|---|
| `GET` | `/api/facturas` | `facturas.ver` / `facturas.editar` / `facturas.conciliar` |
| `POST` | `/api/facturas/generar` | `facturas.editar` |
| `POST` | `/api/facturas/:id/movimientos` | `facturas.editar` |
| `PATCH` | `/api/facturas/:id/estado` | `facturas.editar` |

### Planillas SAP
| Metodo | Ruta | Permiso |
|---|---|---|
| `GET` | `/api/planillas-sap` | `planillas.ver` / `planillas.generar` |
| `POST` | `/api/planillas-sap/generar` | `planillas.generar` |
| `PATCH` | `/api/planillas-sap/:id/estado` | `planillas.generar` |

### Cargas SAP / Romana
| Metodo | Ruta | Permiso |
|---|---|---|
| `POST` | `/api/fletes/cargas-sap/vbeln` | `fletes.sap.etl.ejecutar` |
| `POST` | `/api/fletes/cargas-sap/rango-fechas` | `fletes.sap.etl.ejecutar` |
| `GET` | `/api/fletes/cargas-sap/jobs` | `fletes.sap.etl.ver` |
| `POST` | `/api/fletes/cargas-romana/rango-fechas` | `fletes.sap.etl.ejecutar` |

### Mantenedores (CRUD generico)
| Metodo | Ruta | Permiso |
|---|---|---|
| `GET` | `/api/mantenedores/:entity` | `mantenedores.view` + entity-specific |
| `POST` | `/api/mantenedores/:entity` | `mantenedores.edit.{entity}` |
| `PUT` | `/api/mantenedores/:entity/:id` | `mantenedores.edit.{entity}` |

Entidades: `temporadas`, `centros-costo`, `tipos-flete`, `tipos-camion`, `choferes`, `empresas-transporte`, `productores`, `nodos`, `rutas`, `tarifas`, `cuentas-mayor`, `usuarios`, `roles`, `permisos`, `camiones`, `detalles-viaje`, `especies`, `imputaciones-flete`.

## Modelo de datos

Esquema: `[cfl]`. Scripts en `cfl-infra/database/modelo-datos/`.

| Grupo | Tablas |
|---|---|
| SAP raw | `SapLikpRaw`, `SapLipsRaw`, `SapEntrega`, `SapEntregaPosicion` + historiales |
| Romana raw | `RomanaCabeceraRaw`, `RomanaDetalleRaw`, `RomanaEntrega`, `RomanaEntregaPosicion` + historiales |
| Staging | `StgLikp`, `StgLips`, `StgRomanaCabecera`, `StgRomanaDetalle` |
| Operacion | `CabeceraFlete`, `DetalleFlete`, `FleteEstadoHistorial`, `FleteSapEntrega`, `FleteRomanaEntrega` |
| Facturacion | `CabeceraFactura`, `PlanillaSap`, `PlanillaSapDocumento`, `PlanillaSapFactura`, `PlanillaSapLinea` |
| Maestros | `Temporada`, `CentroCosto`, `CuentaMayor`, `TipoFlete`, `TipoCamion`, `Camion`, `EmpresaTransporte`, `Chofer`, `Movil`, `NodoLogistico`, `Ruta`, `Tarifa`, `ImputacionFlete`, `DetalleViaje`, `Especie`, `Productor` |
| Seguridad | `Usuario`, `Rol`, `Permiso`, `UsuarioRol`, `RolPermiso`, `Auditoria`, `TokenBlocklist` |
