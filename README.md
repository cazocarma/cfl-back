# cfl-back

Backend base del proyecto de control de fletes.

## Variables de entorno

`cfl-back` usa el archivo centralizado `cfl-infra/.env`.
No se usa `.env` local dentro de `cfl-back`.

Variables relevantes:

- `PORT`
- `CORS_ORIGIN`
- `AUTHN_JWT_SECRET`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

## Levantar en local

```bash
npm install
npm run dev
```

## Levantar con Docker Compose

```bash
docker compose up
```

API disponible en `http://localhost:4000`.

## Endpoints principales

- `GET /health`
- `POST /api/authn/login`
- `GET /api/authn/context`
- `GET /api/dashboard/resumen`
- `GET /api/dashboard/fletes/no-ingresados`
- `GET /api/mantenedores/resumen`
- `GET /api/mantenedores/:entity`
- `POST /api/mantenedores/:entity`
- `PUT /api/mantenedores/:entity/:id`
- `DELETE /api/mantenedores/:entity/:id`

Entidades soportadas en `:entity`:

- `temporadas`
- `centros-costo`
- `tipos-flete`
- `tipos-camion`
- `choferes`
- `transportistas`
- `nodos`
- `tarifas`
- `usuarios`
