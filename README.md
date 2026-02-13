# cfl-back

Backend base del proyecto de control de fletes.

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
