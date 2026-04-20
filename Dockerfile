# ══════════════════════════════════════════════════════════
#  CFL Backend — Multi-stage Docker build
#
#  Stages:
#    - deps    : instala node_modules de runtime (--omit=dev),
#                cacheables por lockfile. Usuario `app` creado.
#    - dev     : hereda deps; CMD con `node --watch`; la fuente
#                entra por bind mount desde compose.override.yml.
#                No hay `npm install` en runtime.
#    - runtime : imagen prd; copia node_modules + fuente;
#                non-root (`app`); sin herramientas de build.
# ══════════════════════════════════════════════════════════

# ── Stage 1: deps ─────────────────────────────────────────
FROM node:22-bookworm-slim@sha256:f3a68cf41a855d227d1b0ab832bed9749469ef38cf4f58182fb8c893bc462383 AS deps

WORKDIR /app

RUN groupadd -r app && useradd -r -g app -d /app app

COPY --chown=app:app package.json package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund \
  && chown -R app:app /app/node_modules

# ── Stage 2: dev ──────────────────────────────────────────
FROM deps AS dev

ENV NODE_ENV=development

USER app

EXPOSE 4000

CMD ["node", "--watch", "src/index.js"]

# ── Stage 3: runtime (prd) ────────────────────────────────
FROM node:22-bookworm-slim@sha256:f3a68cf41a855d227d1b0ab832bed9749469ef38cf4f58182fb8c893bc462383 AS runtime

ENV NODE_ENV=production

WORKDIR /app

RUN groupadd -r app && useradd -r -g app -d /app app

COPY --from=deps --chown=app:app /app/node_modules ./node_modules

COPY --chown=app:app . .

USER app

EXPOSE 4000

CMD ["node", "src/index.js"]
