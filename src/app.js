const express = require("express");
const helmet = require("helmet");
const cors = require("cors");
const { config } = require("./config");
const { logger } = require("./logger");
const { getPool, getActiveDatabase } = require("./db");
const { dashboardRouter } = require("./routes/dashboard.routes");
const { mantenedoresRouter } = require("./routes/mantenedores.routes");
const { fletesRouter } = require("./routes/fletes.routes");
const { fletesSapLoadsRouter } = require("./routes/fletes-sap-loads.routes");
const { authnRouter } = require("./routes/authn.routes");
const { operacionesRouter } = require("./routes/operaciones.routes");
const { facturasRouter } = require("./routes/facturas.routes");
const { planillasSapRouter } = require("./routes/planillas-sap.routes");
const { requireJwtAuthn } = require("./middleware/authn.middleware");
const { auditMiddleware } = require("./middleware/audit.middleware");
const { writeLimiter, readLimiter } = require("./middleware/rate-limit.middleware");
const { normalizeJsonTextPayload } = require("./text-normalizer");

const app = express();

app.set("trust proxy", 1);

app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
        fontSrc: ["'self'", "https://fonts.gstatic.com"],
        imgSrc: ["'self'", "data:"],
        connectSrc: ["'self'"],
      },
    },
    crossOriginEmbedderPolicy: false,
  })
);

app.use(
  cors({
    origin: config.app.corsOrigin,
  })
);
app.use(express.json({ limit: "10mb" }));

app.use((req, res, next) => {
  const originalJson = res.json.bind(res);
  res.json = (payload) => originalJson(normalizeJsonTextPayload(payload));
  next();
});

app.get("/", (_req, res) => {
  res.json({
    service: "cfl-back",
    status: "ok",
  });
});

app.get("/health", async (_req, res) => {
  try {
    const pool = await getPool();
    await pool.request().query("SELECT 1 AS ok;");
    res.status(200).json({
      healthy: true,
      db: {
        connected: true,
        database: getActiveDatabase(),
      },
    });
  } catch (error) {
    logger.error({ err: error.message }, "health check BD connection error");
    res.status(503).json({
      healthy: false,
      db: {
        connected: false,
      },
    });
  }
});

app.use((req, res, next) => {
  const isPublicRequest =
    (req.method === "GET" && (req.path === "/" || req.path === "/health")) ||
    (req.method === "POST" && req.path === "/api/authn/login");

  if (isPublicRequest) {
    return next();
  }

  return requireJwtAuthn(req, res, next);
});

app.use(writeLimiter);
app.use(readLimiter);

app.use(auditMiddleware);

app.use("/api/dashboard", dashboardRouter);
app.use("/api/mantenedores", mantenedoresRouter);
app.use("/api/fletes/cargas-sap", fletesSapLoadsRouter);
app.use("/api/fletes", fletesRouter);
app.use("/api/operaciones", operacionesRouter);
app.use("/api/facturas", facturasRouter);
app.use("/api/planillas-sap", planillasSapRouter);
app.use("/api/authn", authnRouter);

app.use((error, req, res, _next) => {
  const code =
    error && Number.isInteger(error.statusCode) ? error.statusCode : 500;
  const dbErrorCode = error && error.number ? error.number : null;

  logger.error(
    {
      err: error.stack || error.message || error,
      code,
      method: req.method,
      path: req.originalUrl,
      userId: req.jwtPayload?.id_usuario || null,
    },
    "unhandled route error",
  );

  if (dbErrorCode === 2627 || dbErrorCode === 2601) {
    res.status(409).json({
      error: "Violacion de unicidad al guardar registro",
    });
    return;
  }

  if (dbErrorCode === 547) {
    res.status(409).json({
      error: "No se puede eliminar o actualizar por integridad referencial",
    });
    return;
  }

  res.status(code).json({
    error: code >= 500 ? "Error interno del servidor" : error.message,
  });
});

module.exports = {
  app,
};
