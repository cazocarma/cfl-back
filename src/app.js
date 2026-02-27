const express = require("express");
const cors = require("cors");
const { config } = require("./config");
const { getPool, getActiveDatabase } = require("./db");
const { dashboardRouter } = require("./routes/dashboard.routes");
const { mantenedoresRouter } = require("./routes/mantenedores.routes");
const { fletesRouter } = require("./routes/fletes.routes");
const { authRouter } = require("./routes/auth.routes");

const app = express();

app.use(
  cors({
    origin: config.app.corsOrigin,
  })
);
app.use(express.json());

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
    res.status(503).json({
      healthy: false,
      db: {
        connected: false,
        database: getActiveDatabase(),
      },
      error: error.message,
    });
  }
});

app.use("/api/dashboard", dashboardRouter);
app.use("/api/mantenedores", mantenedoresRouter);
app.use("/api/fletes", fletesRouter);
app.use("/api/auth", authRouter);

app.use((error, _req, res, _next) => {
  const message = error && error.message ? error.message : "Error interno del servidor";
  const code = error && Number.isInteger(error.statusCode) ? error.statusCode : 500;
  const dbErrorCode = error && error.number ? error.number : null;

  if (dbErrorCode === 2627 || dbErrorCode === 2601) {
    res.status(409).json({
      error: "Violacion de unicidad al guardar registro",
      detail: message,
    });
    return;
  }

  if (dbErrorCode === 547) {
    res.status(409).json({
      error: "No se puede eliminar o actualizar por integridad referencial",
      detail: message,
    });
    return;
  }

  res.status(code).json({
    error: message,
  });
});

module.exports = {
  app,
};
