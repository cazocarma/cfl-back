const { app } = require("./app");
const { config } = require("./config");
const { closePool } = require("./db");
const { logger } = require("./logger");

const server = app.listen(config.app.port, "0.0.0.0", () => {
  logger.info(`cfl-back listening on port ${config.app.port}`);
});

async function gracefulShutdown(signal) {
  logger.info(`received ${signal}, shutting down`);
  server.close(async () => {
    try {
      await closePool();
    } finally {
      process.exit(0);
    }
  });
}

process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
