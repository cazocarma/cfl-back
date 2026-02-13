const { app } = require("./app");
const { config } = require("./config");
const { closePool } = require("./db");

const server = app.listen(config.app.port, "0.0.0.0", () => {
  // eslint-disable-next-line no-console
  console.log(`cfl-back listening on port ${config.app.port}`);
});

async function gracefulShutdown(signal) {
  // eslint-disable-next-line no-console
  console.log(`received ${signal}, shutting down`);
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
