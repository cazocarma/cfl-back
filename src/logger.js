const pino = require("pino");

const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  ...(process.env.NODE_ENV !== "production" && {
    transport: {
      target: "pino/file",
      options: { destination: 1 },
    },
  }),
});

module.exports = { logger };
