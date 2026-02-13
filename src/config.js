const DEFAULT_DB_PORT = 1433;

function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

const config = {
  app: {
    env: process.env.NODE_ENV || "development",
    port: toNumber(process.env.PORT, 4000),
    corsOrigin: process.env.CORS_ORIGIN || "*",
  },
  db: {
    host: process.env.DB_HOST || "localhost",
    port: toNumber(process.env.DB_PORT, DEFAULT_DB_PORT),
    user: process.env.DB_USER || "sa",
    password: process.env.DB_PASSWORD || "",
    database: process.env.DB_NAME || "master",
  },
};

module.exports = {
  config,
};
