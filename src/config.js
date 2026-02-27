const fs = require("fs");
const path = require("path");

const DEFAULT_DB_PORT = 1433;

function parseEnvValue(raw) {
  const value = raw.trim();

  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const separator = trimmed.indexOf("=");
    if (separator <= 0) {
      continue;
    }

    const key = trimmed.slice(0, separator).trim();
    const value = parseEnvValue(trimmed.slice(separator + 1));

    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function bootstrapEnv() {
  const explicitEnvFile = process.env.CFL_ENV_FILE
    ? path.resolve(process.cwd(), process.env.CFL_ENV_FILE)
    : null;

  const candidateFiles = [
    explicitEnvFile,
    path.resolve(__dirname, "..", "..", "cfl-infra", ".env"),
    path.resolve(process.cwd(), ".env"),
    path.resolve(__dirname, "..", ".env"),
  ].filter(Boolean);

  for (const filePath of candidateFiles) {
    loadEnvFile(filePath);
  }
}

function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

bootstrapEnv();

const config = {
  app: {
    env: process.env.NODE_ENV || "development",
    port: toNumber(process.env.PORT, 4000),
    corsOrigin: process.env.CORS_ORIGIN || "*",
  },
  db: {
    host: process.env.DB_HOST,
    port: toNumber(process.env.DB_PORT, DEFAULT_DB_PORT),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  },
};

module.exports = {
  config,
};
