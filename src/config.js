const fs = require("fs");
const path = require("path");

const DEFAULT_DB_PORT = 1433;
const DEFAULT_AUTHN_JWT_SECRET = "cfl-dev-secret";

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
  const infraEnvFile = path.resolve(__dirname, "..", "..", "cfl-infra", ".env");
  loadEnvFile(infraEnvFile);
}

function toNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function trimTrailingSlash(value, fallback) {
  const normalized = String(value || fallback || "").trim();
  return normalized.replace(/\/+$/, "");
}

bootstrapEnv();

const config = {
  app: {
    env: process.env.NODE_ENV || "development",
    port: toNumber(process.env.PORT, 4000),
    corsOrigin: process.env.CORS_ORIGIN || "*",
  },
  authn: {
    jwtSecret: process.env.AUTHN_JWT_SECRET || DEFAULT_AUTHN_JWT_SECRET,
  },
  db: {
    host: process.env.DB_HOST,
    port: toNumber(process.env.DB_PORT, DEFAULT_DB_PORT),
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  },
  sapAdapter: {
    baseUrl: trimTrailingSlash(
      process.env.SAP_ADAPTER_BASE_URL || process.env.SAP_ADAPTER_API_URL,
      ""
    ),
    token: process.env.SAP_ADAPTER_API_TOKEN || process.env.SAP_ADAPTER_TOKEN || "",
    defaultDestination: (process.env.SAP_ADAPTER_DEFAULT_DESTINATION || "PRD").trim().toUpperCase(),
    requestTimeoutMs: toNumber(process.env.SAP_ADAPTER_REQUEST_TIMEOUT_MS, 125000),
  },
  cflSapLoad: {
    maxDateRangeDays: toNumber(process.env.CFL_ETL_MAX_DATE_RANGE_DAYS, 30),
  },
};

if (
  config.authn.jwtSecret === DEFAULT_AUTHN_JWT_SECRET &&
  config.app.env === "production"
) {
  console.error(
    "[SEGURIDAD] AUTHN_JWT_SECRET no esta configurado. Se esta usando el secreto de desarrollo en produccion. " +
      "Configura la variable de entorno AUTHN_JWT_SECRET con un valor seguro."
  );
}

module.exports = {
  config,
};
