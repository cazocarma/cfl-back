const fs = require("fs");
const path = require("path");
const { logger } = require("./logger");

const DEFAULT_DB_PORT = 1433;
const MIN_JWT_SECRET_BYTES = 32;
const MIN_SAP_TOKEN_BYTES = 16;

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
    corsOrigin: process.env.CORS_ORIGIN || false,
  },
  authn: {
    jwtSecret: requireJwtSecret(),
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
    token: validateSapToken(),
    defaultDestination: (process.env.SAP_ADAPTER_DEFAULT_DESTINATION || "PRD").trim().toUpperCase(),
    requestTimeoutMs: toNumber(process.env.SAP_ADAPTER_REQUEST_TIMEOUT_MS, 125000),
  },
  cflSapLoad: {
    maxDateRangeDays: toNumber(process.env.CFL_ETL_MAX_DATE_RANGE_DAYS, 30),
  },
};

function requireJwtSecret() {
  const secret = process.env.AUTHN_JWT_SECRET;

  if (!secret) {
    logger.fatal(
      "[SEGURIDAD FATAL] La variable de entorno AUTHN_JWT_SECRET no esta definida. " +
        "Genera un secreto seguro con: " +
        `node -e "console.log(require('crypto').randomBytes(${MIN_JWT_SECRET_BYTES}).toString('hex'))"`
    );
    process.exit(1);
  }

  const entropyBytes = Buffer.byteLength(secret, "utf8");
  if (entropyBytes < MIN_JWT_SECRET_BYTES) {
    logger.fatal(
      `[SEGURIDAD FATAL] AUTHN_JWT_SECRET es demasiado corto (${entropyBytes} bytes). ` +
        `Se requieren al menos ${MIN_JWT_SECRET_BYTES} bytes (${MIN_JWT_SECRET_BYTES * 2} caracteres hex).`
    );
    process.exit(1);
  }

  return secret;
}

function validateSapToken() {
  const token = process.env.SAP_ADAPTER_API_TOKEN || process.env.SAP_ADAPTER_TOKEN || "";

  if (!token) {
    logger.warn("SAP_ADAPTER_API_TOKEN no esta definido — integracion SAP deshabilitada");
    return "";
  }

  const tokenBytes = Buffer.byteLength(token, "utf8");
  if (tokenBytes < MIN_SAP_TOKEN_BYTES) {
    logger.warn(
      `SAP_ADAPTER_API_TOKEN es muy corto (${tokenBytes} bytes). ` +
        `Se recomiendan al menos ${MIN_SAP_TOKEN_BYTES} bytes.`
    );
  }

  return token;
}

module.exports = {
  config,
};
