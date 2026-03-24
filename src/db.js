const sql = require("mssql");
const { config } = require("./config");

const FALLBACK_DATABASE = "master";

let poolPromise = null;
let activeDatabase = null;

function toSnakeCaseKey(key) {
  if (typeof key !== "string" || !key) return null;
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/([A-Z]+)([A-Z][a-z0-9]+)/g, "$1_$2")
    .replace(/[\s\-]+/g, "_")
    .toLowerCase();
}

function toPascalCaseKey(key) {
  if (typeof key !== "string" || !key || !key.includes("_")) return null;
  return key
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function augmentRowKeys(row) {
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    return row;
  }

  for (const key of Object.keys(row)) {
    const value = row[key];

    const snake = toSnakeCaseKey(key);
    if (snake && !(snake in row)) {
      row[snake] = value;
    }

    const pascal = toPascalCaseKey(key);
    if (pascal && !(pascal in row)) {
      row[pascal] = value;
    }
  }

  return row;
}

function augmentResultRows(result) {
  if (!result || typeof result !== "object") {
    return result;
  }

  if (Array.isArray(result.recordset)) {
    result.recordset = result.recordset.map(augmentRowKeys);
  }

  if (Array.isArray(result.recordsets)) {
    result.recordsets = result.recordsets.map((recordset) =>
      Array.isArray(recordset) ? recordset.map(augmentRowKeys) : recordset
    );
  }

  return result;
}

if (!sql.Request.prototype.__cflQueryAugmented) {
  const originalQuery = sql.Request.prototype.query;

  sql.Request.prototype.query = async function patchedQuery(...args) {
    const result = await originalQuery.apply(this, args);
    return augmentResultRows(result);
  };

  sql.Request.prototype.__cflQueryAugmented = true;
}

function createPool(database) {
  const pool = new sql.ConnectionPool({
    server: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database,
    options: {
      encrypt: false,
      trustServerCertificate: true,
    },
    pool: {
      min: 2,
      max: 20,
      acquireTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
    },
    requestTimeout: 30000,
  });

  return pool.connect();
}

async function databaseHasCflSchema(pool) {
  const result = await pool
    .request()
    .query("SELECT 1 AS ok FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'cfl';");

  return result.recordset.length > 0;
}

async function connectWithFallback() {
  const targetDb = config.db.database || FALLBACK_DATABASE;
  const normalizedTargetDb = targetDb.toLowerCase();

  try {
    const targetPool = await createPool(targetDb);
    const hasSchema = await databaseHasCflSchema(targetPool);

    if (hasSchema || normalizedTargetDb === FALLBACK_DATABASE) {
      activeDatabase = targetDb;
      return targetPool;
    }

    // eslint-disable-next-line no-console
    console.warn(
      `database "${targetDb}" reachable but schema "cfl" not found; falling back to "${FALLBACK_DATABASE}"`
    );
    await targetPool.close();
  } catch (error) {
    if (normalizedTargetDb === FALLBACK_DATABASE) {
      throw error;
    }

    // eslint-disable-next-line no-console
    console.warn(
      `failed to connect to database "${targetDb}" (${error.message}); falling back to "${FALLBACK_DATABASE}"`
    );
  }

  const fallbackPool = await createPool(FALLBACK_DATABASE);
  activeDatabase = FALLBACK_DATABASE;
  return fallbackPool;
}

async function getPool() {
  if (!poolPromise) {
    poolPromise = connectWithFallback().catch((error) => {
      poolPromise = null;
      throw error;
    });
  }

  return poolPromise;
}

async function closePool() {
  if (!poolPromise) {
    return;
  }

  const pool = await poolPromise;
  poolPromise = null;
  activeDatabase = null;
  await pool.close();
}

function getActiveDatabase() {
  return activeDatabase;
}

module.exports = {
  sql,
  getPool,
  closePool,
  getActiveDatabase,
};
