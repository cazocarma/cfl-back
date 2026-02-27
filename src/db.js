const sql = require("mssql");
const { config } = require("./config");

const FALLBACK_DATABASE = "master";

let poolPromise = null;
let activeDatabase = null;

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
      min: 0,
      max: 10,
      idleTimeoutMillis: 30000,
    },
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
