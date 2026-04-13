const { config } = require("../../config");
const { logger } = require("../../logger");
const { buildDomainError } = require("../../helpers");

// ---------------------------------------------------------------------------
// HTTP transport — generic SAP ETL service client
// ---------------------------------------------------------------------------

function ensureConfig() {
  if (!config.sapEtl.baseUrl || !config.sapEtl.token) {
    throw buildDomainError(
      "SAP ETL no configurado. Revisa SAP_ETL_BASE_URL y SAP_ETL_API_TOKEN",
      500
    );
  }
}

async function callEtl(path, body) {
  ensureConfig();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.sapEtl.requestTimeoutMs);

  try {
    const response = await fetch(`${config.sapEtl.baseUrl}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.sapEtl.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }

    if (!response.ok || payload?.error) {
      const errorDetail = payload?.error || {};
      const message = errorDetail.message || `sap-etl respondio ${response.status}`;
      const error = new Error(message);
      error.statusCode = response.status >= 500 ? response.status : 502;
      throw error;
    }

    return payload?.data || {};
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error("sap-etl excedio el timeout configurado");
      timeoutError.statusCode = 504;
      throw timeoutError;
    }

    if (error.statusCode) {
      throw error;
    }

    const networkError = new Error(`No se pudo conectar con sap-etl: ${error.message}`);
    networkError.statusCode = 502;
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

// ---------------------------------------------------------------------------
// RFC query: POST /api/v1/sap/rfc/query
// ---------------------------------------------------------------------------

async function queryRfc(destination, table, fields, where, rowCount = 0) {
  const data = await callEtl("/api/v1/sap/rfc/query", {
    destination,
    table,
    fields,
    where,
    rowCount,
  });
  return data?.records || [];
}

// ---------------------------------------------------------------------------
// OData query: POST /api/v1/sap/odata/query
// ---------------------------------------------------------------------------

async function queryOdata(destination, entitySet, options = {}) {
  const body = { destination, entitySet };
  if (options.filter) body.filter = options.filter;
  if (options.select) body.select = options.select;
  if (options.orderBy) body.orderBy = options.orderBy;
  if (options.top) body.top = options.top;

  const data = await callEtl("/api/v1/sap/odata/query", body);
  return data?.records || [];
}

module.exports = {
  queryRfc,
  queryOdata,
};
