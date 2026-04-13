const { config } = require("../../config");
const { logger } = require("../../logger");
const { buildDomainError } = require("../../helpers");
const { formatDateToSap } = require("./utils");

// ---------------------------------------------------------------------------
// Campos SAP requeridos por repository.js
// ---------------------------------------------------------------------------

const LIKP_FIELDS = [
  "VBELN", "XBLNR", "VSTEL", "KUNNR", "VKORG", "ERNAM", "ERDAT", "LFART",
  "YWT_CDFIELD11", "YWT_CDFIELD12", "YWT_XBLNR", "YWT_DDFIELD21",
  "YWT_DDFIELD23", "YWT_CDFIELD24", "YWT_DDFIELD27", "YWT_DDFIELD28",
  "YWT_DDFIELD29", "YWT_DDFIELD30", "YWT_CDFIELD92",
  "BTGEW", "NTGEW", "WADAT_IST",
];

const LIPS_FIELDS = [
  "VBELN", "POSNR", "UECHA", "UEPOS", "MATNR", "CHARG",
  "LFIMG", "GEWEI", "ARKTX", "WERKS", "LGORT", "ERDAT",
];

const VBELN_BATCH_SIZE = 40;

// ---------------------------------------------------------------------------
// HTTP transport — POST /api/v1/sap/rfc/query
// ---------------------------------------------------------------------------

async function callSapEtl(body) {
  if (!config.sapEtl.baseUrl || !config.sapEtl.token) {
    throw buildDomainError(
      "SAP ETL no configurado. Revisa SAP_ETL_BASE_URL y SAP_ETL_API_TOKEN",
      500
    );
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.sapEtl.requestTimeoutMs);

  try {
    const response = await fetch(`${config.sapEtl.baseUrl}/api/v1/sap/rfc/query`, {
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
      const message =
        errorDetail.message || `sap-etl respondio ${response.status}`;
      const error = new Error(message);
      error.statusCode = response.status >= 500 ? response.status : 502;
      throw error;
    }

    return payload?.data?.records || [];
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
// Query helpers
// ---------------------------------------------------------------------------

function escapeAbap(value) {
  return String(value).replace(/'/g, "''");
}

async function queryTable(destination, table, fields, where, rowCount = 0) {
  return callSapEtl({ destination, table, fields, where, rowCount });
}

function buildVbelnInClause(vbelns) {
  return vbelns.map((v) => `'${escapeAbap(v)}'`).join(",");
}

function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

async function queryLipsForVbelns(destination, vbelns) {
  if (!vbelns || vbelns.length === 0) return [];

  const unique = [...new Set(vbelns)];
  const batches = chunkArray(unique, VBELN_BATCH_SIZE);
  const results = [];

  for (const batch of batches) {
    const where = `VBELN IN (${buildVbelnInClause(batch)})`;
    const rows = await queryTable(destination, "LIPS", LIPS_FIELDS, where);
    results.push(...rows);
  }

  return results;
}

// ---------------------------------------------------------------------------
// Delivery type texts (non-fatal)
// ---------------------------------------------------------------------------

async function fetchDeliveryTypeTexts(destination, lfartCodes) {
  if (!lfartCodes || lfartCodes.length === 0) return new Map();

  try {
    const inClause = lfartCodes.map((c) => `'${escapeAbap(c)}'`).join(",");
    const records = await queryTable(
      destination,
      "TVSAKT",
      ["LFART", "VTEXT"],
      `SPRAS = 'S' AND LFART IN (${inClause})`
    );

    const texts = new Map();
    for (const r of records) {
      const code = String(r.LFART || "").trim();
      if (code && !texts.has(code)) {
        texts.set(code, String(r.VTEXT || "").trim());
      }
    }
    return texts;
  } catch (err) {
    logger.warn({ err: err.message }, "No se pudieron obtener textos de tipos de entrega (TVSAKT)");
    return new Map();
  }
}

function collectLfartCodes(likpRows) {
  const codes = new Set();
  for (const row of likpRows) {
    const lfart = String(row.LFART || "").trim();
    if (lfart) codes.add(lfart);
  }
  return [...codes];
}

function collectVbelns(likpRows) {
  const vbelns = new Set();
  for (const row of likpRows) {
    const v = String(row.VBELN || "").trim();
    if (v) vbelns.add(v);
  }
  return [...vbelns];
}

// ---------------------------------------------------------------------------
// Extraction functions (same interface as before)
// ---------------------------------------------------------------------------

async function extractByVbeln(destination, vbeln) {
  const where = `VBELN = '${escapeAbap(vbeln)}'`;

  const [likpRows, lipsRows] = await Promise.all([
    queryTable(destination, "LIKP", LIKP_FIELDS, where),
    queryTable(destination, "LIPS", LIPS_FIELDS, where),
  ]);

  const deliveryTypeTexts = await fetchDeliveryTypeTexts(
    destination,
    collectLfartCodes(likpRows)
  );

  return {
    likp_rows: likpRows,
    lips_rows: lipsRows,
    delivery_type_texts: deliveryTypeTexts,
  };
}

async function extractByXblnr(destination, xblnr) {
  const likpRows = await queryTable(
    destination,
    "LIKP",
    LIKP_FIELDS,
    `XBLNR = '${escapeAbap(xblnr)}'`
  );

  if (likpRows.length === 0) {
    return { likp_rows: [], lips_rows: [], delivery_type_texts: new Map() };
  }

  const vbelns = collectVbelns(likpRows);

  const [lipsRows, deliveryTypeTexts] = await Promise.all([
    queryLipsForVbelns(destination, vbelns),
    fetchDeliveryTypeTexts(destination, collectLfartCodes(likpRows)),
  ]);

  return {
    likp_rows: likpRows,
    lips_rows: lipsRows,
    delivery_type_texts: deliveryTypeTexts,
  };
}

async function extractByDateRange(destination, fromDate, toDate) {
  const sapFrom = formatDateToSap(new Date(fromDate + "T00:00:00Z"));
  const sapTo = formatDateToSap(new Date(toDate + "T00:00:00Z"));

  const likpRows = await queryTable(
    destination,
    "LIKP",
    LIKP_FIELDS,
    `ERDAT >= '${sapFrom}' AND ERDAT <= '${sapTo}'`
  );

  if (likpRows.length === 0) {
    return { likp_rows: [], lips_rows: [], delivery_type_texts: new Map() };
  }

  const vbelns = collectVbelns(likpRows);

  const [lipsRows, deliveryTypeTexts] = await Promise.all([
    queryLipsForVbelns(destination, vbelns),
    fetchDeliveryTypeTexts(destination, collectLfartCodes(likpRows)),
  ]);

  return {
    likp_rows: likpRows,
    lips_rows: lipsRows,
    delivery_type_texts: deliveryTypeTexts,
  };
}

module.exports = {
  extractByVbeln,
  extractByXblnr,
  extractByDateRange,
};
