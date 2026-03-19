const { config } = require("../../config");
const { buildDomainError } = require("../../helpers");

// ---------------------------------------------------------------------------
// HTTP transport
// ---------------------------------------------------------------------------

async function callSapAdapter(method, path, body) {
  if (!config.sapAdapter.baseUrl || !config.sapAdapter.token) {
    throw buildDomainError(
      "SAP adapter no configurado. Revisa SAP_ADAPTER_BASE_URL y SAP_ADAPTER_API_TOKEN",
      500
    );
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.sapAdapter.requestTimeoutMs);

  try {
    const options = {
      method,
      headers: {
        Authorization: `Bearer ${config.sapAdapter.token}`,
      },
      signal: controller.signal,
    };

    if (body !== undefined && body !== null) {
      options.headers["Content-Type"] = "application/json";
      options.body = JSON.stringify(body);
    }

    const response = await fetch(`${config.sapAdapter.baseUrl}${path}`, options);

    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }

    if (!response.ok || payload?.success === false) {
      const errorDetail = payload?.error || {};
      const message =
        errorDetail.detail ||
        errorDetail.message ||
        `sap-adapter respondio ${response.status}`;

      const error = new Error(message);
      error.statusCode = response.status >= 500 ? response.status : 502;
      throw error;
    }

    return payload?.data || null;
  } catch (error) {
    if (error.name === "AbortError") {
      const timeoutError = new Error("sap-adapter excedio el timeout configurado");
      timeoutError.statusCode = 504;
      throw timeoutError;
    }

    if (error.statusCode) {
      throw error;
    }

    const networkError = new Error(`No se pudo conectar con sap-adapter: ${error.message}`);
    networkError.statusCode = 502;
    throw networkError;
  } finally {
    clearTimeout(timeout);
  }
}

// ---------------------------------------------------------------------------
// CFL response → raw SAP row mapping
//
// The specific CFL endpoints on the adapter return camelCase DTOs with parsed
// dates/times/decimals.  The repository pipeline expects raw SAP dictionaries
// (VBELN, ERDAT as "yyyyMMdd", etc.) so that hashing remains deterministic.
// These helpers reverse-map the DTOs back to the raw format.
// ---------------------------------------------------------------------------

function cflDateToSap(value) {
  if (!value) return "";
  return String(value).slice(0, 10).replace(/-/g, "");
}

function cflTimeToSap(value) {
  if (!value) return "";
  return String(value).replace(/:/g, "");
}

function cflDecimalToRaw(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}

function mapCflHeaderToLikpRow(h) {
  return {
    VBELN: h.sapNumeroEntrega || "",
    XBLNR: h.sapReferencia || "",
    VSTEL: h.sapPuestoExpedicion || "",
    KUNNR: h.sapDestinatario || "",
    VKORG: h.sapOrganizacionVentas || "",
    ERNAM: h.sapCreadoPor || "",
    ERDAT: cflDateToSap(h.sapFechaCreacion),
    LFART: h.sapClaseEntrega || "",
    YWT_CDFIELD11: cflDateToSap(h.sapFechaCarga),
    YWT_CDFIELD12: cflTimeToSap(h.sapHoraCarga),
    YWT_XBLNR: h.sapGuiaRemision || "",
    YWT_DDFIELD21: h.sapNombreChofer || "",
    YWT_DDFIELD23: h.sapIdFiscalChofer || "",
    YWT_CDFIELD24: h.sapEmpresaTransporte || "",
    YWT_DDFIELD27: h.sapPatente || "",
    YWT_DDFIELD28: h.sapCarro || "",
    YWT_DDFIELD29: cflDateToSap(h.sapFechaSalida),
    YWT_DDFIELD30: cflTimeToSap(h.sapHoraSalida),
    YWT_CDFIELD92: h.sapCodigoTipoFlete || "",
    BTGEW: cflDecimalToRaw(h.sapPesoTotal),
    NTGEW: cflDecimalToRaw(h.sapPesoNeto),
    WADAT_IST: cflDateToSap(h.sapFechaEntregaReal),
  };
}

function mapCflItemToLipsRow(item) {
  return {
    VBELN: item.sapNumeroEntrega || "",
    POSNR: item.sapPosicion || "",
    UECHA: item.sapPosEntregaSuperior || "",
    UEPOS: item.sapPosReferenciaEntrega || "",
    MATNR: item.sapMaterial || "",
    CHARG: item.sapLote || "",
    LFIMG: cflDecimalToRaw(item.sapCantidadEntregada),
    GEWEI: item.sapUnidadPeso || "",
    ARKTX: item.sapDenominacionMaterial || "",
    WERKS: item.sapCentro || "",
    LGORT: item.sapAlmacen || "",
    ERDAT: cflDateToSap(item.sapFechaCreacion),
  };
}

function mapCflDeliveryTypeTexts(textsObj) {
  const texts = new Map();
  if (textsObj && typeof textsObj === "object") {
    for (const [lfart, text] of Object.entries(textsObj)) {
      if (lfart && !texts.has(lfart)) {
        texts.set(lfart, String(text || "").trim());
      }
    }
  }
  return texts;
}

function mapCflResponseToExtraction(data) {
  const headers = Array.isArray(data?.headers) ? data.headers : [];
  const items = Array.isArray(data?.items) ? data.items : [];
  const textsObj = data?.deliveryTypeTexts || {};

  return {
    likp_rows: headers.map(mapCflHeaderToLikpRow),
    lips_rows: items.map(mapCflItemToLipsRow),
    delivery_type_texts: mapCflDeliveryTypeTexts(textsObj),
  };
}

// ---------------------------------------------------------------------------
// Specific CFL extraction functions
// ---------------------------------------------------------------------------

async function extractByVbeln(destination, vbeln) {
  const qs = `destination=${encodeURIComponent(destination)}&includeDeliveryTypeTexts=true`;
  const data = await callSapAdapter(
    "GET",
    `/api/v1/cfl/deliveries/${encodeURIComponent(vbeln)}?${qs}`,
    null
  );
  return mapCflResponseToExtraction(data);
}

async function extractByDateRange(destination, fromDate, toDate) {
  const data = await callSapAdapter(
    "POST",
    "/api/v1/cfl/deliveries/by-date-range",
    {
      destination,
      fromDate,
      toDate,
      includeDeliveryTypeTexts: true,
    }
  );
  return mapCflResponseToExtraction(data);
}

module.exports = {
  extractByVbeln,
  extractByDateRange,
};
