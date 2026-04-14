const { config } = require("../../config");
const { logger } = require("../../logger");
const { queryOdata } = require("../sap-sync/sap-query");

const ODATA_DESTINATION = "PRD_GW";
const ODATA_ENTITY_SET = "YWTGW_GET_REPORT_ROMANA_SRV/ywtgw_get_romana_detSet";

function f(record, name) {
  return record[name] ?? record[name.charAt(0).toLowerCase() + name.slice(1)] ?? "";
}

function str(record, name, max) {
  return String(f(record, name) || "").trim().slice(0, max || 999);
}

function num(record, name) {
  return Number(f(record, name) || 0) || 0;
}

/**
 * Normaliza cualquier valor de fecha que llegue desde el ETL de Romana a un
 * string ISO `YYYY-MM-DD`, que mssql acepta sin ambigüedad en columnas DATE.
 *
 * Casos detectados en producción:
 *   - SAP/OData devuelve "09-02-2026" (DD-MM-YYYY chileno). El driver `sql.Date`
 *     o `new Date(...)` lo interpreta como MM-DD-YYYY y termina persistiendo
 *     `2026-09-02`. Raíz del bug de "fechas en septiembre en vez de febrero".
 *   - También puede venir como "09/02/2026", "09.02.2026", "20260209",
 *     ISO "2026-02-09[T...]", o Date object.
 *   - `/Date(1234567890000)/` (Microsoft JSON date) — formato raro pero posible.
 *
 * Devuelve `null` si la entrada no se puede interpretar.
 */
function normalizeSapDate(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date) {
    return isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
  }
  const s = String(value).trim();
  if (!s) return null;

  // Ya es ISO (con o sin hora).
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;

  // SAP compacto "YYYYMMDD".
  const compact = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (compact) return `${compact[1]}-${compact[2]}-${compact[3]}`;

  // DD[/.-]MM[/.-]YYYY (formato chileno / europeo). Acepta sufijo de hora.
  const dmy = s.match(/^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{4})(?:[\sT].*)?$/);
  if (dmy) {
    const d = dmy[1].padStart(2, "0");
    const m = dmy[2].padStart(2, "0");
    return `${dmy[3]}-${m}-${d}`;
  }

  // /Date(ms)/ (Microsoft JSON).
  const msDate = s.match(/^\/Date\((-?\d+)\)\/$/);
  if (msDate) {
    const d = new Date(Number(msDate[1]));
    return isNaN(d.getTime()) ? null : d.toISOString().slice(0, 10);
  }

  // Último recurso: new Date(). Evitamos esto para inputs DD-MM-YYYY porque
  // JS lo interpreta como MM-DD-YYYY.
  const parsed = new Date(s);
  return isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
}

function splitHeaderAndDetail(records) {
  const headersMap = new Map();
  const details = [];

  for (const r of records) {
    const nPartida = str(r, "NPartida", 20);
    const guia = str(r, "Guia", 30);
    if (!nPartida && !guia) continue;

    const headerKey = `${nPartida}|${guia}`;
    const posnr = str(r, "Posnr", 10);

    if (!headersMap.has(headerKey)) {
      headersMap.set(headerKey, {
        idRomana: str(r, "IdRomana", 20),
        numeroPartida: nPartida,
        guiaDespacho: guia,
        tipoDocumento: str(r, "Tdoc", 10),
        tipoDocumentoTexto: str(r, "TextTdoc", 40),
        estadoRomana: str(r, "Estado", 10),
        estadoRomanaTexto: str(r, "TextEstado", 40),
        patente: str(r, "Patente", 20),
        carro: str(r, "Carro", 20),
        conductor: str(r, "Conductor", 80),
        creadoPor: str(r, "Ernam", 12),
        creadoPorNombre: str(r, "TextErnam", 80),
        fechaCreacionSap: normalizeSapDate(f(r, "Erdat")),
        fechaModificacionSap: normalizeSapDate(f(r, "Aedat")),
        ordenCompra: str(r, "Ebeln", 20),
        codigoProductor: str(r, "CampoProduc", 20),
        centro: str(r, "Werks", 10),
        centroNombre: str(r, "TextWerks", 40),
        plantaDestino: str(r, "Pdest", 10),
        plantaDestinoNombre: str(r, "TextPdest", 40),
        almacenDestino: str(r, "Adest", 10),
        almacenDestinoNombre: str(r, "TextAdest", 40),
        temporada: str(r, "YwtBtfield40", 10),
        csg: str(r, "YwtBtfield17", 20),
        guiaAlterna: str(r, "YwtBtfield02", 30),
        productorDescripcion: str(r, "TextYwtBtfield17", 80),
        productorDireccion: str(r, "DirYwtBtfield17", 150),
        productorComuna: str(r, "YwtBtfield23", 60),
        productorProvincia: str(r, "YwtBtfield24", 60),
        peticionBorrado: f(r, "Loevm") === true || f(r, "Loevm") === "true",
        actualizadoPor: str(r, "ErnamUpdate", 12),
        actualizadoPorNombre: str(r, "TextErnamUp", 80),
      });
    }

    if (posnr) {
      details.push({
        numeroPartida: nPartida,
        guiaDespacho: guia,
        posicion: posnr,
        material: str(r, "Matnr", 40),
        materialDescripcion: str(r, "Maktg", 40),
        lote: str(r, "Charg", 20),
        pesoReal: num(r, "PesoReal"),
        unidadMedida: str(r, "Meins", 5),
        envase: str(r, "Envase", 20),
        envaseDescripcion: str(r, "TextEnvase", 40),
        subEnvase: str(r, "SubEnvase", 20),
        subEnvaseDescripcion: str(r, "TextSubEnvase", 40),
        posicionOrdenCompra: str(r, "Ebelp", 10),
        codigoEspecie: str(r, "YwtBdfield01", 10),
        especieDescripcion: str(r, "Field01Denomina", 40),
        codigoGrupoVariedad: str(r, "YwtBdfield02", 10),
        grupoVariedadDescripcion: str(r, "Field02Denomina", 40),
        codigoManejo: str(r, "YwtBdfield03", 10),
        manejoDescripcion: str(r, "Field03Denomina", 40),
        centro: str(r, "Werks", 10),
        almacen: str(r, "Lgort", 10),
        almacenDescripcion: str(r, "TextLgort", 40),
        variedadAgronomica: str(r, "YwtBtfield08", 10),
        variedadAgronomicaDescripcion: str(r, "TextVagro", 40),
        tipoVariedad: str(r, "YwtBtfield46", 10),
        tipoVariedadDescripcion: str(r, "TextTvariedad", 40),
        tipoFrio: str(r, "YwtBtfield13", 10),
        tipoFrioDescripcion: str(r, "TextTfrio", 40),
        destino: str(r, "YwtBtfield49", 10),
        destinoDescripcion: str(r, "TextDest", 40),
        lineaProduccion: str(r, "YwtBtfield28", 20),
        fechaCosecha: normalizeSapDate(f(r, "YwtBtfield09")),
        psa: str(r, "YwtBtfield18", 20),
        ggn: str(r, "YwtBtfield19", 20),
        sdp: str(r, "YwtBtfield52", 10),
        unidadMadurez: str(r, "YwtBtfield51", 20),
        cuartel: str(r, "YwtBtfield21", 20),
        exportadorMP: str(r, "YwtBtfield63", 20),
        exportadorMPDescripcion: str(r, "Btfield63DenominaLarg", 80),
        pesoPromedioEnvase: str(r, "CheckPesPro", 20),
        pesoRealEnvase: str(r, "CheckPesReal", 20),
        cantidadSubEnvaseL: num(r, "CantSubEnvL") || null,
        pesoEnvase: num(r, "PesoEnv") || null,
        pesoSubEnvase: num(r, "PesoSenv") || null,
        cantidadSubEnvaseV: num(r, "CantSubEnvV") || null,
      });
    }
  }

  return {
    cabecera_rows: [...headersMap.values()],
    detalle_rows: details,
  };
}

// ---------------------------------------------------------------------------
// Extraction functions (sin Werks obligatorio)
// ---------------------------------------------------------------------------

async function extractByDateRange(centro, fromDate, toDate) {
  const filter = `Werks eq '${centro}' and (Erdat ge datetime'${fromDate}T00:00:00' and Erdat le datetime'${toDate}T23:59:59')`;
  logger.info({ filter }, "Consultando Romana OData por rango de fechas");
  const records = await queryOdata(ODATA_DESTINATION, ODATA_ENTITY_SET, { filter });
  return splitHeaderAndDetail(records || []);
}

async function extractByNPartida(centro, nPartida) {
  const filter = `Werks eq '${centro}' and NPartida eq '${nPartida}'`;
  logger.info({ filter }, "Consultando Romana OData por N° partida");
  const records = await queryOdata(ODATA_DESTINATION, ODATA_ENTITY_SET, { filter });
  return splitHeaderAndDetail(records || []);
}

async function extractByGuiaDespacho(centro, guia) {
  const filter = `Werks eq '${centro}' and Guia eq '${guia}'`;
  logger.info({ filter }, "Consultando Romana OData por guía de despacho");
  const records = await queryOdata(ODATA_DESTINATION, ODATA_ENTITY_SET, { filter });
  return splitHeaderAndDetail(records || []);
}

module.exports = {
  extractByDateRange,
  extractByNPartida,
  extractByGuiaDespacho,
};
