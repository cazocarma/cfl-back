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
        fechaCreacionSap: f(r, "Erdat") || null,
        fechaModificacionSap: f(r, "Aedat") || null,
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
        fechaCosecha: f(r, "YwtBtfield09") || null,
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
