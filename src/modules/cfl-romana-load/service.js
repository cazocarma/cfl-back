const crypto = require("crypto");
const { buildDomainError } = require("../../helpers");
const { config } = require("../../config");
const { logger } = require("../../logger");
const { JOB_STATUS, JOB_TYPE } = require("./constants");
const { extractByDateRange, extractByNPartida, extractByGuiaDespacho } = require("./romana-etl-client");
const { persistExtraction } = require("./repository");

class CflRomanaLoadService {
  async createDateRangeJob({ centro, fechaDesde, fechaHasta, authnClaims }) {
    if (!centro) throw buildDomainError("centro es requerido", 400);
    if (!fechaDesde || !fechaHasta) throw buildDomainError("fecha_desde y fecha_hasta son requeridos", 400);
    if (fechaDesde > fechaHasta) throw buildDomainError("fecha_desde no puede ser mayor que fecha_hasta", 400);
    return this._execute(JOB_TYPE.DATE_RANGE, () => extractByDateRange(centro, fechaDesde, fechaHasta));
  }

  async createNPartidaJob({ centro, nPartida, fechaReferencia, authnClaims }) {
    if (!centro) throw buildDomainError("centro es requerido", 400);
    if (!nPartida) throw buildDomainError("n_partida es requerido", 400);
    const fecha = this._validateFechaReferencia(fechaReferencia);
    return this._execute(JOB_TYPE.NPARTIDA, () => extractByNPartida(centro, String(nPartida).trim(), fecha));
  }

  async createGuiaJob({ centro, guia, fechaReferencia, authnClaims }) {
    if (!centro) throw buildDomainError("centro es requerido", 400);
    if (!guia) throw buildDomainError("guia es requerida", 400);
    const fecha = this._validateFechaReferencia(fechaReferencia);
    return this._execute("GUIA", () => extractByGuiaDespacho(centro, String(guia).trim(), fecha));
  }

  _validateFechaReferencia(value) {
    if (!value) throw buildDomainError("fecha_referencia es requerida", 400);
    const s = String(value).trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) {
      throw buildDomainError("fecha_referencia debe tener formato YYYY-MM-DD", 400);
    }
    const ref = new Date(`${s}T00:00:00`);
    if (isNaN(ref.getTime())) throw buildDomainError("fecha_referencia inválida", 400);
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    if (ref.getTime() > today.getTime()) {
      throw buildDomainError("fecha_referencia no puede ser futura", 400);
    }
    const maxPast = new Date(today);
    maxPast.setFullYear(today.getFullYear() - 2);
    if (ref.getTime() < maxPast.getTime()) {
      throw buildDomainError("fecha_referencia no puede ser anterior a 2 años", 400);
    }
    return s;
  }

  async _execute(jobType, extractFn) {
    const jobId = crypto.randomUUID();
    const sourceSystem = `ROMANA_${config.sapEtl.defaultDestination || "PRD"}`;

    logger.info({ jobId, jobType }, "Romana job started");
    const extraction = await extractFn();

    if (extraction.cabecera_rows.length === 0) {
      return { job_id: jobId, status: JOB_STATUS.COMPLETED, message: "Sin datos para los parametros solicitados", totals: { filas_extraidas: 0, filas_insertadas: 0, filas_actualizadas: 0, filas_sin_cambio: 0 } };
    }

    const result = await persistExtraction(jobId, sourceSystem, extraction);
    logger.info({ jobId, totals: result.totals }, "Romana job completed");

    return { job_id: jobId, status: JOB_STATUS.COMPLETED, message: `Carga completada: ${result.raw.cabecera_rows_inserted} cabeceras, ${result.raw.detalle_rows_inserted} detalles insertados`, ...result };
  }
}

const cflRomanaLoadService = new CflRomanaLoadService();
module.exports = { cflRomanaLoadService };
