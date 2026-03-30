const crypto = require("crypto");
const { buildDomainError } = require("../../helpers");
const { config } = require("../../config");
const { JOB_STATUS, JOB_TYPE } = require("./constants");
const {
  parseIsoDate,
  formatDateToIso,
  normalizeDestination,
  normalizeVbeln,
  normalizeXblnr,
  inclusiveDateRangeDays,
  buildScopeKey,
  serializeJobParams,
  buildSourceSystem,
} = require("./utils");
const { extractByVbeln, extractByXblnr, extractByDateRange } = require("./sap-adapter-client");
const {
  failStaleJobs,
  insertQueuedJob,
  markJobRunning,
  updateJobSnapshot,
  markJobFinished,
  getJobRecord,
  getLatestJobRecord,
  getRecentJobRecords,
  persistExtraction,
} = require("./repository");

const JOB_POLL_INTERVAL_MS = 2500;

function normalizeRequestedSourceSystem(value) {
  const rawValue = String(value || "").trim().toUpperCase();
  const normalizedValue = rawValue.startsWith("SAP_") ? rawValue.slice(4) : rawValue;
  return normalizeDestination(normalizedValue, null);
}

function normalizeVbelnList(values) {
  const input = Array.isArray(values) ? values : [values];
  const unique = new Set();
  const normalized = [];

  for (const value of input) {
    const current = normalizeVbeln(value);
    if (!current) {
      return null;
    }
    if (unique.has(current)) {
      continue;
    }
    unique.add(current);
    normalized.push(current);
  }

  return normalized.length > 0 ? normalized : null;
}

function normalizeXblnrList(values) {
  const input = Array.isArray(values) ? values : [values];
  const unique = new Set();
  const normalized = [];

  for (const value of input) {
    const current = normalizeXblnr(value);
    if (!current) {
      return null;
    }
    if (unique.has(current)) {
      continue;
    }
    unique.add(current);
    normalized.push(current);
  }

  return normalized.length > 0 ? normalized : null;
}

function createEmptyBackendMetrics() {
  return {
    filas_extraidas: 0,
    filas_insertadas: 0,
    filas_actualizadas: 0,
    filas_sin_cambio: 0,
  };
}

function countUniqueDeliveries(rows) {
  const unique = new Set();
  for (const row of Array.isArray(rows) ? rows : []) {
    const value = normalizeVbeln(row?.VBELN || row?.vbeln || null);
    if (value) {
      unique.add(value);
    }
  }
  return unique.size;
}

function sumRawInserted(summary) {
  return Number(summary?.raw?.likp_rows_inserted || 0) + Number(summary?.raw?.lips_rows_inserted || 0);
}

function sumCanonicalUpdated(summary) {
  const canonical = summary?.canonical || {};
  return (
    Number(canonical.entregas_insertadas || 0) +
    Number(canonical.entregas_actualizadas || 0) +
    Number(canonical.posiciones_insertadas || 0) +
    Number(canonical.posiciones_actualizadas || 0)
  );
}

function accumulateBackendMetrics(target, summary) {
  const totals = summary?.totals || {};
  target.filas_extraidas += Number(totals.filas_extraidas || 0);
  target.filas_insertadas += Number(totals.filas_insertadas || 0);
  target.filas_actualizadas += Number(totals.filas_actualizadas || 0);
  target.filas_sin_cambio += Number(totals.filas_sin_cambio || 0);
}

function buildUiRequestType(jobDefinition) {
  if (jobDefinition.job_type === JOB_TYPE.VBELN) return "vbeln";
  if (jobDefinition.job_type === JOB_TYPE.XBLNR) return "xblnr";
  return "rango_fechas";
}

function createBaseSnapshot(jobDefinition) {
  const now = new Date().toISOString();
  return {
    tipo_solicitud: buildUiRequestType(jobDefinition),
    estado: JOB_STATUS.QUEUED,
    etapa_actual: "en_cola",
    mensaje: "Job en cola esperando turno de procesamiento.",
    creado_en: now,
    actualizado_en: now,
    iniciado_en: null,
    finalizado_en: null,
    porcentaje_avance: 0,
    source_system: buildSourceSystem(jobDefinition.destination),
    poll_interval_ms: JOB_POLL_INTERVAL_MS,
    parametros: {
      vbeln: jobDefinition.job_type === JOB_TYPE.VBELN ? [...jobDefinition.vbelns] : null,
      xblnr: jobDefinition.job_type === JOB_TYPE.XBLNR ? [...jobDefinition.xblnrs] : null,
      fecha_desde: jobDefinition.fecha_desde || null,
      fecha_hasta: jobDefinition.fecha_hasta || null,
    },
    resumen: {
      solicitados:
        jobDefinition.job_type === JOB_TYPE.VBELN ? jobDefinition.vbelns.length :
        jobDefinition.job_type === JOB_TYPE.XBLNR ? jobDefinition.xblnrs.length : 0,
      procesados: 0,
      insertados_raw: 0,
      actualizados_canonicos: 0,
      omitidos: 0,
      errores: 0,
    },
    resultados: [],
    errores: [],
    backend_metrics: createEmptyBackendMetrics(),
  };
}

function cloneSnapshot(snapshot) {
  return JSON.parse(JSON.stringify(snapshot));
}

function touchSnapshot(snapshot, updates = {}) {
  const touchedAt = new Date().toISOString();
  const next = {
    ...snapshot,
    ...updates,
    actualizado_en: touchedAt,
  };

  if (!next.creado_en) {
    next.creado_en = touchedAt;
  }

  return next;
}

function buildVbelnSuccessResult(vbeln, summary) {
  const rawInserted = sumRawInserted(summary);
  const canonicalUpdated = sumCanonicalUpdated(summary);
  const detailParts = [
    `raw insertado=${rawInserted}`,
    `canonico actualizado=${canonicalUpdated}`,
    `filas extraidas=${Number(summary?.totals?.filas_extraidas || 0)}`,
  ];

  return {
    vbeln,
    sap_numero_entrega: vbeln,
    estado: "COMPLETED",
    accion: "sincronizado",
    detalle: detailParts.join(", "),
    id_sap_entrega: null,
    id_cabecera_flete: null,
  };
}

function buildVbelnOmittedResult(vbeln) {
  return {
    vbeln,
    sap_numero_entrega: vbeln,
    estado: "OMITTED",
    accion: "sin_datos",
    detalle: "SAP no devolvio filas LIKP para el VBELN solicitado.",
    id_sap_entrega: null,
    id_cabecera_flete: null,
  };
}

function buildVbelnErrorResult(vbeln, error) {
  return {
    vbeln,
    sap_numero_entrega: vbeln,
    estado: "FAILED",
    accion: "error",
    detalle: String(error?.message || "Fallo inesperado al procesar el VBELN"),
    id_sap_entrega: null,
    id_cabecera_flete: null,
  };
}

function buildErrorItem({ code, message, detail, vbeln = null, stage = null }) {
  return {
    codigo: code || "JOB_ERROR",
    mensaje: message || "Error no especificado",
    detalle: detail || message || null,
    vbeln,
    etapa: stage,
  };
}

function deriveFinalStatus(snapshot) {
  const processed = Number(snapshot?.resumen?.procesados || 0);
  const errors = Number(snapshot?.resumen?.errores || 0);
  const omitted = Number(snapshot?.resumen?.omitidos || 0);

  if (processed > 0 && errors === 0 && omitted === 0) {
    return JOB_STATUS.COMPLETED;
  }
  if (processed > 0) {
    return JOB_STATUS.PARTIAL_SUCCESS;
  }
  if (errors > 0) {
    return JOB_STATUS.FAILED;
  }
  if (omitted > 0) {
    return JOB_STATUS.PARTIAL_SUCCESS;
  }
  return JOB_STATUS.COMPLETED;
}

function buildFinalMessage(snapshot) {
  const requested = Number(snapshot?.resumen?.solicitados || 0);
  const processed = Number(snapshot?.resumen?.procesados || 0);
  const omitted = Number(snapshot?.resumen?.omitidos || 0);
  const errors = Number(snapshot?.resumen?.errores || 0);

  if (snapshot.estado === JOB_STATUS.COMPLETED) {
    return requested > 0
      ? `Job completado. ${processed} de ${requested} solicitudes procesadas sin observaciones.`
      : "Job completado.";
  }

  if (snapshot.estado === JOB_STATUS.PARTIAL_SUCCESS) {
    return `Job completado con observaciones. Procesados=${processed}, omitidos=${omitted}, errores=${errors}.`;
  }

  return `Job fallido. Procesados=${processed}, omitidos=${omitted}, errores=${errors}.`;
}

function buildUnexpectedFailureSnapshot(jobDefinition, error) {
  const snapshot = createBaseSnapshot(jobDefinition);
  const failed = touchSnapshot(snapshot, {
    estado: JOB_STATUS.FAILED,
    etapa_actual: "fallido",
    porcentaje_avance: 100,
    iniciado_en: new Date().toISOString(),
    finalizado_en: new Date().toISOString(),
    mensaje: String(error?.message || "La ejecucion termino con error"),
  });
  failed.errores.push(
    buildErrorItem({
      code: "JOB_FATAL",
      message: failed.mensaje,
      detail: failed.mensaje,
      stage: "job",
    })
  );
  failed.resumen.errores = Math.max(1, Number(failed.resumen.errores || 0));
  return failed;
}

class CflSapLoadService {
  constructor() {
    this.pendingJobs = [];
    this.activeJobId = null;
    this.scopeJobs = new Map();
    this.initialized = false;
    this.processing = false;
    this.initPromise = null;
  }

  async initialize() {
    if (this.initialized) {
      return;
    }
    if (!this.initPromise) {
      this.initPromise = failStaleJobs()
        .then(() => {
          this.initialized = true;
        })
        .catch((error) => {
          this.initPromise = null;
          throw error;
        });
    }
    await this.initPromise;
  }

  async createVbelnJob({ sourceSystem, destination, vbeln, authnClaims }) {
    const normalizedDestination = normalizeDestination(
      normalizeRequestedSourceSystem(sourceSystem) || destination,
      config.sapAdapter.defaultDestination
    );
    if (!normalizedDestination) {
      throw buildDomainError("source_system invalido", 400);
    }

    const normalizedVbelns = normalizeVbelnList(vbeln);
    if (!normalizedVbelns) {
      throw buildDomainError("vbeln invalido. Debe enviar un arreglo con al menos un VBELN valido", 400);
    }

    return this.enqueueJob({
      job_type: JOB_TYPE.VBELN,
      destination: normalizedDestination,
      vbelns: normalizedVbelns,
      fecha_desde: null,
      fecha_hasta: null,
      requested_by: {
        id_usuario: Number(authnClaims?.id_usuario || 0) || null,
        username: authnClaims?.username || null,
        role: authnClaims?.role || null,
      },
    });
  }

  async createXblnrJob({ sourceSystem, destination, xblnr, authnClaims }) {
    const normalizedDestination = normalizeDestination(
      normalizeRequestedSourceSystem(sourceSystem) || destination,
      config.sapAdapter.defaultDestination
    );
    if (!normalizedDestination) {
      throw buildDomainError("source_system invalido", 400);
    }

    const normalizedXblnrs = normalizeXblnrList(xblnr);
    if (!normalizedXblnrs) {
      throw buildDomainError("xblnr invalido. Debe enviar un arreglo con al menos un XBLNR valido", 400);
    }

    return this.enqueueJob({
      job_type: JOB_TYPE.XBLNR,
      destination: normalizedDestination,
      vbelns: [],
      xblnrs: normalizedXblnrs,
      fecha_desde: null,
      fecha_hasta: null,
      requested_by: {
        id_usuario: Number(authnClaims?.id_usuario || 0) || null,
        username: authnClaims?.username || null,
        role: authnClaims?.role || null,
      },
    });
  }

  async createDateRangeJob({ sourceSystem, destination, fechaDesde, fechaHasta, authnClaims }) {
    const normalizedDestination = normalizeDestination(
      normalizeRequestedSourceSystem(sourceSystem) || destination,
      config.sapAdapter.defaultDestination
    );
    if (!normalizedDestination) {
      throw buildDomainError("source_system invalido", 400);
    }

    const fromDate = parseIsoDate(fechaDesde);
    const toDate = parseIsoDate(fechaHasta);
    if (!fromDate || !toDate) {
      throw buildDomainError("fecha_desde y fecha_hasta deben usar formato YYYY-MM-DD", 400);
    }
    if (fromDate.getTime() > toDate.getTime()) {
      throw buildDomainError("fecha_desde no puede ser mayor que fecha_hasta", 400);
    }

    const rangeDays = inclusiveDateRangeDays(fromDate, toDate);
    if (rangeDays > config.cflSapLoad.maxDateRangeDays) {
      throw buildDomainError(
        `El rango no puede exceder ${config.cflSapLoad.maxDateRangeDays} dias`,
        422
      );
    }

    return this.enqueueJob({
      job_type: JOB_TYPE.DATE_RANGE,
      destination: normalizedDestination,
      vbelns: [],
      fecha_desde: formatDateToIso(fromDate),
      fecha_hasta: formatDateToIso(toDate),
      requested_by: {
        id_usuario: Number(authnClaims?.id_usuario || 0) || null,
        username: authnClaims?.username || null,
        role: authnClaims?.role || null,
      },
    });
  }

  async enqueueJob(jobDefinition) {
    await this.initialize();

    const scopeKey = buildScopeKey(jobDefinition);
    if (this.scopeJobs.has(scopeKey)) {
      const error = buildDomainError("Ya existe un job activo o en cola para la misma carga", 409);
      error.data = {
        existing_job_id: this.scopeJobs.get(scopeKey),
      };
      throw error;
    }

    const jobId = crypto.randomUUID();
    const fullJobDefinition = {
      ...jobDefinition,
      job_id: jobId,
    };

    const initialSnapshot = createBaseSnapshot(fullJobDefinition);
    fullJobDefinition.parametros_json = serializeJobParams(fullJobDefinition);
    fullJobDefinition.resumen_json = JSON.stringify(initialSnapshot);

    await insertQueuedJob(fullJobDefinition);

    this.pendingJobs.push({
      job_id: jobId,
      scope_key: scopeKey,
      definition: fullJobDefinition,
    });
    this.scopeJobs.set(scopeKey, jobId);

    void this.processQueue();

    return getJobRecord(jobId, this.getQueueSnapshot(jobId));
  }

  async processQueue() {
    if (this.processing) {
      return;
    }

    this.processing = true;
    try {
      while (this.pendingJobs.length > 0) {
        const nextJob = this.pendingJobs.shift();
        this.activeJobId = nextJob.job_id;

        try {
          const runningSnapshot = touchSnapshot(createBaseSnapshot(nextJob.definition), {
            estado: JOB_STATUS.RUNNING,
            etapa_actual: "inicializando",
            iniciado_en: new Date().toISOString(),
            porcentaje_avance: 1,
            mensaje: "Job en ejecucion. Preparando extraccion SAP.",
          });

          await markJobRunning(nextJob.job_id, runningSnapshot);

          const finalSnapshot = await this.executeJob(nextJob.definition, runningSnapshot);
          await markJobFinished(
            nextJob.job_id,
            finalSnapshot.estado,
            finalSnapshot,
            finalSnapshot.estado === JOB_STATUS.FAILED ? finalSnapshot.mensaje : null
          );
        } catch (error) {
          const failedSnapshot = buildUnexpectedFailureSnapshot(nextJob.definition, error);
          await markJobFinished(nextJob.job_id, JOB_STATUS.FAILED, failedSnapshot, error.message);
        } finally {
          this.activeJobId = null;
          this.scopeJobs.delete(nextJob.scope_key);
        }
      }
    } finally {
      this.processing = false;
    }
  }

  async executeJob(jobDefinition, runningSnapshot) {
    if (jobDefinition.job_type === JOB_TYPE.VBELN) {
      return this.executeVbelnJob(jobDefinition, runningSnapshot);
    }
    if (jobDefinition.job_type === JOB_TYPE.XBLNR) {
      return this.executeXblnrJob(jobDefinition, runningSnapshot);
    }

    return this.executeDateRangeJob(jobDefinition, runningSnapshot);
  }

  async executeVbelnJob(jobDefinition, runningSnapshot) {
    const snapshot = cloneSnapshot(runningSnapshot);
    const total = jobDefinition.vbelns.length;

    snapshot.resumen.solicitados = total;
    snapshot.mensaje = `Procesando ${total} VBELN solicitado(s).`;
    snapshot.actualizado_en = new Date().toISOString();
    await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

    for (let index = 0; index < jobDefinition.vbelns.length; index += 1) {
      const vbeln = jobDefinition.vbelns[index];
      Object.assign(
        snapshot,
        touchSnapshot(snapshot, {
          etapa_actual: "extrayendo",
          mensaje: `Extrayendo VBELN ${index + 1} de ${total}: ${vbeln}.`,
          porcentaje_avance: Math.min(95, Math.max(5, Math.round((index / total) * 90))),
        })
      );
      await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

      try {
        const extraction = await extractByVbeln(jobDefinition.destination, vbeln);

        if ((extraction?.likp_rows || []).length === 0) {
          snapshot.resumen.omitidos += 1;
          snapshot.resultados.push(buildVbelnOmittedResult(vbeln));
          snapshot.errores.push(
            buildErrorItem({
              code: "VBELN_NO_DATA",
              message: "SAP no devolvio datos para el VBELN solicitado",
              detail: `No se encontraron filas LIKP para ${vbeln}.`,
              vbeln,
              stage: "extraccion",
            })
          );
        } else {
          Object.assign(
            snapshot,
            touchSnapshot(snapshot, {
              etapa_actual: "persistiendo",
              mensaje: `Persistiendo VBELN ${index + 1} de ${total}: ${vbeln}.`,
              porcentaje_avance: Math.min(
                95,
                Math.max(10, Math.round(((index + 0.5) / total) * 90))
              ),
            })
          );
          await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

          const persisted = await persistExtraction(jobDefinition, extraction);
          snapshot.resumen.procesados += 1;
          snapshot.resumen.insertados_raw += sumRawInserted(persisted);
          snapshot.resumen.actualizados_canonicos += sumCanonicalUpdated(persisted);
          accumulateBackendMetrics(snapshot.backend_metrics, persisted);
          snapshot.resultados.push(buildVbelnSuccessResult(vbeln, persisted));
        }
      } catch (error) {
        snapshot.resumen.errores += 1;
        snapshot.resultados.push(buildVbelnErrorResult(vbeln, error));
        snapshot.errores.push(
          buildErrorItem({
            code: "VBELN_PROCESSING_ERROR",
            message: `Fallo el procesamiento del VBELN ${vbeln}`,
            detail: error.message,
            vbeln,
            stage: snapshot.etapa_actual || "procesamiento",
          })
        );
      }

      const completedCount =
        Number(snapshot.resumen.procesados || 0) +
        Number(snapshot.resumen.omitidos || 0) +
        Number(snapshot.resumen.errores || 0);
      Object.assign(
        snapshot,
        touchSnapshot(snapshot, {
          etapa_actual: "procesando",
          mensaje: `Avance ${completedCount} de ${total} VBELN.`,
          porcentaje_avance: Math.min(95, Math.max(10, Math.round((completedCount / total) * 95))),
        })
      );
      await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);
    }

    snapshot.estado = deriveFinalStatus(snapshot);
    Object.assign(
      snapshot,
      touchSnapshot(snapshot, {
        etapa_actual: "finalizado",
        porcentaje_avance: 100,
        finalizado_en: new Date().toISOString(),
        mensaje: buildFinalMessage(snapshot),
      })
    );
    return snapshot;
  }

  async executeXblnrJob(jobDefinition, runningSnapshot) {
    const snapshot = cloneSnapshot(runningSnapshot);
    const total = jobDefinition.xblnrs.length;

    snapshot.resumen.solicitados = total;
    snapshot.mensaje = `Procesando ${total} XBLNR solicitado(s).`;
    snapshot.actualizado_en = new Date().toISOString();
    await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

    for (let index = 0; index < jobDefinition.xblnrs.length; index += 1) {
      const xblnr = jobDefinition.xblnrs[index];
      Object.assign(
        snapshot,
        touchSnapshot(snapshot, {
          etapa_actual: "extrayendo",
          mensaje: `Extrayendo XBLNR ${index + 1} de ${total}: ${xblnr}.`,
          porcentaje_avance: Math.min(95, Math.max(5, Math.round((index / total) * 90))),
        })
      );
      await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

      try {
        const extraction = await extractByXblnr(jobDefinition.destination, xblnr);

        if ((extraction?.likp_rows || []).length === 0) {
          snapshot.resumen.omitidos += 1;
          snapshot.resultados.push({
            vbeln: null,
            sap_numero_entrega: xblnr,
            estado: "OMITTED",
            accion: "sin_datos",
            detalle: "SAP no devolvio filas LIKP para el XBLNR solicitado.",
            id_sap_entrega: null,
            id_cabecera_flete: null,
          });
          snapshot.errores.push(
            buildErrorItem({
              code: "XBLNR_NO_DATA",
              message: "SAP no devolvio datos para el XBLNR solicitado",
              detail: `No se encontraron filas LIKP para ${xblnr}.`,
              vbeln: xblnr,
              stage: "extraccion",
            })
          );
        } else {
          Object.assign(
            snapshot,
            touchSnapshot(snapshot, {
              etapa_actual: "persistiendo",
              mensaje: `Persistiendo XBLNR ${index + 1} de ${total}: ${xblnr}.`,
              porcentaje_avance: Math.min(
                95,
                Math.max(10, Math.round(((index + 0.5) / total) * 90))
              ),
            })
          );
          await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

          const persisted = await persistExtraction(jobDefinition, extraction);
          snapshot.resumen.procesados += 1;
          snapshot.resumen.insertados_raw += sumRawInserted(persisted);
          snapshot.resumen.actualizados_canonicos += sumCanonicalUpdated(persisted);
          accumulateBackendMetrics(snapshot.backend_metrics, persisted);
          snapshot.resultados.push({
            vbeln: null,
            sap_numero_entrega: xblnr,
            estado: "COMPLETED",
            accion: "sincronizado",
            detalle: `raw insertado=${sumRawInserted(persisted)}, canonico actualizado=${sumCanonicalUpdated(persisted)}, filas extraidas=${Number(persisted?.totals?.filas_extraidas || 0)}`,
            id_sap_entrega: null,
            id_cabecera_flete: null,
          });
        }
      } catch (error) {
        snapshot.resumen.errores += 1;
        snapshot.resultados.push({
          vbeln: null,
          sap_numero_entrega: xblnr,
          estado: "FAILED",
          accion: "error",
          detalle: String(error?.message || "Fallo inesperado al procesar el XBLNR"),
          id_sap_entrega: null,
          id_cabecera_flete: null,
        });
        snapshot.errores.push(
          buildErrorItem({
            code: "XBLNR_PROCESSING_ERROR",
            message: `Fallo el procesamiento del XBLNR ${xblnr}`,
            detail: error.message,
            vbeln: xblnr,
            stage: snapshot.etapa_actual || "procesamiento",
          })
        );
      }

      const completedCount =
        Number(snapshot.resumen.procesados || 0) +
        Number(snapshot.resumen.omitidos || 0) +
        Number(snapshot.resumen.errores || 0);
      Object.assign(
        snapshot,
        touchSnapshot(snapshot, {
          etapa_actual: "procesando",
          mensaje: `Avance ${completedCount} de ${total} XBLNR.`,
          porcentaje_avance: Math.min(95, Math.max(10, Math.round((completedCount / total) * 95))),
        })
      );
      await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);
    }

    snapshot.estado = deriveFinalStatus(snapshot);
    Object.assign(
      snapshot,
      touchSnapshot(snapshot, {
        etapa_actual: "finalizado",
        porcentaje_avance: 100,
        finalizado_en: new Date().toISOString(),
        mensaje: buildFinalMessage(snapshot),
      })
    );
    return snapshot;
  }

  async executeDateRangeJob(jobDefinition, runningSnapshot) {
    const snapshot = cloneSnapshot(runningSnapshot);
    Object.assign(
      snapshot,
      touchSnapshot(snapshot, {
        etapa_actual: "extrayendo",
        mensaje: `Extrayendo entregas SAP entre ${jobDefinition.fecha_desde} y ${jobDefinition.fecha_hasta}.`,
        porcentaje_avance: 10,
      })
    );
    await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

    const extraction = await extractByDateRange(
      jobDefinition.destination,
      jobDefinition.fecha_desde,
      jobDefinition.fecha_hasta
    );

    const requested = countUniqueDeliveries(extraction?.likp_rows || []);
    snapshot.resumen.solicitados = requested;

    if (requested === 0) {
      snapshot.estado = JOB_STATUS.COMPLETED;
      snapshot.resultados.push({
        vbeln: null,
        sap_numero_entrega: null,
        estado: "COMPLETED",
        accion: "sin_datos",
        detalle: `No se encontraron entregas entre ${jobDefinition.fecha_desde} y ${jobDefinition.fecha_hasta}.`,
        id_sap_entrega: null,
        id_cabecera_flete: null,
      });
      Object.assign(
        snapshot,
        touchSnapshot(snapshot, {
          etapa_actual: "finalizado",
          porcentaje_avance: 100,
          finalizado_en: new Date().toISOString(),
          mensaje: "Carga completada. SAP no devolvio entregas para el rango solicitado.",
        })
      );
      return snapshot;
    }

    Object.assign(
      snapshot,
      touchSnapshot(snapshot, {
        etapa_actual: "persistiendo",
        mensaje: `Persistiendo ${requested} entrega(s) encontradas en SAP.`,
        porcentaje_avance: 60,
      })
    );
    await updateJobSnapshot(jobDefinition.job_id, JOB_STATUS.RUNNING, snapshot);

    const persisted = await persistExtraction(jobDefinition, extraction);
    snapshot.resumen.procesados = requested;
    snapshot.resumen.insertados_raw = sumRawInserted(persisted);
    snapshot.resumen.actualizados_canonicos = sumCanonicalUpdated(persisted);
    accumulateBackendMetrics(snapshot.backend_metrics, persisted);
    snapshot.resultados.push({
      vbeln: null,
      sap_numero_entrega: null,
      estado: "COMPLETED",
      accion: "sincronizado",
      detalle: `Rango ${jobDefinition.fecha_desde}..${jobDefinition.fecha_hasta}: ${requested} entrega(s) procesadas.`,
      id_sap_entrega: null,
      id_cabecera_flete: null,
    });

    snapshot.estado = deriveFinalStatus(snapshot);
    Object.assign(
      snapshot,
      touchSnapshot(snapshot, {
        etapa_actual: "finalizado",
        porcentaje_avance: 100,
        finalizado_en: new Date().toISOString(),
        mensaje: buildFinalMessage(snapshot),
      })
    );
    return snapshot;
  }

  getQueueSnapshot(jobId) {
    if (this.activeJobId === jobId) {
      return {
        is_active: true,
        queue_position: 0,
      };
    }

    const pendingIndex = this.pendingJobs.findIndex((job) => job.job_id === jobId);
    if (pendingIndex >= 0) {
      return {
        is_active: false,
        queue_position: pendingIndex + 1,
      };
    }

    return null;
  }

  async getJob(jobId) {
    await this.initialize();
    return getJobRecord(jobId, this.getQueueSnapshot(jobId));
  }

  async getLatestJob(userId = null) {
    await this.initialize();
    const latest = await getLatestJobRecord(null, userId);
    if (!latest) {
      return null;
    }

    return getJobRecord(latest.job_id, this.getQueueSnapshot(latest.job_id));
  }

  async getRecentJobs(limit = 20, userId = null) {
    await this.initialize();
    return getRecentJobRecords(limit, userId);
  }
}

const cflSapLoadService = new CflSapLoadService();

module.exports = {
  cflSapLoadService,
};
