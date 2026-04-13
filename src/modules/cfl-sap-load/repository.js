const crypto = require("crypto");
const { getPool, sql } = require("../../db");
const { CFL_SAP_LOAD_PROCESS, JOB_STATUS, JOB_TYPE } = require("./constants");
const {
  DEFAULT_DATE,
  padOrTrim,
  trunc,
  parseSapDate,
  parseSapTime,
  parseSapDecimal,
  formatDateToIso,
  buildSourceName,
  buildSourceSystem,
  parseJsonObject,
  parseIsoDate,
} = require("./utils");

function sha256Buffer(input) {
  return crypto.createHash("sha256").update(input || "", "utf8").digest();
}

function normalizeRowValue(row, field) {
  const direct = row?.[field];
  if (direct !== undefined && direct !== null) {
    return String(direct).trim();
  }

  const lowerKey = String(field || "").toLowerCase();
  const lowerValue = row?.[lowerKey];
  return String(lowerValue || "").trim();
}

function parseIsoDateSafe(value) {
  return value ? parseIsoDate(String(value)) : null;
}

function transformLikpRows(rows, sourceSystem, textsByLfart, executionId, extractedAtUtc, createdAtUtc) {
  return rows.map((row) => {
    const vbeln = trunc(normalizeRowValue(row, "VBELN"), 20);
    const xblnr = normalizeRowValue(row, "XBLNR");
    const vstel = normalizeRowValue(row, "VSTEL");
    const kunnr = normalizeRowValue(row, "KUNNR");
    const vkorg = normalizeRowValue(row, "VKORG");
    const ernam = normalizeRowValue(row, "ERNAM");
    const erdat = parseSapDate(normalizeRowValue(row, "ERDAT"), DEFAULT_DATE);
    const lfartRaw = normalizeRowValue(row, "LFART");
    const lfart = padOrTrim(lfartRaw, 4);
    const tipoEntregaTxt = trunc(textsByLfart.get(lfartRaw) || "", 20);
    const fechaCarga = parseSapDate(normalizeRowValue(row, "YWT_CDFIELD11"), DEFAULT_DATE);
    const horaCarga = parseSapTime(normalizeRowValue(row, "YWT_CDFIELD12"));
    const guiaRemision = normalizeRowValue(row, "YWT_XBLNR");
    const nombreChofer = normalizeRowValue(row, "YWT_DDFIELD21");
    const idFiscalChofer = normalizeRowValue(row, "YWT_DDFIELD23");
    const empresaTransporte = normalizeRowValue(row, "YWT_CDFIELD24");
    const patente = normalizeRowValue(row, "YWT_DDFIELD27");
    const carro = normalizeRowValue(row, "YWT_DDFIELD28");
    const fechaSalida = parseSapDate(normalizeRowValue(row, "YWT_DDFIELD29"), DEFAULT_DATE);
    const horaSalida = parseSapTime(normalizeRowValue(row, "YWT_DDFIELD30"));
    const codigoTipoFlete = normalizeRowValue(row, "YWT_CDFIELD92");
    const pesoTotal = parseSapDecimal(normalizeRowValue(row, "BTGEW"), 0);
    const pesoNeto = parseSapDecimal(normalizeRowValue(row, "NTGEW"), 0);
    const fechaEntregaReal = parseSapDate(normalizeRowValue(row, "WADAT_IST"), DEFAULT_DATE);

    const hashInput = [
      sourceSystem,
      vbeln,
      xblnr,
      vstel,
      kunnr,
      vkorg,
      ernam,
      formatDateToIso(erdat),
      lfart,
      tipoEntregaTxt,
      formatDateToIso(fechaCarga),
      horaCarga,
      guiaRemision,
      nombreChofer,
      idFiscalChofer,
      empresaTransporte,
      patente,
      carro,
      formatDateToIso(fechaSalida),
      horaSalida,
      codigoTipoFlete,
      "",
      "",
      String(pesoTotal),
      String(pesoNeto),
      formatDateToIso(fechaEntregaReal),
    ].join("|");

    return {
      idEjecucion: executionId,
      fechaExtraccion: extractedAtUtc,
      sistemaFuente: sourceSystem,
      hashFila: sha256Buffer(hashInput),
      estadoFila: "ACTIVE",
      fechaCreacion: createdAtUtc,
      sapNumeroEntrega: vbeln,
      sapReferencia: padOrTrim(xblnr, 25),
      sapPuestoExpedicion: padOrTrim(vstel, 4),
      sapDestinatario: trunc(kunnr, 20) || null,
      sapOrganizacionVentas: padOrTrim(vkorg, 4),
      sapCreadoPor: padOrTrim(ernam, 12),
      sapFechaCreacion: erdat,
      sapClaseEntrega: lfart,
      sapTipoEntrega: tipoEntregaTxt,
      sapFechaCarga: fechaCarga,
      sapHoraCarga: horaCarga,
      sapGuiaRemision: padOrTrim(guiaRemision, 25),
      sapNombreChofer: trunc(nombreChofer, 40),
      sapIdFiscalChofer: trunc(idFiscalChofer, 20),
      sapEmpresaTransporte: padOrTrim(empresaTransporte, 3),
      sapPatente: trunc(patente, 20),
      sapCarro: trunc(carro, 20),
      sapFechaSalida: fechaSalida,
      sapHoraSalida: horaSalida,
      sapCodigoTipoFlete: padOrTrim(codigoTipoFlete, 4),
      sapCentroCosto: null,
      sapCuentaMayor: null,
      sapPesoTotal: pesoTotal,
      sapPesoNeto: pesoNeto,
      sapFechaEntregaReal: fechaEntregaReal,
    };
  });
}

function transformLipsRows(rows, sourceSystem, executionId, extractedAtUtc, createdAtUtc) {
  return rows.map((row) => {
    const vbeln = trunc(normalizeRowValue(row, "VBELN"), 20);
    const posnr = padOrTrim(normalizeRowValue(row, "POSNR"), 6);
    const uecha = normalizeRowValue(row, "UECHA");
    const uepos = normalizeRowValue(row, "UEPOS");
    const posSuperior = uecha || uepos || "";
    const material = trunc(normalizeRowValue(row, "MATNR"), 40);
    const lote = trunc(normalizeRowValue(row, "CHARG"), 20);
    const cantidad = parseSapDecimal(normalizeRowValue(row, "LFIMG"), 0);
    const unidadPeso = padOrTrim(normalizeRowValue(row, "GEWEI"), 3);
    const denominacionMaterial = trunc(normalizeRowValue(row, "ARKTX"), 40);
    const centro = padOrTrim(normalizeRowValue(row, "WERKS"), 4);
    const almacen = padOrTrim(normalizeRowValue(row, "LGORT"), 4);

    const hashInput = [
      sourceSystem,
      vbeln,
      posnr,
      posSuperior,
      material,
      lote,
      String(cantidad),
      unidadPeso,
      denominacionMaterial,
      centro,
      almacen,
    ].join("|");

    return {
      idEjecucion: executionId,
      fechaExtraccion: extractedAtUtc,
      sistemaFuente: sourceSystem,
      hashFila: sha256Buffer(hashInput),
      estadoFila: "ACTIVE",
      fechaCreacion: createdAtUtc,
      sapNumeroEntrega: vbeln,
      sapPosicion: posnr,
      sapPosicionSuperior: posSuperior ? padOrTrim(posSuperior, 6) : null,
      sapLote: lote || null,
      sapMaterial: material,
      sapCantidadEntregada: cantidad,
      sapUnidadPeso: unidadPeso,
      sapDenominacionMaterial: denominacionMaterial,
      sapCentro: centro,
      sapAlmacen: almacen,
    };
  });
}

function createLikpStageTable() {
  const table = new sql.Table("[cfl].[StgLikp]");
  table.create = false;
  table.columns.add("IdEjecucion", sql.UniqueIdentifier, { nullable: false });
  table.columns.add("FechaExtraccion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SistemaFuente", sql.NVarChar(50), { nullable: false });
  table.columns.add("HashFila", sql.VarBinary(32), { nullable: false });
  table.columns.add("EstadoFila", sql.NVarChar(20), { nullable: false });
  table.columns.add("FechaCreacion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SapNumeroEntrega", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapReferencia", sql.Char(25), { nullable: false });
  table.columns.add("SapPuestoExpedicion", sql.Char(4), { nullable: false });
  table.columns.add("SapDestinatario", sql.NVarChar(20), { nullable: true });
  table.columns.add("SapOrganizacionVentas", sql.Char(4), { nullable: false });
  table.columns.add("SapCreadoPor", sql.Char(12), { nullable: false });
  table.columns.add("SapFechaCreacion", sql.Date, { nullable: false });
  table.columns.add("SapClaseEntrega", sql.Char(4), { nullable: false });
  table.columns.add("SapTipoEntrega", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapFechaCarga", sql.Date, { nullable: false });
  table.columns.add("SapHoraCarga", sql.VarChar(8), { nullable: false });
  table.columns.add("SapGuiaRemision", sql.Char(25), { nullable: false });
  table.columns.add("SapNombreChofer", sql.NVarChar(40), { nullable: false });
  table.columns.add("SapIdFiscalChofer", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapEmpresaTransporte", sql.Char(3), { nullable: false });
  table.columns.add("SapPatente", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapCarro", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapFechaSalida", sql.Date, { nullable: false });
  table.columns.add("SapHoraSalida", sql.VarChar(8), { nullable: false });
  table.columns.add("SapCodigoTipoFlete", sql.Char(4), { nullable: false });
  table.columns.add("SapCentroCosto", sql.Char(10), { nullable: true });
  table.columns.add("SapCuentaMayor", sql.Char(10), { nullable: true });
  table.columns.add("SapPesoTotal", sql.Decimal(15, 3), { nullable: false });
  table.columns.add("SapPesoNeto", sql.Decimal(15, 3), { nullable: false });
  table.columns.add("SapFechaEntregaReal", sql.Date, { nullable: false });
  return table;
}

function createLipsStageTable() {
  const table = new sql.Table("[cfl].[StgLips]");
  table.create = false;
  table.columns.add("IdEjecucion", sql.UniqueIdentifier, { nullable: false });
  table.columns.add("FechaExtraccion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SistemaFuente", sql.NVarChar(50), { nullable: false });
  table.columns.add("HashFila", sql.VarBinary(32), { nullable: false });
  table.columns.add("EstadoFila", sql.NVarChar(20), { nullable: false });
  table.columns.add("FechaCreacion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SapNumeroEntrega", sql.NVarChar(20), { nullable: false });
  table.columns.add("SapPosicion", sql.Char(6), { nullable: false });
  table.columns.add("SapPosicionSuperior", sql.Char(6), { nullable: true });
  table.columns.add("SapLote", sql.NVarChar(20), { nullable: true });
  table.columns.add("SapMaterial", sql.NVarChar(40), { nullable: false });
  table.columns.add("SapCantidadEntregada", sql.Decimal(13, 3), { nullable: false });
  table.columns.add("SapUnidadPeso", sql.Char(3), { nullable: false });
  table.columns.add("SapDenominacionMaterial", sql.NVarChar(40), { nullable: false });
  table.columns.add("SapCentro", sql.Char(4), { nullable: false });
  table.columns.add("SapAlmacen", sql.Char(4), { nullable: false });
  return table;
}

async function failStaleJobs() {
  const pool = await getPool();
  await pool
    .request()
    .input("tipoProceso", sql.NVarChar(50), CFL_SAP_LOAD_PROCESS)
    .input("estadoQueued", sql.NVarChar(20), JOB_STATUS.QUEUED)
    .input("estadoRunning", sql.NVarChar(20), JOB_STATUS.RUNNING)
    .input("estadoFailed", sql.NVarChar(20), JOB_STATUS.FAILED)
    .input("finishedAt", sql.DateTime2(0), new Date())
    .input("message", sql.NVarChar(4000), "Job interrumpido por reinicio o caida de cfl-back")
    .query(`
      UPDATE [cfl].[EtlEjecucion]
      SET
        [Estado] = @estadoFailed,
        [FechaFinProceso] = COALESCE([FechaFinProceso], @finishedAt),
        [MensajeError] = COALESCE(NULLIF([MensajeError], ''), @message)
      WHERE [TipoProceso] = @tipoProceso
        AND [Estado] IN (@estadoQueued, @estadoRunning);
    `);
}

async function insertQueuedJob(jobDefinition) {
  const pool = await getPool();
  const now = new Date();
  await pool
    .request()
    .input("idEjecucion", sql.UniqueIdentifier, jobDefinition.job_id)
    .input("sistemaFuente", sql.NVarChar(50), buildSourceSystem(jobDefinition.destination))
    .input("nombreFuente", sql.NVarChar(100), buildSourceName(jobDefinition))
    .input("fechaExtraccion", sql.DateTime2(0), now)
    .input("marcaAguaDesde", sql.DateTime2(0), parseIsoDateSafe(jobDefinition.fecha_desde))
    .input("marcaAguaHasta", sql.DateTime2(0), parseIsoDateSafe(jobDefinition.fecha_hasta))
    .input("estado", sql.NVarChar(20), JOB_STATUS.QUEUED)
    .input("fechaCreacion", sql.DateTime2(0), now)
    .input("tipoProceso", sql.NVarChar(50), CFL_SAP_LOAD_PROCESS)
    .input("parametrosJson", sql.NVarChar(sql.MAX), jobDefinition.parametros_json)
    .input("resumenJson", sql.NVarChar(sql.MAX), jobDefinition.resumen_json || null)
    .query(`
      INSERT INTO [cfl].[EtlEjecucion]
      (
        [IdEjecucion],
        [SistemaFuente],
        [NombreFuente],
        [FechaExtraccion],
        [MarcaAguaDesde],
        [MarcaAguaHasta],
        [Estado],
        [FilasExtraidas],
        [FilasInsertadas],
        [FilasActualizadas],
        [FilasSinCambio],
        [MensajeError],
        [FechaCreacion],
        [TipoProceso],
        [ParametrosJson],
        [ResumenJson],
        [FechaInicioProceso],
        [FechaFinProceso]
      )
      VALUES
      (
        @idEjecucion,
        @sistemaFuente,
        @nombreFuente,
        @fechaExtraccion,
        @marcaAguaDesde,
        @marcaAguaHasta,
        @estado,
        0,
        0,
        0,
        0,
        NULL,
        @fechaCreacion,
        @tipoProceso,
        @parametrosJson,
        @resumenJson,
        NULL,
        NULL
      );
    `);
}

async function markJobRunning(jobId, snapshot) {
  const pool = await getPool();
  const now = new Date();
  await pool
    .request()
    .input("idEjecucion", sql.UniqueIdentifier, jobId)
    .input("estado", sql.NVarChar(20), JOB_STATUS.RUNNING)
    .input("startedAt", sql.DateTime2(0), now)
    .input("resumenJson", sql.NVarChar(sql.MAX), snapshot ? JSON.stringify(snapshot) : null)
    .query(`
      UPDATE [cfl].[EtlEjecucion]
      SET
        [Estado] = @estado,
        [FechaInicioProceso] = @startedAt,
        [FechaExtraccion] = @startedAt,
        [MensajeError] = NULL,
        [ResumenJson] = COALESCE(@resumenJson, [ResumenJson])
      WHERE [IdEjecucion] = @idEjecucion;
    `);
}

async function updateJobSnapshot(jobId, status, snapshot, errorMessage = null) {
  const pool = await getPool();
  await pool
    .request()
    .input("idEjecucion", sql.UniqueIdentifier, jobId)
    .input("estado", sql.NVarChar(20), status || null)
    .input("mensajeError", sql.NVarChar(4000), errorMessage)
    .input("resumenJson", sql.NVarChar(sql.MAX), snapshot ? JSON.stringify(snapshot) : null)
    .query(`
      UPDATE [cfl].[EtlEjecucion]
      SET
        [Estado] = COALESCE(@estado, [Estado]),
        [MensajeError] = CASE WHEN @mensajeError IS NULL THEN [MensajeError] ELSE @mensajeError END,
        [ResumenJson] = COALESCE(@resumenJson, [ResumenJson])
      WHERE [IdEjecucion] = @idEjecucion;
    `);
}

async function markJobFinished(jobId, status, summary, errorMessage) {
  const pool = await getPool();
  const finishedAt = new Date();
  const metrics = summary?.backend_metrics || {};
  await pool
    .request()
    .input("idEjecucion", sql.UniqueIdentifier, jobId)
    .input("estado", sql.NVarChar(20), status)
    .input("filasExtraidas", sql.Int, Number(metrics.filas_extraidas || 0))
    .input("filasInsertadas", sql.Int, Number(metrics.filas_insertadas || 0))
    .input("filasActualizadas", sql.Int, Number(metrics.filas_actualizadas || 0))
    .input("filasSinCambio", sql.Int, Number(metrics.filas_sin_cambio || 0))
    .input("mensajeError", sql.NVarChar(4000), errorMessage || null)
    .input("resumenJson", sql.NVarChar(sql.MAX), summary ? JSON.stringify(summary) : null)
    .input("finishedAt", sql.DateTime2(0), finishedAt)
    .query(`
      UPDATE [cfl].[EtlEjecucion]
      SET
        [Estado] = @estado,
        [FilasExtraidas] = @filasExtraidas,
        [FilasInsertadas] = @filasInsertadas,
        [FilasActualizadas] = @filasActualizadas,
        [FilasSinCambio] = @filasSinCambio,
        [MensajeError] = @mensajeError,
        [ResumenJson] = @resumenJson,
        [FechaFinProceso] = @finishedAt
      WHERE [IdEjecucion] = @idEjecucion;
    `);
}

function toUiJobStatus(status, summary) {
  const normalized = String(status || "").trim().toUpperCase();
  if (normalized === "SUCCEEDED" || normalized === "SUCCESS") {
    return "COMPLETED";
  }
  if (
    normalized === "COMPLETED" &&
    (Number(summary?.resumen?.errores || 0) > 0 || Number(summary?.resumen?.omitidos || 0) > 0)
  ) {
    return "PARTIAL_SUCCESS";
  }
  if (normalized === "PARTIAL_SUCCESS") {
    return "PARTIAL_SUCCESS";
  }
  return normalized || "UNKNOWN";
}

function toUiJobType(params) {
  if (params?.job_type === JOB_TYPE.XBLNR) return "xblnr";
  return params?.job_type === JOB_TYPE.VBELN ? "vbeln" : "rango_fechas";
}

function normalizeJobRecord(record, queueSnapshot) {
  if (!record) {
    return null;
  }

  const params = parseJsonObject(record.ParametrosJson || record.parametros_json) || {};
  const summary = parseJsonObject(record.ResumenJson || record.resumen_json) || {};
  const uiStatus = toUiJobStatus(record.Estado || record.estado, summary);
  const mensajeError = record.MensajeError || record.mensaje_error || null;
  const errores = Array.isArray(summary.errores) ? summary.errores : [];
  const resultados = Array.isArray(summary.resultados) ? summary.resultados : [];
  const resumen = summary.resumen || {};
  const defaultStage =
    uiStatus === JOB_STATUS.QUEUED
      ? "En cola"
      : uiStatus === JOB_STATUS.RUNNING
        ? "Procesando"
        : uiStatus === JOB_STATUS.COMPLETED
          ? "Completado"
          : uiStatus === JOB_STATUS.PARTIAL_SUCCESS
            ? "Finalizado con observaciones"
            : uiStatus === JOB_STATUS.FAILED
              ? "Fallido"
              : null;
  const defaultMessage =
    uiStatus === JOB_STATUS.QUEUED
      ? "Job en cola para ejecutar carga SAP on-demand"
      : uiStatus === JOB_STATUS.RUNNING
        ? "Job ejecutandose en backend"
        : null;
  const actualizadoEn =
    summary.actualizado_en ||
    record.FechaFinProceso ||
    record.fecha_fin_proceso ||
    record.FechaInicioProceso ||
    record.fecha_inicio_proceso ||
    record.FechaCreacion ||
    record.fecha_creacion ||
    null;

  return {
    job_id: record.IdEjecucion || record.id_ejecucion,
    tipo_solicitud: toUiJobType(params),
    estado: uiStatus,
    etapa_actual: summary.etapa_actual || defaultStage,
    mensaje: summary.mensaje || mensajeError || defaultMessage,
    creado_en: record.FechaCreacion || record.fecha_creacion || null,
    actualizado_en: actualizadoEn,
    iniciado_en: record.FechaInicioProceso || record.fecha_inicio_proceso || null,
    finalizado_en: record.FechaFinProceso || record.fecha_fin_proceso || null,
    porcentaje_avance: Number.isFinite(Number(summary.porcentaje_avance))
      ? Number(summary.porcentaje_avance)
      : queueSnapshot?.is_active
        ? 1
        : 0,
    source_system:
      record.SistemaFuente ||
      record.sistema_fuente ||
      summary.source_system ||
      params.source_system ||
      params.destination ||
      null,
    poll_interval_ms: Number(summary.poll_interval_ms || 2500),
    parametros: {
      vbeln: Array.isArray(params.vbeln) ? params.vbeln : null,
      fecha_desde: params.fecha_desde || null,
      fecha_hasta: params.fecha_hasta || null,
    },
    resumen: {
      solicitados: Number(resumen.solicitados || 0),
      procesados: Number(resumen.procesados || 0),
      insertados_raw: Number(resumen.insertados_raw || 0),
      actualizados_canonicos: Number(resumen.actualizados_canonicos || 0),
      omitidos: Number(resumen.omitidos || 0),
      errores: Number(resumen.errores || 0),
    },
    queue_position: queueSnapshot?.queue_position ?? null,
    resultados,
    errores:
      errores.length > 0
        ? errores
        : mensajeError
          ? [{ codigo: "JOB_ERROR", mensaje: mensajeError, detalle: mensajeError }]
          : [],
  };
}

async function getJobRecord(jobId, queueSnapshot = null) {
  const pool = await getPool();
  const result = await pool
    .request()
    .input("idEjecucion", sql.UniqueIdentifier, jobId)
    .input("tipoProceso", sql.NVarChar(50), CFL_SAP_LOAD_PROCESS)
    .query(`
      SELECT TOP 1 *
      FROM [cfl].[EtlEjecucion]
      WHERE [TipoProceso] = @tipoProceso
        AND [IdEjecucion] = @idEjecucion;
    `);

  return normalizeJobRecord(result.recordset[0] || null, queueSnapshot);
}

async function getLatestJobRecord(queueSnapshot = null, userId = null) {
  const pool = await getPool();
  const req = pool
    .request()
    .input("tipoProceso", sql.NVarChar(50), CFL_SAP_LOAD_PROCESS);

  let filterSql = "";
  if (userId) {
    req.input("userId", sql.NVarChar(20), String(userId));
    filterSql = "AND JSON_VALUE([ParametrosJson], '$.requested_by.id_usuario') = @userId";
  }

  const result = await req.query(`
    SELECT TOP 1 *
    FROM [cfl].[EtlEjecucion]
    WHERE [TipoProceso] = @tipoProceso
      ${filterSql}
    ORDER BY [FechaCreacion] DESC, [IdEtlEjecucion] DESC;
  `);

  return normalizeJobRecord(result.recordset[0] || null, queueSnapshot);
}

async function getRecentJobRecords(limit = 20, userId = null) {
  const pool = await getPool();
  const req = pool
    .request()
    .input("tipoProceso", sql.NVarChar(50), CFL_SAP_LOAD_PROCESS)
    .input("limit", sql.Int, limit);

  let filterSql = "";
  if (userId) {
    req.input("userId", sql.NVarChar(20), String(userId));
    filterSql = "AND JSON_VALUE([ParametrosJson], '$.requested_by.id_usuario') = @userId";
  }

  const result = await req.query(`
    SELECT TOP (@limit) *
    FROM [cfl].[EtlEjecucion]
    WHERE [TipoProceso] = @tipoProceso
      ${filterSql}
    ORDER BY [FechaCreacion] DESC, [IdEtlEjecucion] DESC;
  `);

  return result.recordset.map((r) => normalizeJobRecord(r, null));
}

async function createStageTables(transaction) {
  await new sql.Request(transaction).query(`
    CREATE TABLE #stg_likp (
      IdEjecucion UNIQUEIDENTIFIER NOT NULL,
      FechaExtraccion DATETIME2(0) NOT NULL,
      SistemaFuente NVARCHAR(50) NOT NULL,
      HashFila BINARY(32) NOT NULL,
      EstadoFila NVARCHAR(20) NOT NULL,
      FechaCreacion DATETIME2(0) NOT NULL,
      SapNumeroEntrega NVARCHAR(20) NOT NULL,
      SapReferencia CHAR(25) NOT NULL,
      SapPuestoExpedicion CHAR(4) NOT NULL,
      SapDestinatario NVARCHAR(20) NULL,
      SapOrganizacionVentas CHAR(4) NOT NULL,
      SapCreadoPor CHAR(12) NOT NULL,
      SapFechaCreacion DATE NOT NULL,
      SapClaseEntrega CHAR(4) NOT NULL,
      SapTipoEntrega NVARCHAR(20) NOT NULL,
      SapFechaCarga DATE NOT NULL,
      SapHoraCarga VARCHAR(8) NOT NULL,
      SapGuiaRemision CHAR(25) NOT NULL,
      SapNombreChofer NVARCHAR(40) NOT NULL,
      SapIdFiscalChofer NVARCHAR(20) NOT NULL,
      SapEmpresaTransporte CHAR(3) NOT NULL,
      SapPatente NVARCHAR(20) NOT NULL,
      SapCarro NVARCHAR(20) NOT NULL,
      SapFechaSalida DATE NOT NULL,
      SapHoraSalida VARCHAR(8) NOT NULL,
      SapCodigoTipoFlete CHAR(4) NOT NULL,
      SapCentroCosto CHAR(10) NULL,
      SapCuentaMayor CHAR(10) NULL,
      SapPesoTotal DECIMAL(15, 3) NOT NULL,
      SapPesoNeto DECIMAL(15, 3) NOT NULL,
      SapFechaEntregaReal DATE NOT NULL
    );

    CREATE TABLE #stg_lips (
      IdEjecucion UNIQUEIDENTIFIER NOT NULL,
      FechaExtraccion DATETIME2(0) NOT NULL,
      SistemaFuente NVARCHAR(50) NOT NULL,
      HashFila BINARY(32) NOT NULL,
      EstadoFila NVARCHAR(20) NOT NULL,
      FechaCreacion DATETIME2(0) NOT NULL,
      SapNumeroEntrega NVARCHAR(20) NOT NULL,
      SapPosicion CHAR(6) NOT NULL,
      SapPosicionSuperior CHAR(6) NULL,
      SapLote NVARCHAR(20) NULL,
      SapMaterial NVARCHAR(40) NOT NULL,
      SapCantidadEntregada DECIMAL(13, 3) NOT NULL,
      SapUnidadPeso CHAR(3) NOT NULL,
      SapDenominacionMaterial NVARCHAR(40) NOT NULL,
      SapCentro CHAR(4) NOT NULL,
      SapAlmacen CHAR(4) NOT NULL
    );
  `);
}

async function bulkInsertStageRows(transaction, stageLikpRows, stageLipsRows) {
  const likpTable = createLikpStageTable();
  for (const row of stageLikpRows) {
    likpTable.rows.add(
      row.idEjecucion,
      row.fechaExtraccion,
      row.sistemaFuente,
      row.hashFila,
      row.estadoFila,
      row.fechaCreacion,
      row.sapNumeroEntrega,
      row.sapReferencia,
      row.sapPuestoExpedicion,
      row.sapDestinatario,
      row.sapOrganizacionVentas,
      row.sapCreadoPor,
      row.sapFechaCreacion,
      row.sapClaseEntrega,
      row.sapTipoEntrega,
      row.sapFechaCarga,
      row.sapHoraCarga,
      row.sapGuiaRemision,
      row.sapNombreChofer,
      row.sapIdFiscalChofer,
      row.sapEmpresaTransporte,
      row.sapPatente,
      row.sapCarro,
      row.sapFechaSalida,
      row.sapHoraSalida,
      row.sapCodigoTipoFlete,
      row.sapCentroCosto,
      row.sapCuentaMayor,
      row.sapPesoTotal,
      row.sapPesoNeto,
      row.sapFechaEntregaReal
    );
  }
  if (likpTable.rows.length > 0) {
    await new sql.Request(transaction).bulk(likpTable);
  }

  if (stageLipsRows.length > 0) {
    const lipsTable = createLipsStageTable();
    for (const row of stageLipsRows) {
      lipsTable.rows.add(
        row.idEjecucion,
        row.fechaExtraccion,
        row.sistemaFuente,
        row.hashFila,
        row.estadoFila,
        row.fechaCreacion,
        row.sapNumeroEntrega,
        row.sapPosicion,
        row.sapPosicionSuperior,
        row.sapLote,
        row.sapMaterial,
        row.sapCantidadEntregada,
        row.sapUnidadPeso,
        row.sapDenominacionMaterial,
        row.sapCentro,
        row.sapAlmacen
      );
    }
    await new sql.Request(transaction).bulk(lipsTable);
  }
}

async function insertLikpDeduped(transaction, executionId) {
  const result = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .query(`
    ;WITH s1 AS
    (
      SELECT s.*,
        rn = ROW_NUMBER() OVER (
          PARTITION BY s.[SistemaFuente], s.[SapNumeroEntrega], s.[HashFila]
          ORDER BY (SELECT 1)
        )
      FROM [cfl].[StgLikp] s
      WHERE s.[IdEjecucion] = @execution_id
    )
    INSERT INTO [cfl].[SapLikpRaw]
    (
      IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
      SapNumeroEntrega, SapReferencia, SapPuestoExpedicion, SapDestinatario, SapOrganizacionVentas,
      SapCreadoPor, SapFechaCreacion, SapClaseEntrega, SapTipoEntrega,
      SapFechaCarga, SapHoraCarga, SapGuiaRemision, SapNombreChofer, SapIdFiscalChofer,
      SapEmpresaTransporte, SapPatente, SapCarro, SapFechaSalida, SapHoraSalida,
      SapCodigoTipoFlete, SapCentroCosto, SapCuentaMayor, SapPesoTotal, SapPesoNeto,
      SapFechaEntregaReal
    )
    SELECT
      s.IdEjecucion, s.FechaExtraccion, s.SistemaFuente, s.HashFila, s.EstadoFila, s.FechaCreacion,
      s.SapNumeroEntrega, s.SapReferencia, s.SapPuestoExpedicion, s.SapDestinatario, s.SapOrganizacionVentas,
      s.SapCreadoPor, s.SapFechaCreacion, s.SapClaseEntrega, s.SapTipoEntrega,
      s.SapFechaCarga, CAST(s.SapHoraCarga AS TIME), s.SapGuiaRemision, s.SapNombreChofer, s.SapIdFiscalChofer,
      s.SapEmpresaTransporte, s.SapPatente, s.SapCarro, s.SapFechaSalida, CAST(s.SapHoraSalida AS TIME),
      s.SapCodigoTipoFlete, s.SapCentroCosto, s.SapCuentaMayor, s.SapPesoTotal, s.SapPesoNeto,
      s.SapFechaEntregaReal
    FROM s1 s
    WHERE s.rn = 1
      AND NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapLikpRaw] t
        WHERE t.SistemaFuente = s.SistemaFuente
          AND t.SapNumeroEntrega = s.SapNumeroEntrega
          AND t.HashFila = s.HashFila
      );

    SELECT inserted = @@ROWCOUNT;
  `);

  return Number(result.recordset[0]?.inserted || 0);
}

async function insertLipsDeduped(transaction, executionId) {
  const result = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .query(`
    ;WITH s1 AS
    (
      SELECT s.*,
        rn = ROW_NUMBER() OVER (
          PARTITION BY s.[SistemaFuente], s.[SapNumeroEntrega], s.[SapPosicion], s.[HashFila]
          ORDER BY (SELECT 1)
        )
      FROM [cfl].[StgLips] s
      WHERE s.[IdEjecucion] = @execution_id
    )
    INSERT INTO [cfl].[SapLipsRaw]
    (
      IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
      SapNumeroEntrega, SapPosicion, SapPosicionSuperior, SapLote, SapMaterial,
      SapCantidadEntregada, SapUnidadPeso, SapDenominacionMaterial, SapCentro, SapAlmacen
    )
    SELECT
      s.IdEjecucion, s.FechaExtraccion, s.SistemaFuente, s.HashFila, s.EstadoFila, s.FechaCreacion,
      s.SapNumeroEntrega, s.SapPosicion, s.SapPosicionSuperior, s.SapLote, s.SapMaterial,
      s.SapCantidadEntregada, s.SapUnidadPeso, s.SapDenominacionMaterial, s.SapCentro, s.SapAlmacen
    FROM s1 s
    WHERE s.rn = 1
      AND NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapLipsRaw] t
        WHERE t.SistemaFuente = s.SistemaFuente
          AND t.SapNumeroEntrega = s.SapNumeroEntrega
          AND t.SapPosicion = s.SapPosicion
          AND t.HashFila = s.HashFila
      );

    SELECT inserted = @@ROWCOUNT;
  `);

  return Number(result.recordset[0]?.inserted || 0);
}

async function normalizeRawState(transaction, executionId) {
  await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .query(`
      IF EXISTS (SELECT 1 FROM [cfl].[StgLikp] WHERE [IdEjecucion] = @execution_id)
      BEGIN
        UPDATE r
        SET r.[EstadoFila] = 'INACTIVE'
        FROM [cfl].[SapLikpRaw] r
        WHERE r.[EstadoFila] = 'ACTIVE'
          AND EXISTS (
            SELECT 1
            FROM [cfl].[StgLikp] s
            WHERE s.[IdEjecucion] = @execution_id
              AND s.[SistemaFuente] = r.[SistemaFuente]
              AND s.[SapNumeroEntrega] = r.[SapNumeroEntrega]
              AND s.[HashFila] <> r.[HashFila]
              AND EXISTS (
                SELECT 1
                FROM [cfl].[SapLikpRaw] prev
                WHERE prev.[SistemaFuente] = s.[SistemaFuente]
                  AND prev.[SapNumeroEntrega] = s.[SapNumeroEntrega]
                  AND prev.[HashFila] = s.[HashFila]
                  AND prev.[EstadoFila] = 'INACTIVE'
              )
          );

        UPDATE r
        SET r.[EstadoFila] = 'ACTIVE'
        FROM [cfl].[SapLikpRaw] r
        INNER JOIN [cfl].[StgLikp] s
          ON s.[IdEjecucion] = @execution_id
         AND s.[SistemaFuente] = r.[SistemaFuente]
         AND s.[SapNumeroEntrega] = r.[SapNumeroEntrega]
         AND s.[HashFila] = r.[HashFila]
        WHERE r.[EstadoFila] = 'INACTIVE';
      END;

      UPDATE r
      SET r.[EstadoFila] = 'INACTIVE'
      FROM [cfl].[SapLikpRaw] r
      INNER JOIN (
        SELECT DISTINCT [SistemaFuente], [SapNumeroEntrega]
        FROM [cfl].[SapLikpRaw]
        WHERE [IdEjecucion] = @execution_id
      ) changed
        ON changed.[SistemaFuente] = r.[SistemaFuente]
       AND changed.[SapNumeroEntrega] = r.[SapNumeroEntrega]
      WHERE r.[IdEjecucion] <> @execution_id
        AND r.[EstadoFila] = 'ACTIVE';

      IF EXISTS (SELECT 1 FROM [cfl].[StgLips] WHERE [IdEjecucion] = @execution_id)
      BEGIN
        UPDATE r
        SET r.[EstadoFila] = 'INACTIVE'
        FROM [cfl].[SapLipsRaw] r
        WHERE r.[EstadoFila] = 'ACTIVE'
          AND EXISTS (
            SELECT 1
            FROM [cfl].[StgLips] s
            WHERE s.[IdEjecucion] = @execution_id
              AND s.[SistemaFuente] = r.[SistemaFuente]
              AND s.[SapNumeroEntrega] = r.[SapNumeroEntrega]
              AND s.[SapPosicion] = r.[SapPosicion]
              AND s.[HashFila] <> r.[HashFila]
              AND EXISTS (
                SELECT 1
                FROM [cfl].[SapLipsRaw] prev
                WHERE prev.[SistemaFuente] = s.[SistemaFuente]
                  AND prev.[SapNumeroEntrega] = s.[SapNumeroEntrega]
                  AND prev.[SapPosicion] = s.[SapPosicion]
                  AND prev.[HashFila] = s.[HashFila]
                  AND prev.[EstadoFila] = 'INACTIVE'
              )
          );

        UPDATE r
        SET r.[EstadoFila] = 'ACTIVE'
        FROM [cfl].[SapLipsRaw] r
        INNER JOIN [cfl].[StgLips] s
          ON s.[IdEjecucion] = @execution_id
         AND s.[SistemaFuente] = r.[SistemaFuente]
         AND s.[SapNumeroEntrega] = r.[SapNumeroEntrega]
         AND s.[SapPosicion] = r.[SapPosicion]
         AND s.[HashFila] = r.[HashFila]
        WHERE r.[EstadoFila] = 'INACTIVE';
      END;

      UPDATE r
      SET r.[EstadoFila] = 'INACTIVE'
      FROM [cfl].[SapLipsRaw] r
      INNER JOIN (
        SELECT DISTINCT [SistemaFuente], [SapNumeroEntrega], [SapPosicion]
        FROM [cfl].[SapLipsRaw]
        WHERE [IdEjecucion] = @execution_id
      ) changed
        ON changed.[SistemaFuente] = r.[SistemaFuente]
       AND changed.[SapNumeroEntrega] = r.[SapNumeroEntrega]
       AND changed.[SapPosicion] = r.[SapPosicion]
      WHERE r.[IdEjecucion] <> @execution_id
        AND r.[EstadoFila] = 'ACTIVE';
    `);
}

async function buildCanonical(transaction, executionId, nowUtc, watermarkFrom, applyBlocking) {
  if (applyBlocking && watermarkFrom) {
    await new sql.Request(transaction)
      .input("execution_id", sql.UniqueIdentifier, executionId)
      .input("watermark_from", sql.Date, watermarkFrom)
      .input("now", sql.DateTime2(0), nowUtc)
      .query(`
        ;WITH d AS
        (
          SELECT
            s.SistemaFuente,
            s.SapNumeroEntrega,
            max_eff_date = MAX(
              CASE
                WHEN r.SapFechaEntregaReal > r.SapFechaCreacion THEN r.SapFechaEntregaReal
                ELSE r.SapFechaCreacion
              END
            )
          FROM [cfl].[SapLikpRaw] r
          INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
            ON s.SistemaFuente = r.SistemaFuente
           AND s.SapNumeroEntrega = r.SapNumeroEntrega
          GROUP BY s.SistemaFuente, s.SapNumeroEntrega
        )
        UPDATE e
        SET
          e.Bloqueado = 1,
          e.FechaBloqueado = COALESCE(e.FechaBloqueado, @now),
          e.FechaActualizacion = @now
        FROM [cfl].[SapEntrega] e
        INNER JOIN d
          ON d.SistemaFuente = e.SistemaFuente
         AND d.SapNumeroEntrega = e.SapNumeroEntrega
        WHERE e.Bloqueado = 0
          AND d.max_eff_date < @watermark_from;
      `);
  }

  const entregaInsertedResult = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      INSERT INTO [cfl].[SapEntrega]
      (
        [SapNumeroEntrega],
        [SistemaFuente],
        [FechaCreacion],
        [FechaActualizacion]
      )
      SELECT s.[SapNumeroEntrega], s.[SistemaFuente], @now, @now
      FROM (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
      WHERE NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapEntrega] e
        WHERE e.[SistemaFuente] = s.[SistemaFuente]
          AND e.[SapNumeroEntrega] = s.[SapNumeroEntrega]
      );
      SELECT affected = @@ROWCOUNT;
    `);

  const entregaHistInsertedResult = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      INSERT INTO [cfl].[SapEntregaHistorial]
      (
        [IdSapEntrega],
        [IdLikpRaw],
        [IdEjecucion],
        [FechaExtraccion],
        [FechaCreacion]
      )
      SELECT
        e.[IdSapEntrega],
        likp.[IdSapLikpRaw],
        likp.[IdEjecucion],
        likp.[FechaExtraccion],
        @now
      FROM [cfl].[SapLikpRaw] likp
      INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
        ON s.[SistemaFuente] = likp.[SistemaFuente]
       AND s.[SapNumeroEntrega] = likp.[SapNumeroEntrega]
      INNER JOIN [cfl].[SapEntrega] e
        ON e.[SistemaFuente] = likp.[SistemaFuente]
       AND e.[SapNumeroEntrega] = likp.[SapNumeroEntrega]
      WHERE NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapEntregaHistorial] h
        WHERE h.[IdLikpRaw] = likp.[IdSapLikpRaw]
      );
      SELECT affected = @@ROWCOUNT;
    `);

  const entregaUpdatedResult = await new sql.Request(transaction)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      UPDATE e
      SET e.[FechaActualizacion] = @now
      FROM [cfl].[SapEntrega] e
      WHERE EXISTS (
        SELECT 1
        FROM [cfl].[SapEntregaHistorial] h
        WHERE h.[IdSapEntrega] = e.[IdSapEntrega]
          AND h.[FechaCreacion] = @now
      );
      SELECT affected = @@ROWCOUNT;
    `);

  const posInsertedResult = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      ;WITH x AS
      (
        SELECT DISTINCT
          e.[IdSapEntrega],
          CAST(
            CASE
              WHEN NULLIF(LTRIM(RTRIM(l.[SapPosicionSuperior])), '') IS NOT NULL
                AND LTRIM(RTRIM(l.[SapPosicionSuperior])) <> '000000'
              THEN l.[SapPosicionSuperior]
              ELSE l.[SapPosicion]
            END
          AS CHAR(6)) AS [pos_logica]
        FROM [cfl].[SapLipsRaw] l
        INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
          ON s.[SistemaFuente] = l.[SistemaFuente]
         AND s.[SapNumeroEntrega] = l.[SapNumeroEntrega]
        INNER JOIN [cfl].[SapEntrega] e
          ON e.[SistemaFuente] = l.[SistemaFuente]
         AND e.[SapNumeroEntrega] = l.[SapNumeroEntrega]
      )
      INSERT INTO [cfl].[SapEntregaPosicion]
      (
        [IdSapEntrega],
        [SapPosicion],
        [FechaCreacion],
        [FechaActualizacion]
      )
      SELECT x.[IdSapEntrega], x.[pos_logica], @now, @now
      FROM x
      WHERE NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapEntregaPosicion] p
        WHERE p.[IdSapEntrega] = x.[IdSapEntrega]
          AND p.[SapPosicion] = x.[pos_logica]
      );
      SELECT affected = @@ROWCOUNT;
    `);

  const posHistInsertedResult = await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      ;WITH lips_x AS
      (
        SELECT
          l.[IdSapLipsRaw],
          l.[IdEjecucion],
          l.[FechaExtraccion],
          l.[SistemaFuente],
          l.[SapNumeroEntrega],
          CAST(
            CASE
              WHEN NULLIF(LTRIM(RTRIM(l.[SapPosicionSuperior])), '') IS NOT NULL
                AND LTRIM(RTRIM(l.[SapPosicionSuperior])) <> '000000'
              THEN l.[SapPosicionSuperior]
              ELSE l.[SapPosicion]
            END
          AS CHAR(6)) AS [pos_logica]
        FROM [cfl].[SapLipsRaw] l
        INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
          ON s.[SistemaFuente] = l.[SistemaFuente]
         AND s.[SapNumeroEntrega] = l.[SapNumeroEntrega]
      )
      INSERT INTO [cfl].[SapEntregaPosicionHistorial]
      (
        [IdSapEntregaPosicion],
        [IdLipsRaw],
        [IdEjecucion],
        [FechaExtraccion],
        [FechaCreacion]
      )
      SELECT
        p.[IdSapEntregaPosicion],
        lx.[IdSapLipsRaw],
        lx.[IdEjecucion],
        lx.[FechaExtraccion],
        @now
      FROM lips_x lx
      INNER JOIN [cfl].[SapEntrega] e
        ON e.[SistemaFuente] = lx.[SistemaFuente]
       AND e.[SapNumeroEntrega] = lx.[SapNumeroEntrega]
      INNER JOIN [cfl].[SapEntregaPosicion] p
        ON p.[IdSapEntrega] = e.[IdSapEntrega]
       AND p.[SapPosicion] = lx.[pos_logica]
      WHERE NOT EXISTS (
        SELECT 1
        FROM [cfl].[SapEntregaPosicionHistorial] h
        WHERE h.[IdLipsRaw] = lx.[IdSapLipsRaw]
      );
      SELECT affected = @@ROWCOUNT;
    `);

  const posUpdatedResult = await new sql.Request(transaction)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      UPDATE p
      SET p.[FechaActualizacion] = @now
      FROM [cfl].[SapEntregaPosicion] p
      WHERE EXISTS (
        SELECT 1
        FROM [cfl].[SapEntregaPosicionHistorial] h
        WHERE h.[IdSapEntregaPosicion] = p.[IdSapEntregaPosicion]
          AND h.[FechaCreacion] = @now
      );
      SELECT affected = @@ROWCOUNT;
    `);

  await new sql.Request(transaction)
    .input("now", sql.DateTime2(0), nowUtc)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .query(`
      UPDATE e
      SET e.CambiadoEnUltimaEjecucion = 0, e.TipoUltimoCambio = NULL
      FROM [cfl].[SapEntrega] e
      INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
        ON s.SistemaFuente = e.SistemaFuente
       AND s.SapNumeroEntrega = e.SapNumeroEntrega
      WHERE e.Bloqueado = 0;

      UPDATE p
      SET p.CambiadoEnUltimaEjecucion = 0, p.TipoUltimoCambio = NULL
      FROM [cfl].[SapEntregaPosicion] p
      INNER JOIN [cfl].[SapEntrega] e ON e.IdSapEntrega = p.IdSapEntrega
      INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
        ON s.SistemaFuente = e.SistemaFuente
       AND s.SapNumeroEntrega = e.SapNumeroEntrega
      WHERE e.Bloqueado = 0;

      UPDATE e
      SET e.IdEjecucionUltimaVista = @execution_id, e.FechaUltimaVista = @now
      FROM [cfl].[SapEntrega] e
      INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
        ON s.SistemaFuente = e.SistemaFuente
       AND s.SapNumeroEntrega = e.SapNumeroEntrega;

      IF EXISTS (SELECT 1 FROM [cfl].[StgLips] WHERE [IdEjecucion] = @execution_id)
      BEGIN
        ;WITH sx AS
        (
          SELECT DISTINCT
            e.IdSapEntrega,
            CAST(
              CASE
                WHEN NULLIF(LTRIM(RTRIM(s.SapPosicionSuperior)), '') IS NOT NULL
                  AND LTRIM(RTRIM(s.SapPosicionSuperior)) <> '000000'
                THEN s.SapPosicionSuperior
                ELSE s.SapPosicion
              END
            AS CHAR(6)) AS pos_logica
          FROM [cfl].[StgLips] s
          INNER JOIN [cfl].[SapEntrega] e
            ON e.SistemaFuente = s.SistemaFuente
           AND e.SapNumeroEntrega = s.SapNumeroEntrega
          WHERE s.IdEjecucion = @execution_id
        )
        UPDATE p
        SET
          p.IdEjecucionUltimaVista = @execution_id,
          p.FechaUltimaVista = @now,
          p.Estado = CASE WHEN p.Estado = 'MISSING' THEN 'ACTIVE' ELSE p.Estado END,
          p.AusenteDesde = CASE WHEN p.Estado = 'MISSING' THEN NULL ELSE p.AusenteDesde END,
          p.CambiadoEnUltimaEjecucion = CASE WHEN p.Estado = 'MISSING' THEN 1 ELSE p.CambiadoEnUltimaEjecucion END,
          p.FechaUltimoCambio = CASE WHEN p.Estado = 'MISSING' THEN @now ELSE p.FechaUltimoCambio END,
          p.TipoUltimoCambio = CASE WHEN p.Estado = 'MISSING' THEN 'REAPPEARED' ELSE p.TipoUltimoCambio END,
          p.FechaActualizacion = CASE WHEN p.Estado = 'MISSING' THEN @now ELSE p.FechaActualizacion END
        FROM [cfl].[SapEntregaPosicion] p
        INNER JOIN sx
          ON sx.IdSapEntrega = p.IdSapEntrega
         AND sx.pos_logica = p.SapPosicion;

        ;WITH in_scope AS
        (
          SELECT DISTINCT e.IdSapEntrega
          FROM [cfl].[SapEntrega] e
          INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
            ON s.SistemaFuente = e.SistemaFuente
           AND s.SapNumeroEntrega = e.SapNumeroEntrega
          WHERE e.Bloqueado = 0
        )
        UPDATE p
        SET
          p.Estado = 'MISSING',
          p.AusenteDesde = COALESCE(p.AusenteDesde, @now),
          p.CambiadoEnUltimaEjecucion = 1,
          p.FechaUltimoCambio = @now,
          p.TipoUltimoCambio = 'MISSING',
          p.FechaActualizacion = @now
        FROM [cfl].[SapEntregaPosicion] p
        INNER JOIN in_scope s
          ON s.IdSapEntrega = p.IdSapEntrega
        WHERE (p.IdEjecucionUltimaVista IS NULL OR p.IdEjecucionUltimaVista <> @execution_id)
          AND p.Estado <> 'MISSING';
      END;

      UPDATE e
      SET
        e.CambiadoEnUltimaEjecucion = 1,
        e.FechaUltimoCambio = @now,
        e.TipoUltimoCambio = CASE WHEN e.FechaCreacion = @now THEN 'NEW' ELSE 'UPDATED' END,
        e.IdEjecucionUltimoCambio = @execution_id,
        e.FechaActualizacion = @now
      FROM [cfl].[SapEntrega] e
      INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
        ON s.SistemaFuente = e.SistemaFuente
       AND s.SapNumeroEntrega = e.SapNumeroEntrega
      WHERE e.Bloqueado = 0
        AND (
          e.FechaCreacion = @now
          OR EXISTS (
            SELECT 1
            FROM [cfl].[SapLikpRaw] r
            WHERE r.IdEjecucion = @execution_id
              AND r.SistemaFuente = e.SistemaFuente
              AND r.SapNumeroEntrega = e.SapNumeroEntrega
          )
        );

      ;WITH changed_positions AS
      (
        SELECT p.IdSapEntregaPosicion
        FROM [cfl].[SapEntregaPosicion] p
        INNER JOIN [cfl].[SapEntrega] e
          ON e.IdSapEntrega = p.IdSapEntrega
        INNER JOIN (SELECT DISTINCT SistemaFuente, SapNumeroEntrega FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id) s
          ON s.SistemaFuente = e.SistemaFuente
         AND s.SapNumeroEntrega = e.SapNumeroEntrega
        WHERE p.FechaCreacion = @now

        UNION

        SELECT p.IdSapEntregaPosicion
        FROM [cfl].[SapEntregaPosicion] p
        INNER JOIN [cfl].[SapEntrega] e
          ON e.IdSapEntrega = p.IdSapEntrega
        INNER JOIN [cfl].[SapLipsRaw] l
          ON l.SistemaFuente = e.SistemaFuente
         AND l.SapNumeroEntrega = e.SapNumeroEntrega
         AND p.SapPosicion = CAST(
           CASE
             WHEN NULLIF(LTRIM(RTRIM(l.SapPosicionSuperior)), '') IS NOT NULL
               AND LTRIM(RTRIM(l.SapPosicionSuperior)) <> '000000'
             THEN l.SapPosicionSuperior
             ELSE l.SapPosicion
           END
         AS CHAR(6))
        WHERE l.IdEjecucion = @execution_id
      )
      UPDATE p
      SET
        p.CambiadoEnUltimaEjecucion = 1,
        p.FechaUltimoCambio = @now,
        p.TipoUltimoCambio = CASE WHEN p.FechaCreacion = @now THEN 'NEW' ELSE 'UPDATED' END,
        p.IdEjecucionUltimoCambio = @execution_id,
        p.FechaActualizacion = @now
      FROM [cfl].[SapEntregaPosicion] p
      INNER JOIN changed_positions c
        ON c.IdSapEntregaPosicion = p.IdSapEntregaPosicion
      INNER JOIN [cfl].[SapEntrega] e
        ON e.IdSapEntrega = p.IdSapEntrega
      WHERE e.Bloqueado = 0;
    `);

  return {
    entregas_insertadas: Number(entregaInsertedResult.recordset[0]?.affected || 0),
    entregas_actualizadas: Number(entregaUpdatedResult.recordset[0]?.affected || 0),
    entregas_historial_insertadas: Number(entregaHistInsertedResult.recordset[0]?.affected || 0),
    posiciones_insertadas: Number(posInsertedResult.recordset[0]?.affected || 0),
    posiciones_actualizadas: Number(posUpdatedResult.recordset[0]?.affected || 0),
    posiciones_historial_insertadas: Number(posHistInsertedResult.recordset[0]?.affected || 0),
  };
}

async function cleanupStageRows(transaction, executionId) {
  await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .query(`
      DELETE FROM [cfl].[StgLikp] WHERE IdEjecucion = @execution_id;
      DELETE FROM [cfl].[StgLips] WHERE IdEjecucion = @execution_id;
    `);
}

async function restoreDiscardedOnReimport(transaction, executionId, nowUtc) {
  await new sql.Request(transaction)
    .input("execution_id", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      UPDATE sd
      SET sd.Activo = 0,
          sd.FechaRestauracion = @now,
          sd.FechaActualizacion = @now
      FROM [cfl].[SapEntregaDescarte] sd
      INNER JOIN [cfl].[SapEntrega] e ON e.IdSapEntrega = sd.IdSapEntrega
      INNER JOIN (
        SELECT DISTINCT SistemaFuente, SapNumeroEntrega
        FROM [cfl].[StgLikp]
        WHERE IdEjecucion = @execution_id
      ) s ON s.SistemaFuente = e.SistemaFuente AND s.SapNumeroEntrega = e.SapNumeroEntrega
      WHERE sd.Activo = 1
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[FleteSapEntrega] fe WHERE fe.IdSapEntrega = e.IdSapEntrega
        );
    `);
}

async function persistExtraction(jobDefinition, extraction) {
  const executionId = jobDefinition.job_id;
  const extractedAtUtc = new Date();
  const createdAtUtc = new Date();
  const sourceSystem = buildSourceSystem(jobDefinition.destination);
  const stageLikpRows = transformLikpRows(
    extraction.likp_rows,
    sourceSystem,
    extraction.delivery_type_texts,
    executionId,
    extractedAtUtc,
    createdAtUtc
  );
  const stageLipsRows = transformLipsRows(
    extraction.lips_rows,
    sourceSystem,
    executionId,
    extractedAtUtc,
    createdAtUtc
  );

  const pool = await getPool();
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    await bulkInsertStageRows(transaction, stageLikpRows, stageLipsRows);

    const likpInserted = await insertLikpDeduped(transaction, executionId);
    const lipsInserted = await insertLipsDeduped(transaction, executionId);
    await normalizeRawState(transaction, executionId);

    const canonical = await buildCanonical(
      transaction,
      executionId,
      createdAtUtc,
      jobDefinition.job_type === JOB_TYPE.DATE_RANGE ? parseIsoDateSafe(jobDefinition.fecha_desde) : null,
      jobDefinition.job_type === JOB_TYPE.DATE_RANGE
    );

    await restoreDiscardedOnReimport(transaction, executionId, createdAtUtc);
    await cleanupStageRows(transaction, executionId);
    await transaction.commit();

    const rawSummary = {
      likp_rows_extracted: stageLikpRows.length,
      lips_rows_extracted: stageLipsRows.length,
      likp_rows_inserted: likpInserted,
      lips_rows_inserted: lipsInserted,
      likp_rows_unchanged: stageLikpRows.length - likpInserted,
      lips_rows_unchanged: stageLipsRows.length - lipsInserted,
    };

    const totals = {
      filas_extraidas: rawSummary.likp_rows_extracted + rawSummary.lips_rows_extracted,
      filas_insertadas:
        rawSummary.likp_rows_inserted +
        rawSummary.lips_rows_inserted +
        canonical.entregas_insertadas +
        canonical.entregas_historial_insertadas +
        canonical.posiciones_insertadas +
        canonical.posiciones_historial_insertadas,
      filas_actualizadas: canonical.entregas_actualizadas + canonical.posiciones_actualizadas,
      filas_sin_cambio: rawSummary.likp_rows_unchanged + rawSummary.lips_rows_unchanged,
    };

    return {
      extracted_at: extractedAtUtc.toISOString(),
      raw: rawSummary,
      canonical,
      totals,
    };
  } catch (error) {
    try {
      await transaction.rollback();
    } catch {
      // no-op
    }
    throw error;
  }
}

module.exports = {
  JOB_TYPE,
  failStaleJobs,
  insertQueuedJob,
  markJobRunning,
  updateJobSnapshot,
  markJobFinished,
  getJobRecord,
  getLatestJobRecord,
  getRecentJobRecords,
  persistExtraction,
};
