const express = require("express");
const { getPool, sql } = require("../db");
const { hasAnyPermission, resolveAuthContext } = require("../authz");

const router = express.Router();

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function parsePositiveInt(value, fallback) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
}

function toNullableTrimmedString(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseOptionalBigInt(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function parseRequiredBigInt(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function normalizeTipoMovimiento(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "PUSH" || normalized === "DESPACHO") {
    return "PUSH";
  }
  if (normalized === "PULL" || normalized === "RETORNO") {
    return "PULL";
  }
  return null;
}

const LIFECYCLE_STATUS = {
  DETECTADO: "DETECTADO",
  ACTUALIZADO: "ACTUALIZADO",
  ANULADO: "ANULADO",
  EN_REVISION: "EN_REVISION",
  COMPLETADO: "COMPLETADO",
  ASIGNADO_FOLIO: "ASIGNADO_FOLIO",
  FACTURADO: "FACTURADO",
};

function normalizeLifecycleStatus(rawStatus) {
  const normalized = String(rawStatus || "").trim().toUpperCase();
  if (!normalized) return null;

  if (normalized === "COMPLETO") return LIFECYCLE_STATUS.COMPLETADO;
  if (normalized === "VALIDADO") return LIFECYCLE_STATUS.ASIGNADO_FOLIO;
  if (normalized === "CERRADO") return LIFECYCLE_STATUS.FACTURADO;

  if (Object.values(LIFECYCLE_STATUS).includes(normalized)) {
    return normalized;
  }

  return null;
}

function deriveLifecycleStatus({
  requestedStatus,
  idFolio,
  idTipoFlete,
  idCentroCostoFinal,
  idDetalleViaje,
  idMovil,
  idTarifa,
  hasDetalles,
}) {
  if (requestedStatus === LIFECYCLE_STATUS.ANULADO) {
    return LIFECYCLE_STATUS.ANULADO;
  }
  if (requestedStatus === LIFECYCLE_STATUS.FACTURADO) {
    return LIFECYCLE_STATUS.FACTURADO;
  }
  if (idFolio && Number(idFolio) > 0) {
    return LIFECYCLE_STATUS.ASIGNADO_FOLIO;
  }

  const isComplete =
    Boolean(idTipoFlete) &&
    Boolean(idCentroCostoFinal) &&
    Boolean(idDetalleViaje) &&
    Boolean(idMovil) &&
    Boolean(idTarifa) &&
    Boolean(hasDetalles);

  return isComplete ? LIFECYCLE_STATUS.COMPLETADO : LIFECYCLE_STATUS.EN_REVISION;
}

async function resolveMovilId(transaction, cabeceraIn, now, fallbackMovilId = null) {
  const explicitMovilId = parseOptionalBigInt(cabeceraIn.id_movil);
  if (explicitMovilId) {
    return explicitMovilId;
  }

  const idEmpresaTransporte = parseOptionalBigInt(cabeceraIn.id_empresa_transporte);
  const idChofer = parseOptionalBigInt(cabeceraIn.id_chofer);
  const idCamion = parseOptionalBigInt(cabeceraIn.id_camion);

  if (!idEmpresaTransporte || !idChofer || !idCamion) {
    return fallbackMovilId;
  }

  const lookup = await new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresaTransporte)
    .input("idChofer", sql.BigInt, idChofer)
    .input("idCamion", sql.BigInt, idCamion)
    .query(`
      SELECT TOP 1 id_movil
      FROM [cfl].[CFL_movil]
      WHERE id_empresa_transporte = @idEmpresa
        AND id_chofer = @idChofer
        AND id_camion = @idCamion
      ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, id_movil ASC;
    `);

  const existingMovilId = lookup.recordset[0]?.id_movil || null;
  if (existingMovilId) {
    return Number(existingMovilId);
  }

  const created = await new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresaTransporte)
    .input("idChofer", sql.BigInt, idChofer)
    .input("idCamion", sql.BigInt, idCamion)
    .input("activo", sql.Bit, true)
    .input("createdAt", sql.DateTime2(0), now)
    .input("updatedAt", sql.DateTime2(0), now)
    .query(`
      INSERT INTO [cfl].[CFL_movil] (
        [id_chofer],
        [id_empresa_transporte],
        [id_camion],
        [activo],
        [created_at],
        [updated_at]
      )
      OUTPUT INSERTED.id_movil
      VALUES (
        @idChofer,
        @idEmpresa,
        @idCamion,
        @activo,
        @createdAt,
        @updatedAt
      );
    `);

  return Number(created.recordset[0].id_movil);
}

async function resolveFolioForLifecycle(transaction, idFolio) {
  const parsed = Number(idFolio);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  const result = await new sql.Request(transaction)
    .input("idFolio", sql.BigInt, parsed)
    .query(`
      SELECT TOP 1 folio_numero
      FROM [cfl].[CFL_folio]
      WHERE id_folio = @idFolio;
    `);

  const row = result.recordset[0] || null;
  if (!row) {
    return parsed;
  }

  const numero = String(row.folio_numero || "").trim();
  return numero === "0" ? null : parsed;
}

function buildMissingDeliveriesQuery(filters) {
  const whereClauses = [
    "NOT EXISTS (SELECT 1 FROM [cfl].[CFL_flete_sap_entrega] fe WHERE fe.id_sap_entrega = c.id_sap_entrega)",
  ];

  if (filters.search) {
    // Soporta búsqueda por entrega, referencia, empresa transporte, chofer y patente.
    whereClauses.push(`(
      c.sap_numero_entrega LIKE @search
      OR c.sap_referencia LIKE @search
      OR c.sap_empresa_transporte LIKE @search
      OR c.sap_nombre_chofer LIKE @search
      OR c.sap_patente LIKE @search
    )`);
  }
  if (filters.sourceSystem) {
    whereClauses.push("c.source_system = @sourceSystem");
  }
  if (filters.fechaDesde) {
    whereClauses.push("c.sap_fecha_salida >= @fechaDesde");
  }
  if (filters.fechaHasta) {
    whereClauses.push("c.sap_fecha_salida <= @fechaHasta");
  }
  if (filters.estado) {
    whereClauses.push("c.estado = @estado");
  }

  return `
    FROM #candidates c
    WHERE ${whereClauses.join(" AND ")}
  `;
}

router.get("/resumen", async (req, res, next) => {
  try {
    const pool = await getPool();
    const query = `
      SELECT
        total_entregas = (SELECT COUNT_BIG(1) FROM [cfl].[CFL_sap_entrega]),
        total_asociadas = (SELECT COUNT_BIG(DISTINCT id_sap_entrega) FROM [cfl].[CFL_flete_sap_entrega]),
        total_sin_cabecera = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_sap_entrega] e
          WHERE NOT EXISTS (
            SELECT 1
            FROM [cfl].[CFL_flete_sap_entrega] fe
            WHERE fe.id_sap_entrega = e.id_sap_entrega
          )
        );
    `;

    const result = await pool.request().query(query);
    res.json({
      data: result.recordset[0],
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/fletes/no-ingresados", async (req, res, next) => {
  const page = parsePositiveInt(req.query.page, 1);
  const pageSize = clamp(parsePositiveInt(req.query.page_size, 25), 1, 500);
  const offset = (page - 1) * pageSize;

  const filters = {
    search: toNullableTrimmedString(req.query.search),
    sourceSystem: toNullableTrimmedString(req.query.source_system),
    fechaDesde: toNullableTrimmedString(req.query.fecha_desde),
    fechaHasta: toNullableTrimmedString(req.query.fecha_hasta),
    estado: toNullableTrimmedString(req.query.estado),
  };

  try {
    const pool = await getPool();
    const request = pool.request();
    request.input("offset", offset);
    request.input("pageSize", pageSize);

    if (filters.search) {
      request.input("search", `%${filters.search}%`);
    }
    if (filters.sourceSystem) {
      request.input("sourceSystem", filters.sourceSystem);
    }
    if (filters.fechaDesde) {
      request.input("fechaDesde", filters.fechaDesde);
    }
    if (filters.fechaHasta) {
      request.input("fechaHasta", filters.fechaHasta);
    }
    if (filters.estado) {
      request.input("estado", filters.estado);
    }

    const baseQuery = buildMissingDeliveriesQuery(filters);

    const sqlQuery = `
      ;WITH ranked_lips AS
      (
        SELECT
          r.source_system,
          r.sap_numero_entrega,
          r.sap_posicion,
          r.sap_cantidad_entregada,
          r.sap_posicion_superior,
          rn = ROW_NUMBER() OVER
          (
            PARTITION BY r.source_system, r.sap_numero_entrega, r.sap_posicion
            ORDER BY
              CASE WHEN r.sap_posicion_superior IS NOT NULL THEN 0 ELSE 1 END,
              r.extracted_at DESC,
              r.raw_id DESC
          )
        FROM [cfl].[CFL_sap_lips_raw] r
        WHERE r.row_status = 'ACTIVE'
      )
      SELECT
        source_system,
        sap_numero_entrega,
        sap_posicion,
        sap_cantidad_entregada
      INTO #lips_current_pref
      FROM ranked_lips
      WHERE rn = 1;

      SELECT
        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system,

        sap_referencia = NULLIF(LTRIM(RTRIM(lk.sap_referencia)), ''),
        sap_guia_remision = NULLIF(LTRIM(RTRIM(lk.sap_guia_remision)), ''),
        sap_codigo_tipo_flete = NULLIF(LTRIM(RTRIM(lk.sap_codigo_tipo_flete)), ''),
        sap_centro_costo = NULLIF(LTRIM(RTRIM(lk.sap_centro_costo)), ''),
        sap_cuenta_mayor = NULLIF(LTRIM(RTRIM(lk.sap_cuenta_mayor)), ''),
        sap_fecha_salida = lk.sap_fecha_salida,
        sap_hora_salida = CONVERT(VARCHAR(8), lk.sap_hora_salida, 108),
        sap_empresa_transporte = NULLIF(LTRIM(RTRIM(lk.sap_empresa_transporte)), ''),
        sap_nombre_chofer = NULLIF(LTRIM(RTRIM(lk.sap_nombre_chofer)), ''),
        sap_patente = NULLIF(LTRIM(RTRIM(lk.sap_patente)), ''),
        sap_carro = NULLIF(LTRIM(RTRIM(lk.sap_carro)), ''),
        sap_peso_total = lk.sap_peso_total,
        sap_peso_neto = lk.sap_peso_neto,

        posiciones_total = COUNT(lp.sap_posicion),
        cantidad_entregada_total = COALESCE(SUM(CAST(lp.sap_cantidad_entregada AS DECIMAL(18,3))), 0),

        id_tipo_flete = tf.id_tipo_flete,
        tipo_flete_nombre = tf.nombre,
        id_centro_costo_final = COALESCE(cc_sap.id_centro_costo, tf.id_centro_costo),
        -- Semantica: candidatos (aun no existe cabecera) => siempre DETECTADO.
        estado = 'DETECTADO',
        puede_ingresar = CAST(
          CASE
            WHEN tf.id_tipo_flete IS NULL THEN 0
            WHEN COALESCE(cc_sap.id_centro_costo, tf.id_centro_costo) IS NULL THEN 0
            WHEN lk.sap_fecha_salida IS NULL THEN 0
            WHEN lk.sap_hora_salida IS NULL THEN 0
            ELSE 1
          END AS BIT
        ),
        motivo_no_ingreso = CASE
          WHEN tf.id_tipo_flete IS NULL THEN CONCAT(
            'Falta configurar Tipo de Flete para sap_codigo_tipo_flete=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.sap_codigo_tipo_flete)), ''), '(NULL)')
          )
          WHEN COALESCE(cc_sap.id_centro_costo, tf.id_centro_costo) IS NULL THEN CONCAT(
            'No se pudo resolver Centro de Costo (sap_centro_costo=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.sap_centro_costo)), ''), '(NULL)'),
            ')'
          )
          WHEN lk.sap_fecha_salida IS NULL THEN 'Falta sap_fecha_salida'
          WHEN lk.sap_hora_salida IS NULL THEN 'Falta sap_hora_salida'
          ELSE NULL
        END,

        e.last_seen_at,
        e.updated_at
      INTO #candidates
      FROM [cfl].[CFL_sap_entrega] e
      LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
        ON lk.sap_numero_entrega = e.sap_numero_entrega
       AND lk.source_system = e.source_system
      LEFT JOIN #lips_current_pref lp
        ON lp.sap_numero_entrega = e.sap_numero_entrega
       AND lp.source_system = e.source_system
      LEFT JOIN [cfl].[CFL_tipo_flete] tf
        ON tf.sap_codigo = lk.sap_codigo_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc_sap
        ON cc_sap.sap_codigo = lk.sap_centro_costo
      GROUP BY
        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system,
        lk.sap_referencia,
        lk.sap_guia_remision,
        lk.sap_codigo_tipo_flete,
        lk.sap_centro_costo,
        lk.sap_cuenta_mayor,
        lk.sap_fecha_salida,
        lk.sap_hora_salida,
        lk.sap_empresa_transporte,
        lk.sap_nombre_chofer,
        lk.sap_patente,
        lk.sap_carro,
        lk.sap_peso_total,
        lk.sap_peso_neto,
        tf.id_tipo_flete,
        tf.nombre,
        tf.id_centro_costo,
        cc_sap.id_centro_costo,
        e.last_seen_at,
        e.updated_at;

      SELECT total = COUNT_BIG(1)
      ${baseQuery};

      SELECT
        id_sap_entrega,
        sap_numero_entrega,
        source_system,
        sap_referencia,
        sap_guia_remision,
        sap_codigo_tipo_flete,
        sap_centro_costo,
        sap_cuenta_mayor,
        sap_fecha_salida,
        sap_hora_salida,
        sap_empresa_transporte,
        sap_nombre_chofer,
        sap_patente,
        sap_carro,
        sap_peso_total,
        sap_peso_neto,
        posiciones_total,
        cantidad_entregada_total,
        id_tipo_flete,
        tipo_flete_nombre,
        id_centro_costo_final,
        estado,
        puede_ingresar,
        motivo_no_ingreso,
        last_seen_at,
        updated_at
      ${baseQuery}
      ORDER BY
        COALESCE(CAST(sap_fecha_salida AS DATETIME2(0)), updated_at) DESC,
        id_sap_entrega DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;

      DROP TABLE #candidates;
      DROP TABLE #lips_current_pref;
    `;

    const result = await request.query(sqlQuery);
    const total = Number(result.recordsets[0][0].total);
    const data = result.recordsets[1];

    res.json({
      data,
      pagination: {
        page,
        page_size: pageSize,
        total,
        total_pages: Math.ceil(total / pageSize),
      },
    });
  } catch (error) {
    next(error);
  }
});

router.get("/fletes/no-ingresados/:id_sap_entrega/detalle", async (req, res, next) => {
  const idSapEntrega = Number(req.params.id_sap_entrega);
  if (!Number.isInteger(idSapEntrega) || idSapEntrega <= 0) {
    res.status(400).json({ error: "id_sap_entrega invalido" });
    return;
  }

  try {
    const pool = await getPool();

    const headerResult = await pool.request().input("idSapEntrega", sql.BigInt, idSapEntrega).query(`
      SELECT TOP 1
        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system,

        sap_referencia = NULLIF(LTRIM(RTRIM(lk.sap_referencia)), ''),
        sap_guia_remision = NULLIF(LTRIM(RTRIM(lk.sap_guia_remision)), ''),
        sap_codigo_tipo_flete = NULLIF(LTRIM(RTRIM(lk.sap_codigo_tipo_flete)), ''),
        sap_centro_costo = NULLIF(LTRIM(RTRIM(lk.sap_centro_costo)), ''),
        sap_cuenta_mayor = NULLIF(LTRIM(RTRIM(lk.sap_cuenta_mayor)), ''),
        sap_fecha_salida = lk.sap_fecha_salida,
        sap_hora_salida = CONVERT(VARCHAR(8), lk.sap_hora_salida, 108),
        sap_empresa_transporte = NULLIF(LTRIM(RTRIM(lk.sap_empresa_transporte)), ''),
        sap_nombre_chofer = NULLIF(LTRIM(RTRIM(lk.sap_nombre_chofer)), ''),
        sap_patente = NULLIF(LTRIM(RTRIM(lk.sap_patente)), ''),
        sap_carro = NULLIF(LTRIM(RTRIM(lk.sap_carro)), ''),
        sap_peso_total = lk.sap_peso_total,
        sap_peso_neto = lk.sap_peso_neto,
        e.last_seen_at,
        e.updated_at
      FROM [cfl].[CFL_sap_entrega] e
      LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
        ON lk.sap_numero_entrega = e.sap_numero_entrega
       AND lk.source_system = e.source_system
      WHERE e.id_sap_entrega = @idSapEntrega;
    `);

    const cabecera = headerResult.recordset[0];
    if (!cabecera) {
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const positionsResult = await pool
      .request()
      .input("sourceSystem", sql.VarChar(50), cabecera.source_system)
      .input("sapNumeroEntrega", sql.VarChar(20), cabecera.sap_numero_entrega)
      .query(`
        ;WITH ranked AS
        (
          SELECT
            r.*,
            rn = ROW_NUMBER() OVER
            (
              PARTITION BY r.source_system, r.sap_numero_entrega, r.sap_posicion
              ORDER BY
                CASE WHEN r.sap_posicion_superior IS NOT NULL THEN 0 ELSE 1 END,
                r.extracted_at DESC,
                r.raw_id DESC
            )
          FROM [cfl].[CFL_sap_lips_raw] r
          WHERE r.row_status = 'ACTIVE'
            AND r.source_system = @sourceSystem
            AND r.sap_numero_entrega = @sapNumeroEntrega
        )
        SELECT
          sap_posicion,
          sap_material,
          sap_denominacion_material,
          sap_cantidad_entregada,
          sap_unidad_peso,
          sap_centro,
          sap_almacen,
          sap_posicion_superior,
          sap_lote
        FROM ranked
        WHERE rn = 1
        ORDER BY sap_posicion ASC;
      `);

    res.json({
      data: {
        cabecera,
        posiciones: positionsResult.recordset,
      },
    });
  } catch (error) {
    next(error);
  }
});

router.get("/fletes/completos-sin-folio", async (req, res, next) => {
  const page = parsePositiveInt(req.query.page, 1);
  const pageSize = clamp(parsePositiveInt(req.query.page_size, 25), 1, 500);
  const offset = (page - 1) * pageSize;
  const estadoFiltro = toNullableTrimmedString(req.query.estado);

  try {
    const pool = await getPool();
    const request = pool.request();
    request.input("offset", offset);
    request.input("pageSize", pageSize);
    request.input("estadoFiltro", sql.VarChar(30), estadoFiltro ? estadoFiltro.toUpperCase() : null);

    const querySql = `
      ;WITH detalle_counts AS (
        SELECT
          id_cabecera_flete,
          total_detalles = COUNT_BIG(1)
        FROM [cfl].[CFL_detalle_flete]
        GROUP BY id_cabecera_flete
      ),
      base AS (
      SELECT
        cf.id_cabecera_flete,
        cf.id_folio,
        folio_numero = fol.folio_numero,
        cf.estado AS estado_original,
        cf.tipo_movimiento,
        cf.fecha_salida,
        cf.hora_salida,
        cf.monto_aplicado,
        cf.observaciones,
        cf.id_tipo_flete,
        cf.id_detalle_viaje,
        cf.id_movil,
        cf.id_tarifa,
        cf.id_cuenta_mayor,
        tf.nombre AS tipo_flete_nombre,
        cf.id_centro_costo_final,
        cc.nombre AS centro_costo_final_nombre,
        det.total_detalles,
        cf.created_at,
        cf.updated_at,
        lk.created_at AS sap_updated_at,

        e.id_sap_entrega,
        sap_numero_entrega = NULLIF(LTRIM(RTRIM(e.sap_numero_entrega)), ''),
        e.source_system,
        sap_guia_remision = NULLIF(LTRIM(RTRIM(lk.sap_guia_remision)), ''),
        numero_guia = COALESCE(
          NULLIF(LTRIM(RTRIM(lk.sap_guia_remision)), ''),
          NULLIF(LTRIM(RTRIM(cf.sap_numero_entrega_sugerido)), ''),
          NULLIF(LTRIM(RTRIM(e.sap_numero_entrega)), '')
        ),
        sap_empresa_transporte = NULLIF(LTRIM(RTRIM(lk.sap_empresa_transporte)), ''),
        sap_nombre_chofer = NULLIF(LTRIM(RTRIM(lk.sap_nombre_chofer)), ''),
        sap_patente = NULLIF(LTRIM(RTRIM(lk.sap_patente)), ''),
        sap_carro = NULLIF(LTRIM(RTRIM(lk.sap_carro)), ''),
        id_ruta = r.id_ruta,
        ruta_nombre = NULLIF(LTRIM(RTRIM(r.nombre_ruta)), ''),
        ruta_origen_nombre = NULLIF(LTRIM(RTRIM(no.nombre)), ''),
        ruta_destino_nombre = NULLIF(LTRIM(RTRIM(nd.nombre)), ''),
        movil_empresa = NULLIF(LTRIM(RTRIM(et.razon_social)), ''),
        movil_chofer_rut = NULLIF(LTRIM(RTRIM(ch.sap_id_fiscal)), ''),
        movil_chofer_nombre = NULLIF(LTRIM(RTRIM(ch.sap_nombre)), ''),
        movil_tipo_camion = NULLIF(LTRIM(RTRIM(tc.nombre)), ''),
        movil_patente = NULLIF(LTRIM(RTRIM(cam.sap_patente)), ''),
        estado_lifecycle = CASE
          WHEN UPPER(ISNULL(cf.estado, '')) = 'ANULADO' THEN 'ANULADO'
          WHEN UPPER(ISNULL(cf.estado, '')) = 'FACTURADO' THEN 'FACTURADO'
          WHEN COALESCE(cf.id_folio, 0) > 0
            AND ISNULL(LTRIM(RTRIM(CAST(fol.folio_numero AS NVARCHAR(50)))), '') <> '0'
            THEN 'ASIGNADO_FOLIO'
          WHEN lk.created_at IS NOT NULL AND cf.updated_at IS NOT NULL AND lk.created_at > cf.updated_at THEN 'ACTUALIZADO'
          WHEN cf.id_tipo_flete IS NOT NULL
            AND cf.id_centro_costo_final IS NOT NULL
            AND cf.id_detalle_viaje IS NOT NULL
            AND cf.id_movil IS NOT NULL
            AND cf.id_tarifa IS NOT NULL
            AND COALESCE(det.total_detalles, 0) > 0 THEN 'COMPLETADO'
          ELSE 'EN_REVISION'
        END
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_folio] fol ON fol.id_folio = cf.id_folio
      LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo_final
      LEFT JOIN [cfl].[CFL_movil] mv ON mv.id_movil = cf.id_movil
      LEFT JOIN [cfl].[CFL_empresa_transporte] et ON et.id_empresa = mv.id_empresa_transporte
      LEFT JOIN [cfl].[CFL_chofer] ch ON ch.id_chofer = mv.id_chofer
      LEFT JOIN [cfl].[CFL_camion] cam ON cam.id_camion = mv.id_camion
      LEFT JOIN [cfl].[CFL_tipo_camion] tc ON tc.id_tipo_camion = cam.id_tipo_camion
      LEFT JOIN [cfl].[CFL_tarifa] tfa ON tfa.id_tarifa = cf.id_tarifa
      LEFT JOIN [cfl].[CFL_ruta] r ON r.id_ruta = tfa.id_ruta
      LEFT JOIN [cfl].[CFL_nodo_logistico] no ON no.id_nodo = r.id_origen_nodo
      LEFT JOIN [cfl].[CFL_nodo_logistico] nd ON nd.id_nodo = r.id_destino_nodo
      LEFT JOIN detalle_counts det ON det.id_cabecera_flete = cf.id_cabecera_flete
      LEFT JOIN [cfl].[CFL_flete_sap_entrega] fe ON fe.id_cabecera_flete = cf.id_cabecera_flete
      LEFT JOIN [cfl].[CFL_sap_entrega] e ON e.id_sap_entrega = fe.id_sap_entrega
      LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
        ON lk.sap_numero_entrega = e.sap_numero_entrega
       AND lk.source_system = e.source_system
      )

      SELECT
        total_rows = COUNT_BIG(1) OVER(),
        id_cabecera_flete,
        id_folio,
        folio_numero,
        estado = estado_lifecycle,
        estado_original,
        tipo_movimiento,
        fecha_salida,
        hora_salida,
        monto_aplicado,
        observaciones,
        id_tipo_flete,
        id_detalle_viaje,
        id_movil,
        id_tarifa,
        id_cuenta_mayor,
        tipo_flete_nombre,
        id_centro_costo_final,
        centro_costo_final_nombre,
        total_detalles,
        created_at,
        updated_at,
        sap_updated_at,
        id_sap_entrega,
        sap_numero_entrega,
        source_system,
        sap_guia_remision,
        numero_guia,
        sap_empresa_transporte,
        sap_nombre_chofer,
        sap_patente,
        sap_carro,
        id_ruta,
        ruta_nombre,
        ruta_origen_nombre,
        ruta_destino_nombre,
        movil_empresa,
        movil_chofer_rut,
        movil_chofer_nombre,
        movil_tipo_camion,
        movil_patente
      FROM base
      WHERE @estadoFiltro IS NULL OR estado_lifecycle = @estadoFiltro
      ORDER BY updated_at DESC, id_cabecera_flete DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;
    `;

    const result = await request.query(querySql);
    const rows = result.recordset || [];
    const total = rows.length > 0 ? Number(rows[0].total_rows || 0) : 0;

    res.json({
      data: rows.map(({ total_rows, ...row }) => row),
      pagination: {
        page,
        page_size: pageSize,
        total,
        total_pages: Math.ceil(total / pageSize),
      },
    });
  } catch (error) {
    next(error);
  }
});

router.post("/folios/asignar", async (req, res, next) => {
  let auth = null;
  try {
    auth = await resolveAuthContext(req);
  } catch (error) {
    next(error);
    return;
  }

  if (!hasAnyPermission(auth, ["folios.asignar", "folios.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para asignar folios",
      role: auth?.primaryRole || null,
    });
    return;
  }

  const body = req.body || {};
  const idFolio = Number(body.id_folio);
  const ids = Array.isArray(body.ids_cabecera_flete) ? body.ids_cabecera_flete : [];

  if (!Number.isInteger(idFolio) || idFolio <= 0) {
    res.status(400).json({ error: "id_folio invalido" });
    return;
  }

  const cabeceraIds = ids.map((value) => Number(value)).filter((value) => Number.isInteger(value) && value > 0);
  if (cabeceraIds.length === 0) {
    res.status(400).json({ error: "Debes enviar ids_cabecera_flete" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();

    const folioResult = await new sql.Request(transaction)
      .input("idFolio", sql.BigInt, idFolio)
      .query(`
        SELECT TOP 1 id_folio, bloqueado, estado, folio_numero
        FROM [cfl].[CFL_folio]
        WHERE id_folio = @idFolio;
      `);

    const folio = folioResult.recordset[0];
    if (!folio) {
      await transaction.rollback();
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    if (folio.bloqueado === true || folio.bloqueado === 1) {
      await transaction.rollback();
      res.status(409).json({ error: "El folio esta bloqueado" });
      return;
    }

    const targetFolioNumero = String(folio.folio_numero || "").trim();
    const targetFolioIsDefault = targetFolioNumero === "0";

    const invalid = [];

    for (const idCabecera of cabeceraIds) {
      const rowResult = await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .query(`
          SELECT TOP 1
            cf.id_cabecera_flete,
            cf.estado,
            cf.id_folio,
            folio_numero = f.folio_numero
          FROM [cfl].[CFL_cabecera_flete] cf
          LEFT JOIN [cfl].[CFL_folio] f ON f.id_folio = cf.id_folio
          WHERE cf.id_cabecera_flete = @idCabecera;
        `);

      const row = rowResult.recordset[0];
      if (!row) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "No existe" });
        continue;
      }
      const folioNumeroActual = String(row.folio_numero || "").trim();
      const folioValue = row.id_folio === null || row.id_folio === undefined ? 0 : Number(row.id_folio);
      const folioEsDefault = folioNumeroActual === "0";
      const normalizedEstado = normalizeLifecycleStatus(row.estado);
      const estadoElegible = normalizedEstado === LIFECYCLE_STATUS.COMPLETADO
        || (folioEsDefault && normalizedEstado === LIFECYCLE_STATUS.ASIGNADO_FOLIO);
      if (!estadoElegible) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: `Estado invalido: ${row.estado}` });
        continue;
      }
      if (folioValue !== 0 && !folioEsDefault) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "Ya tiene folio asignado" });
        continue;
      }
    }

    if (invalid.length > 0) {
      await transaction.rollback();
      res.status(422).json({ error: "Hay registros no elegibles", invalid });
      return;
    }

    for (const idCabecera of cabeceraIds) {
      await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .input("idFolio", sql.BigInt, idFolio)
        .input("estadoAsignado", sql.VarChar(20), targetFolioIsDefault ? LIFECYCLE_STATUS.COMPLETADO : LIFECYCLE_STATUS.ASIGNADO_FOLIO)
        .input("updatedAt", sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CFL_cabecera_flete]
          SET id_folio = @idFolio,
              estado = @estadoAsignado,
              updated_at = @updatedAt
          WHERE id_cabecera_flete = @idCabecera;
        `);
    }

    await transaction.commit();

    res.json({
      message: "Folio asignado",
      data: {
        id_folio: idFolio,
        updated: cabeceraIds.length,
      },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }
    next(error);
  }
});

router.post("/folios/asignar-nuevo", async (req, res, next) => {
  let auth = null;
  try {
    auth = await resolveAuthContext(req);
  } catch (error) {
    next(error);
    return;
  }

  if (!hasAnyPermission(auth, ["folios.asignar", "folios.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para asignar folios",
      role: auth?.primaryRole || null,
    });
    return;
  }

  const body = req.body || {};
  const ids = Array.isArray(body.ids_cabecera_flete) ? body.ids_cabecera_flete : [];
  const cabeceraIds = ids.map((value) => Number(value)).filter((value) => Number.isInteger(value) && value > 0);
  if (cabeceraIds.length === 0) {
    res.status(400).json({ error: "Debes enviar ids_cabecera_flete" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();
    const invalid = [];
    const centerCostSet = new Set();
    const salidaDates = [];

    for (const idCabecera of cabeceraIds) {
      const rowResult = await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .query(`
          SELECT TOP 1
            cf.id_cabecera_flete,
            cf.estado,
            cf.id_folio,
            cf.id_centro_costo_final,
            cf.fecha_salida,
            folio_numero = f.folio_numero
          FROM [cfl].[CFL_cabecera_flete] cf
          LEFT JOIN [cfl].[CFL_folio] f ON f.id_folio = cf.id_folio
          WHERE cf.id_cabecera_flete = @idCabecera;
        `);

      const row = rowResult.recordset[0];
      if (!row) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "No existe" });
        continue;
      }

      const folioNumeroActual = String(row.folio_numero || "").trim();
      const folioValue = row.id_folio === null || row.id_folio === undefined ? 0 : Number(row.id_folio);
      const folioEsDefault = folioNumeroActual === "0";
      const normalizedEstado = normalizeLifecycleStatus(row.estado);
      const estadoElegible = normalizedEstado === LIFECYCLE_STATUS.COMPLETADO
        || (folioEsDefault && normalizedEstado === LIFECYCLE_STATUS.ASIGNADO_FOLIO);

      if (!estadoElegible) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: `Estado invalido: ${row.estado}` });
        continue;
      }
      if (folioValue !== 0 && !folioEsDefault) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "Ya tiene folio asignado" });
        continue;
      }

      const idCentroCosto = Number(row.id_centro_costo_final || 0);
      if (!Number.isInteger(idCentroCosto) || idCentroCosto <= 0) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "Centro de costo invalido" });
        continue;
      }
      centerCostSet.add(idCentroCosto);

      if (row.fecha_salida) {
        salidaDates.push(new Date(row.fecha_salida));
      }
    }

    if (invalid.length > 0) {
      await transaction.rollback();
      res.status(422).json({ error: "Hay registros no elegibles", invalid });
      return;
    }

    const centerCosts = Array.from(centerCostSet);
    if (centerCosts.length !== 1) {
      await transaction.rollback();
      res.status(422).json({
        error: "Los movimientos seleccionados deben pertenecer al mismo centro de costo para crear un solo folio",
        centros_costo: centerCosts,
      });
      return;
    }

    const idCentroCosto = Number(centerCosts[0]);

    const temporadaResult = await new sql.Request(transaction).query(`
      SELECT TOP 1 id_temporada
      FROM [cfl].[CFL_temporada]
      WHERE activa = 1
      ORDER BY CASE WHEN ISNULL(cerrada, 0) = 0 THEN 0 ELSE 1 END, fecha_inicio DESC, id_temporada DESC;
    `);

    const idTemporada = Number(temporadaResult.recordset[0]?.id_temporada || 0);
    if (!Number.isInteger(idTemporada) || idTemporada <= 0) {
      await transaction.rollback();
      res.status(409).json({ error: "No existe una temporada activa para crear folio" });
      return;
    }

    const nextNumberResult = await new sql.Request(transaction)
      .input("idTemporada", sql.BigInt, idTemporada)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .query(`
        SELECT
          next_num = COALESCE(MAX(TRY_CONVERT(BIGINT, NULLIF(LTRIM(RTRIM(folio_numero)), ''))), 0) + 1
        FROM [cfl].[CFL_folio] WITH (UPDLOCK, HOLDLOCK)
        WHERE id_temporada = @idTemporada
          AND id_centro_costo = @idCentroCosto
          AND TRY_CONVERT(BIGINT, NULLIF(LTRIM(RTRIM(folio_numero)), '')) IS NOT NULL;
      `);

    const folioNumero = String(nextNumberResult.recordset[0]?.next_num || "1");
    const periodoDesde = salidaDates.length > 0
      ? new Date(Math.min(...salidaDates.map((d) => d.getTime())))
      : null;
    const periodoHasta = salidaDates.length > 0
      ? new Date(Math.max(...salidaDates.map((d) => d.getTime())))
      : null;

    const createFolioResult = await new sql.Request(transaction)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .input("idTemporada", sql.BigInt, idTemporada)
      .input("folioNumero", sql.VarChar(30), folioNumero)
      .input("periodoDesde", sql.DateTime2(0), periodoDesde)
      .input("periodoHasta", sql.DateTime2(0), periodoHasta)
      .input("estado", sql.VarChar(20), "ABIERTO")
      .input("bloqueado", sql.Bit, false)
      .input("createdAt", sql.DateTime2(0), now)
      .input("updatedAt", sql.DateTime2(0), now)
      .query(`
        INSERT INTO [cfl].[CFL_folio] (
          [id_centro_costo],
          [id_temporada],
          [folio_numero],
          [periodo_desde],
          [periodo_hasta],
          [estado],
          [bloqueado],
          [created_at],
          [updated_at]
        )
        OUTPUT INSERTED.id_folio
        VALUES (
          @idCentroCosto,
          @idTemporada,
          @folioNumero,
          @periodoDesde,
          @periodoHasta,
          @estado,
          @bloqueado,
          @createdAt,
          @updatedAt
        );
      `);

    const idFolio = Number(createFolioResult.recordset[0]?.id_folio || 0);
    if (!Number.isInteger(idFolio) || idFolio <= 0) {
      await transaction.rollback();
      res.status(500).json({ error: "No se pudo crear el folio" });
      return;
    }

    for (const idCabecera of cabeceraIds) {
      await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .input("idFolio", sql.BigInt, idFolio)
        .input("estadoAsignado", sql.VarChar(20), LIFECYCLE_STATUS.ASIGNADO_FOLIO)
        .input("updatedAt", sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CFL_cabecera_flete]
          SET id_folio = @idFolio,
              estado = @estadoAsignado,
              updated_at = @updatedAt
          WHERE id_cabecera_flete = @idCabecera;
        `);
    }

    await transaction.commit();

    res.json({
      message: "Nuevo folio creado y asignado",
      data: {
        id_folio: idFolio,
        folio_numero: folioNumero,
        id_temporada: idTemporada,
        id_centro_costo: idCentroCosto,
        updated: cabeceraIds.length,
      },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }

    const sqlCode = error?.number || error?.originalError?.info?.number || null;
    if (sqlCode === 2601 || sqlCode === 2627) {
      res.status(409).json({ error: "Conflicto al generar folio. Reintenta la asignacion." });
      return;
    }

    next(error);
  }
});

router.post("/fletes/:id_cabecera_flete/anular", async (req, res, next) => {
  const idCabecera = Number(req.params.id_cabecera_flete);
  if (!Number.isInteger(idCabecera) || idCabecera <= 0) {
    res.status(400).json({ error: "id_cabecera_flete invalido" });
    return;
  }

  let auth = null;
  try {
    auth = await resolveAuthContext(req);
  } catch (error) {
    next(error);
    return;
  }

  if (!hasAnyPermission(auth, ["fletes.editar", "fletes.estado.cambiar", "excepciones.gestionar", "mantenedores.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para anular fletes",
      role: auth?.primaryRole || null,
    });
    return;
  }

  try {
    const pool = await getPool();
    const current = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
      SELECT TOP 1 id_cabecera_flete, estado
      FROM [cfl].[CFL_cabecera_flete]
      WHERE id_cabecera_flete = @idCabecera;
    `);

    const row = current.recordset[0];
    if (!row) {
      res.status(404).json({ error: "Cabecera de flete no encontrada" });
      return;
    }

    const normalized = normalizeLifecycleStatus(row.estado);
    if (normalized === LIFECYCLE_STATUS.FACTURADO) {
      res.status(409).json({ error: "Un flete FACTURADO no se puede anular" });
      return;
    }

    await pool
      .request()
      .input("idCabecera", sql.BigInt, idCabecera)
      .input("estado", sql.VarChar(20), LIFECYCLE_STATUS.ANULADO)
      .input("updatedAt", sql.DateTime2(0), new Date())
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET estado = @estado,
            updated_at = @updatedAt
        WHERE id_cabecera_flete = @idCabecera;
      `);

    res.json({
      message: "Flete anulado",
      data: {
        id_cabecera_flete: idCabecera,
        estado: LIFECYCLE_STATUS.ANULADO,
      },
    });
  } catch (error) {
    next(error);
  }
});

router.post("/fletes/no-ingresados/:id_sap_entrega/crear", async (req, res, next) => {
  const idSapEntrega = Number(req.params.id_sap_entrega);
  if (!Number.isInteger(idSapEntrega) || idSapEntrega <= 0) {
    res.status(400).json({ error: "id_sap_entrega invalido" });
    return;
  }

  const body = req.body || {};
  const cabeceraIn = body.cabecera || {};
  const detallesIn = Array.isArray(body.detalles) ? body.detalles : [];

  const idTipoFlete = parseRequiredBigInt(cabeceraIn.id_tipo_flete);
  const idCentroCostoFinal = parseRequiredBigInt(cabeceraIn.id_centro_costo_final);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const idDetalleViaje = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idFolio = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifa = parseOptionalBigInt(cabeceraIn.id_tarifa);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoFinal) {
    res.status(400).json({ error: "Falta id_centro_costo_final" });
    return;
  }
  if (!tipoMovimiento) {
    res.status(400).json({ error: "tipo_movimiento invalido (Despacho/Retorno)" });
    return;
  }
  if (!fechaSalida) {
    res.status(400).json({ error: "Falta fecha_salida (YYYY-MM-DD)" });
    return;
  }
  if (!horaSalida) {
    res.status(400).json({ error: "Falta hora_salida (HH:MM[:SS])" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();
    const idMovil = await resolveMovilId(transaction, cabeceraIn, now);
    const lifecycleFolioId = await resolveFolioForLifecycle(transaction, idFolio);
    const estado = deriveLifecycleStatus({
      requestedStatus,
      idFolio: lifecycleFolioId,
      idTipoFlete,
      idCentroCostoFinal,
      idDetalleViaje,
      idMovil,
      idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    const entregaResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1
          e.id_sap_entrega,
          e.sap_numero_entrega,
          e.source_system,
          lk.sap_codigo_tipo_flete,
          lk.sap_centro_costo,
          lk.sap_cuenta_mayor
        FROM [cfl].[CFL_sap_entrega] e
        LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
          ON lk.sap_numero_entrega = e.sap_numero_entrega
         AND lk.source_system = e.source_system
        WHERE e.id_sap_entrega = @idSapEntrega;
      `);

    const entrega = entregaResult.recordset[0];
    if (!entrega) {
      await transaction.rollback();
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const existsResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 1 AS already_linked
        FROM [cfl].[CFL_flete_sap_entrega]
        WHERE id_sap_entrega = @idSapEntrega;
      `);

    if (existsResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({ error: "La entrega SAP ya se encuentra asociada" });
      return;
    }

    const sapCuentaMayor = toNullableTrimmedString(entrega.sap_cuenta_mayor);
    const sapTipoFlete = toNullableTrimmedString(entrega.sap_codigo_tipo_flete);
    const sapCentroCosto = toNullableTrimmedString(entrega.sap_centro_costo);

    const cuentaMayorFinalRaw = toNullableTrimmedString(cabeceraIn.cuenta_mayor_final) || sapCuentaMayor;

    const insertCabeceraReq = new sql.Request(transaction);
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, idDetalleViaje);
    insertCabeceraReq.input("idFolio", sql.BigInt, idFolio);
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), entrega.sap_numero_entrega);
    insertCabeceraReq.input("sapCodigoTipoFleteSug", sql.Char(4), sapTipoFlete ? sapTipoFlete.slice(0, 4) : null);
    insertCabeceraReq.input("sapCentroCostoSug", sql.Char(10), sapCentroCosto ? sapCentroCosto.slice(0, 10) : null);
    insertCabeceraReq.input("sapCuentaMayorSug", sql.Char(10), sapCuentaMayor ? sapCuentaMayor.slice(0, 10) : null);
    insertCabeceraReq.input("cuentaMayorFinal", sql.Char(10), cuentaMayorFinalRaw ? cuentaMayorFinalRaw.slice(0, 10) : null);
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), tipoMovimiento);
    insertCabeceraReq.input("estado", sql.VarChar(20), estado);
    insertCabeceraReq.input("fechaSalida", sql.Date, fechaSalida);
    insertCabeceraReq.input("horaSalida", sql.VarChar(8), horaSalida);
    insertCabeceraReq.input("montoAplicado", sql.Decimal(18, 2), montoAplicado);
    insertCabeceraReq.input("idMovil", sql.BigInt, idMovil);
    insertCabeceraReq.input("idTarifa", sql.BigInt, idTarifa);
    insertCabeceraReq.input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones));
    insertCabeceraReq.input("idUsuarioCreador", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_usuario_creador));
    insertCabeceraReq.input("idTipoFlete", sql.BigInt, idTipoFlete);
    insertCabeceraReq.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("idCentroCostoFinal", sql.BigInt, idCentroCostoFinal);

    const cabeceraResult = await insertCabeceraReq.query(`
      INSERT INTO [cfl].[CFL_cabecera_flete] (
        [id_detalle_viaje],
        [id_folio],
        [sap_numero_entrega_sugerido],
        [sap_codigo_tipo_flete_sugerido],
        [sap_centro_costo_sugerido],
        [sap_cuenta_mayor_sugerida],
        [cuenta_mayor_final],
        [tipo_movimiento],
        [estado],
        [fecha_salida],
        [hora_salida],
        [monto_aplicado],
        [id_movil],
        [id_tarifa],
        [observaciones],
        [id_usuario_creador],
        [id_tipo_flete],
        [created_at],
        [updated_at],
        [id_centro_costo_final]
      )
      OUTPUT INSERTED.id_cabecera_flete
      VALUES (
        @idDetalleViaje,
        @idFolio,
        @sapNumeroEntrega,
        @sapCodigoTipoFleteSug,
        @sapCentroCostoSug,
        @sapCuentaMayorSug,
        @cuentaMayorFinal,
        @tipoMovimiento,
        @estado,
        @fechaSalida,
        CAST(@horaSalida AS TIME),
        @montoAplicado,
        @idMovil,
        @idTarifa,
        @observaciones,
        @idUsuarioCreador,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCentroCostoFinal
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].id_cabecera_flete;

    await new sql.Request(transaction)
      .input("idCabeceraFlete", sql.BigInt, idCabeceraFlete)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .input("origenDatos", sql.VarChar(10), (toNullableTrimmedString(entrega.source_system) || "SAP").slice(0, 10))
      .input("tipoRelacion", sql.VarChar(20), "PRINCIPAL")
      .input("createdAt", sql.DateTime2(0), now)
      .query(`
        INSERT INTO [cfl].[CFL_flete_sap_entrega] (
          [id_cabecera_flete],
          [id_sap_entrega],
          [origen_datos],
          [tipo_relacion],
          [created_at]
        )
        VALUES (
          @idCabeceraFlete,
          @idSapEntrega,
          @origenDatos,
          @tipoRelacion,
          @createdAt
        );
      `);

    for (const detalle of detallesIn) {
      const material = toNullableTrimmedString(detalle.material);
      const descripcion = toNullableTrimmedString(detalle.descripcion);
      const unidad = toNullableTrimmedString(detalle.unidad);
      const cantidad = detalle.cantidad === null || detalle.cantidad === undefined || detalle.cantidad === "" ? null : Number(detalle.cantidad);
      const peso = detalle.peso === null || detalle.peso === undefined || detalle.peso === "" ? null : Number(detalle.peso);
      const idEspecie = parseOptionalBigInt(detalle.id_especie);

      await new sql.Request(transaction)
        .input("idCabeceraFlete", sql.BigInt, idCabeceraFlete)
        .input("idEspecie", sql.BigInt, idEspecie)
        .input("material", sql.VarChar(50), material)
        .input("descripcion", sql.VarChar(100), descripcion)
        .input("cantidad", sql.Decimal(12, 2), Number.isFinite(cantidad) ? cantidad : null)
        .input("unidad", sql.Char(3), unidad ? unidad.slice(0, 3) : null)
        .input("peso", sql.Decimal(15, 3), Number.isFinite(peso) ? peso : null)
        .input("createdAt", sql.DateTime2(0), now)
        .query(`
          INSERT INTO [cfl].[CFL_detalle_flete] (
            [id_cabecera_flete],
            [id_especie],
            [material],
            [descripcion],
            [cantidad],
            [unidad],
            [peso],
            [created_at]
          )
          VALUES (
            @idCabeceraFlete,
            @idEspecie,
            @material,
            @descripcion,
            @cantidad,
            @unidad,
            @peso,
            @createdAt
          );
        `);
    }

    await transaction.commit();

    res.status(201).json({
      message: "Cabecera y detalle creados",
      data: {
        id_cabecera_flete: idCabeceraFlete,
        id_sap_entrega: idSapEntrega,
      },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }
    next(error);
  }
});

router.post("/fletes/no-ingresados/:id_sap_entrega/ingresar", async (req, res, next) => {
  const idSapEntrega = Number(req.params.id_sap_entrega);
  if (!Number.isInteger(idSapEntrega) || idSapEntrega <= 0) {
    res.status(400).json({ error: "id_sap_entrega invalido" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();

    const deliveryRequest = new sql.Request(transaction);
    deliveryRequest.input("idSapEntrega", sql.BigInt, idSapEntrega);

    const deliveryResult = await deliveryRequest.query(`
      SELECT TOP 1
        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system,
        lk.sap_codigo_tipo_flete,
        lk.sap_centro_costo,
        lk.sap_cuenta_mayor,
        CONVERT(VARCHAR(10), lk.sap_fecha_salida, 23) AS sap_fecha_salida_iso,
        CONVERT(VARCHAR(8), lk.sap_hora_salida, 108) AS sap_hora_salida_iso
      FROM [cfl].[CFL_sap_entrega] e
      LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
        ON lk.sap_numero_entrega = e.sap_numero_entrega
       AND lk.source_system = e.source_system
      WHERE e.id_sap_entrega = @idSapEntrega;
    `);

    const delivery = deliveryResult.recordset[0];
    if (!delivery) {
      await transaction.rollback();
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const existsRequest = new sql.Request(transaction);
    existsRequest.input("idSapEntrega", sql.BigInt, idSapEntrega);
    const existsResult = await existsRequest.query(`
      SELECT TOP 1 1 AS already_linked
      FROM [cfl].[CFL_flete_sap_entrega]
      WHERE id_sap_entrega = @idSapEntrega;
    `);

    if (existsResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({
        error: "La entrega SAP ya se encuentra asociada a una cabecera de flete",
      });
      return;
    }

    const sapTipoFlete = toNullableTrimmedString(delivery.sap_codigo_tipo_flete);
    if (!sapTipoFlete) {
      await transaction.rollback();
      res.status(422).json({
        error: "La entrega no tiene sap_codigo_tipo_flete y no se puede crear la cabecera",
      });
      return;
    }

    const tipoFleteRequest = new sql.Request(transaction);
    tipoFleteRequest.input("sapCodigo", sql.VarChar(20), sapTipoFlete);
    const tipoFleteResult = await tipoFleteRequest.query(`
      SELECT TOP 1 id_tipo_flete, id_centro_costo
      FROM [cfl].[CFL_tipo_flete]
      WHERE sap_codigo = @sapCodigo
      ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, id_tipo_flete ASC;
    `);

    const tipoFlete = tipoFleteResult.recordset[0];
    if (!tipoFlete) {
      await transaction.rollback();
      res.status(422).json({
        error: `No existe un tipo de flete configurado para sap_codigo_tipo_flete=${sapTipoFlete}`,
      });
      return;
    }

    const sapCentroCosto = toNullableTrimmedString(delivery.sap_centro_costo);
    let idCentroCostoFinal = null;

    if (sapCentroCosto) {
      const centroRequest = new sql.Request(transaction);
      centroRequest.input("sapCentroCosto", sql.VarChar(20), sapCentroCosto);
      const centroResult = await centroRequest.query(`
        SELECT TOP 1 id_centro_costo
        FROM [cfl].[CFL_centro_costo]
        WHERE sap_codigo = @sapCentroCosto
        ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, id_centro_costo ASC;
      `);

      idCentroCostoFinal = centroResult.recordset[0]?.id_centro_costo || null;
    }

    if (!idCentroCostoFinal) {
      idCentroCostoFinal = tipoFlete.id_centro_costo || null;
    }

    if (!idCentroCostoFinal) {
      await transaction.rollback();
      res.status(422).json({
        error: "No se pudo resolver id_centro_costo_final para la cabecera de flete",
      });
      return;
    }

    const fechaSalida = delivery.sap_fecha_salida_iso || now.toISOString().slice(0, 10);
    const horaSalida = delivery.sap_hora_salida_iso || now.toISOString().slice(11, 19);
    const sapCuentaMayor = toNullableTrimmedString(delivery.sap_cuenta_mayor);

    const insertCabeceraRequest = new sql.Request(transaction);
    insertCabeceraRequest.input("sapNumeroEntrega", sql.VarChar(20), delivery.sap_numero_entrega);
    insertCabeceraRequest.input("sapCodigoTipoFleteSugerido", sql.Char(4), sapTipoFlete.slice(0, 4));
    insertCabeceraRequest.input("sapCentroCostoSugerido", sql.Char(10), sapCentroCosto ? sapCentroCosto.slice(0, 10) : null);
    insertCabeceraRequest.input("sapCuentaMayorSugerida", sql.Char(10), sapCuentaMayor ? sapCuentaMayor.slice(0, 10) : null);
    insertCabeceraRequest.input("cuentaMayorFinal", sql.Char(10), sapCuentaMayor ? sapCuentaMayor.slice(0, 10) : null);
    insertCabeceraRequest.input("tipoMovimiento", sql.VarChar(4), "PUSH");
    insertCabeceraRequest.input("estado", sql.VarChar(20), LIFECYCLE_STATUS.EN_REVISION);
    insertCabeceraRequest.input("fechaSalida", sql.Date, fechaSalida);
    insertCabeceraRequest.input("horaSalida", sql.VarChar(8), horaSalida);
    insertCabeceraRequest.input("montoAplicado", sql.Decimal(18, 2), 0);
    insertCabeceraRequest.input("idTipoFlete", sql.BigInt, tipoFlete.id_tipo_flete);
    insertCabeceraRequest.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraRequest.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraRequest.input("idCentroCostoFinal", sql.BigInt, idCentroCostoFinal);

    const cabeceraResult = await insertCabeceraRequest.query(`
      INSERT INTO [cfl].[CFL_cabecera_flete] (
        [sap_numero_entrega_sugerido],
        [sap_codigo_tipo_flete_sugerido],
        [sap_centro_costo_sugerido],
        [sap_cuenta_mayor_sugerida],
        [cuenta_mayor_final],
        [tipo_movimiento],
        [estado],
        [fecha_salida],
        [hora_salida],
        [monto_aplicado],
        [id_tipo_flete],
        [created_at],
        [updated_at],
        [id_centro_costo_final]
      )
      OUTPUT INSERTED.id_cabecera_flete
      VALUES (
        @sapNumeroEntrega,
        @sapCodigoTipoFleteSugerido,
        @sapCentroCostoSugerido,
        @sapCuentaMayorSugerida,
        @cuentaMayorFinal,
        @tipoMovimiento,
        @estado,
        @fechaSalida,
        CAST(@horaSalida AS TIME),
        @montoAplicado,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCentroCostoFinal
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].id_cabecera_flete;

    const insertBridgeRequest = new sql.Request(transaction);
    insertBridgeRequest.input("idCabeceraFlete", sql.BigInt, idCabeceraFlete);
    insertBridgeRequest.input("idSapEntrega", sql.BigInt, idSapEntrega);
    insertBridgeRequest.input(
      "origenDatos",
      sql.VarChar(10),
      (toNullableTrimmedString(delivery.source_system) || "SAP").slice(0, 10)
    );
    insertBridgeRequest.input("tipoRelacion", sql.VarChar(20), "PRINCIPAL");
    insertBridgeRequest.input("createdAt", sql.DateTime2(0), now);

    const bridgeResult = await insertBridgeRequest.query(`
      INSERT INTO [cfl].[CFL_flete_sap_entrega] (
        [id_cabecera_flete],
        [id_sap_entrega],
        [origen_datos],
        [tipo_relacion],
        [created_at]
      )
      OUTPUT INSERTED.id_flete_sap_entrega
      VALUES (
        @idCabeceraFlete,
        @idSapEntrega,
        @origenDatos,
        @tipoRelacion,
        @createdAt
      );
    `);

    const idFleteSapEntrega = bridgeResult.recordset[0].id_flete_sap_entrega;

    await transaction.commit();

    res.status(201).json({
      message: "Cabecera de flete creada y entrega SAP asociada",
      data: {
        id_cabecera_flete: idCabeceraFlete,
        id_flete_sap_entrega: idFleteSapEntrega,
        id_sap_entrega: idSapEntrega,
      },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }
    next(error);
  }
});

module.exports = {
  dashboardRouter: router,
};

