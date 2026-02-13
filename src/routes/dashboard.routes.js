const express = require("express");
const { getPool, sql } = require("../db");

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

    const sql = `
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
        -- Semantica: candidatos (aun no existe cabecera) => siempre Detectado.
        estado = 'Detectado',
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

    const result = await request.query(sql);
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

  try {
    const pool = await getPool();
    const request = pool.request();
    request.input("offset", offset);
    request.input("pageSize", pageSize);

    const sql = `
      SELECT
        cf.id_cabecera_flete,
        cf.id_folio,
        cf.estado,
        cf.tipo_movimiento,
        cf.fecha_salida,
        cf.hora_salida,
        cf.monto_aplicado,
        cf.observaciones,
        cf.id_tipo_flete,
        tf.nombre AS tipo_flete_nombre,
        cf.id_centro_costo_final,
        cc.nombre AS centro_costo_final_nombre,
        cf.created_at,
        cf.updated_at,

        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system,
        sap_guia_remision = NULLIF(LTRIM(RTRIM(lk.sap_guia_remision)), ''),
        sap_empresa_transporte = NULLIF(LTRIM(RTRIM(lk.sap_empresa_transporte)), ''),
        sap_nombre_chofer = NULLIF(LTRIM(RTRIM(lk.sap_nombre_chofer)), ''),
        sap_patente = NULLIF(LTRIM(RTRIM(lk.sap_patente)), ''),
        sap_carro = NULLIF(LTRIM(RTRIM(lk.sap_carro)), '')
      INTO #base
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo_final
      LEFT JOIN [cfl].[CFL_flete_sap_entrega] fe ON fe.id_cabecera_flete = cf.id_cabecera_flete
      LEFT JOIN [cfl].[CFL_sap_entrega] e ON e.id_sap_entrega = fe.id_sap_entrega
      LEFT JOIN [cfl].[vw_cfl_sap_likp_current] lk
        ON lk.sap_numero_entrega = e.sap_numero_entrega
       AND lk.source_system = e.source_system
      WHERE cf.estado = 'Completo'
        AND (cf.id_folio IS NULL OR cf.id_folio = 0);

      SELECT total = COUNT_BIG(1) FROM #base;

      SELECT *
      FROM #base
      ORDER BY updated_at DESC, id_cabecera_flete DESC
      OFFSET @offset ROWS FETCH NEXT @pageSize ROWS ONLY;

      DROP TABLE #base;
    `;

    const result = await request.query(sql);
    const total = Number(result.recordsets[0][0].total);

    res.json({
      data: result.recordsets[1],
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
        SELECT TOP 1 id_folio, bloqueado, estado
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

    const invalid = [];

    for (const idCabecera of cabeceraIds) {
      const rowResult = await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .query(`
          SELECT TOP 1 id_cabecera_flete, estado, id_folio
          FROM [cfl].[CFL_cabecera_flete]
          WHERE id_cabecera_flete = @idCabecera;
        `);

      const row = rowResult.recordset[0];
      if (!row) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "No existe" });
        continue;
      }
      const folioValue = row.id_folio === null || row.id_folio === undefined ? 0 : Number(row.id_folio);
      if (row.estado !== "Completo") {
        invalid.push({ id_cabecera_flete: idCabecera, reason: `Estado invalido: ${row.estado}` });
        continue;
      }
      if (folioValue !== 0) {
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
        .input("updatedAt", sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CFL_cabecera_flete]
          SET id_folio = @idFolio,
              estado = 'Validado',
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
  const tipoMovimiento = toNullableTrimmedString(cabeceraIn.tipo_movimiento) || "PUSH";
  const estado = toNullableTrimmedString(cabeceraIn.estado) || "Detectado";
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoFinal) {
    res.status(400).json({ error: "Falta id_centro_costo_final" });
    return;
  }
  if (!["PUSH", "PULL"].includes(tipoMovimiento)) {
    res.status(400).json({ error: "tipo_movimiento invalido (PUSH/PULL)" });
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
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_detalle_viaje));
    insertCabeceraReq.input("idFolio", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_folio));
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
    insertCabeceraReq.input("idMovil", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_movil));
    insertCabeceraReq.input("idTarifa", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_tarifa));
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
    insertCabeceraRequest.input("estado", sql.VarChar(20), "Detectado");
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

