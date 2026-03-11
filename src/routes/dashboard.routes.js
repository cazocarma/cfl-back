const express = require("express");
const { getPool, sql } = require("../db");
const { hasAnyPermission, resolveAuthContext } = require("../authz");
const {
  clamp,
  parsePositiveInt,
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  normalizeTipoMovimiento,
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
  resolveMovilId,
  resolveFolioForLifecycle,
} = require("../helpers");

const router = express.Router();

function buildMissingDeliveriesQuery(filters) {
  const whereClauses = [
    "NOT EXISTS (SELECT 1 FROM [cfl].[FleteSapEntrega] fe WHERE fe.IdSapEntrega = c.IdSapEntrega)",
    "NOT EXISTS (SELECT 1 FROM [cfl].[SapEntregaDescarte] sd WHERE sd.IdSapEntrega = c.IdSapEntrega AND sd.Activo = 1)",
  ];

  if (filters.search) {
    // Soporta búsqueda por entrega, guia, empresa transporte, chofer y patente.
    whereClauses.push(`(
      c.SapNumeroEntrega LIKE @search
      OR c.SapGuiaRemision LIKE @search
      OR c.SapEmpresaTransporte LIKE @search
      OR c.SapNombreChofer LIKE @search
      OR c.SapPatente LIKE @search
    )`);
  }
  if (filters.sourceSystem) {
    whereClauses.push("c.SistemaFuente = @sourceSystem");
  }
  if (filters.fechaDesde) {
    whereClauses.push("c.SapFechaSalida >= @fechaDesde");
  }
  if (filters.fechaHasta) {
    whereClauses.push("c.SapFechaSalida <= @fechaHasta");
  }
  if (filters.estado) {
    whereClauses.push("c.Estado = @estado");
  }

  return `
    FROM #candidates c
    WHERE ${whereClauses.join(" AND ")}
  `;
}

function hasAnyRole(auth, roles = []) {
  if (!auth || !Array.isArray(auth.roleNames)) {
    return false;
  }

  const roleSet = new Set(auth.roleNames.map((role) => String(role || "").trim().toLowerCase()));
  return roles.some((role) => roleSet.has(String(role || "").trim().toLowerCase()));
}

async function resolveProductorIdByDestinatario(transaction, explicitIdProductor, sapDestinatario) {
  if (explicitIdProductor) return explicitIdProductor;

  const destinatario = toNullableTrimmedString(sapDestinatario);
  if (!destinatario) return null;

  const result = await new sql.Request(transaction)
    .input("destinatario", sql.NVarChar(40), destinatario)
    .query(`
      SELECT TOP 1 IdProductor
      FROM [cfl].[Productor]
      WHERE
        NULLIF(LTRIM(RTRIM(CodigoProveedor)), '') = @destinatario
        OR NULLIF(LTRIM(RTRIM(Rut)), '') = @destinatario
      ORDER BY
        CASE WHEN Activo = 1 THEN 0 ELSE 1 END,
        CASE WHEN NULLIF(LTRIM(RTRIM(CodigoProveedor)), '') = @destinatario THEN 0 ELSE 1 END,
        IdProductor ASC;
    `);

  return Number(result.recordset[0]?.IdProductor || 0) || null;
}

router.get("/resumen", async (req, res, next) => {
  try {
    const pool = await getPool();
    const query = `
      SELECT
        total_entregas = (SELECT COUNT_BIG(1) FROM [cfl].[SapEntrega]),
        total_asociadas = (SELECT COUNT_BIG(DISTINCT IdSapEntrega) FROM [cfl].[FleteSapEntrega]),
        total_sin_cabecera = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[SapEntrega] e
          WHERE NOT EXISTS (
            SELECT 1
            FROM [cfl].[FleteSapEntrega] fe
            WHERE fe.IdSapEntrega = e.IdSapEntrega
          )
          AND NOT EXISTS (
            SELECT 1
            FROM [cfl].[SapEntregaDescarte] sd
            WHERE sd.IdSapEntrega = e.IdSapEntrega
              AND sd.Activo = 1
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
      SELECT
        SistemaFuente,
        SapNumeroEntrega,
        SapPosicion,
        SapCantidadEntregada
      INTO #lips_current_pref
      FROM [cfl].[VW_LipsActual];

      SELECT
        e.IdSapEntrega,
        e.SapNumeroEntrega,
        e.SistemaFuente,

        SapGuiaRemision = NULLIF(LTRIM(RTRIM(lk.SapGuiaRemision)), ''),
        SapCodigoTipoFlete = NULLIF(LTRIM(RTRIM(lk.SapCodigoTipoFlete)), ''),
        SapCentroCosto = NULLIF(LTRIM(RTRIM(lk.SapCentroCosto)), ''),
        SapCuentaMayor = NULLIF(LTRIM(RTRIM(lk.SapCuentaMayor)), ''),
        SapDestinatario = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), ''),
        IdProductor = prod.IdProductor,
        ProductorCodigoProveedor = prod.CodigoProveedor,
        ProductorRut = prod.Rut,
        ProductorNombre = prod.Nombre,
        SapFechaSalida = lk.SapFechaSalida,
        SapHoraSalida = CONVERT(VARCHAR(8), lk.SapHoraSalida, 108),
        SapEmpresaTransporte = NULLIF(LTRIM(RTRIM(lk.SapEmpresaTransporte)), ''),
        SapNombreChofer = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), ''),
        SapPatente = NULLIF(LTRIM(RTRIM(lk.SapPatente)), ''),
        SapCarro = NULLIF(LTRIM(RTRIM(lk.SapCarro)), ''),
        SapPesoTotal = lk.SapPesoTotal,
        SapPesoNeto = lk.SapPesoNeto,

        posiciones_total = COUNT(lp.SapPosicion),
        cantidad_entregada_total = COALESCE(SUM(CAST(lp.SapCantidadEntregada AS DECIMAL(18,3))), 0),

        IdTipoFlete = tf.IdTipoFlete,
        tipo_flete_nombre = tf.Nombre,
        IdCentroCosto = COALESCE(cc_sap.IdCentroCosto, tf.IdCentroCosto),
        -- Semantica: candidatos (aun no existe cabecera) => siempre DETECTADO.
        Estado = 'DETECTADO',
        puede_ingresar = CAST(
          CASE
            WHEN tf.IdTipoFlete IS NULL THEN 0
            WHEN COALESCE(cc_sap.IdCentroCosto, tf.IdCentroCosto) IS NULL THEN 0
            WHEN lk.SapFechaSalida IS NULL THEN 0
            WHEN lk.SapHoraSalida IS NULL THEN 0
            ELSE 1
          END AS BIT
        ),
        motivo_no_ingreso = CASE
          WHEN tf.IdTipoFlete IS NULL THEN CONCAT(
            'Falta configurar Tipo de Flete para SapCodigoTipoFlete=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.SapCodigoTipoFlete)), ''), '(NULL)')
          )
          WHEN COALESCE(cc_sap.IdCentroCosto, tf.IdCentroCosto) IS NULL THEN CONCAT(
            'No se pudo resolver Centro de Costo (SapCentroCosto=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.SapCentroCosto)), ''), '(NULL)'),
            ')'
          )
          WHEN lk.SapFechaSalida IS NULL THEN 'Falta SapFechaSalida'
          WHEN lk.SapHoraSalida IS NULL THEN 'Falta SapHoraSalida'
          ELSE NULL
        END,

        e.FechaUltimaVista,
        e.FechaActualizacion
      INTO #candidates
      FROM [cfl].[SapEntrega] e
      LEFT JOIN [cfl].[VW_LikpActual] lk
        ON lk.SapNumeroEntrega = e.SapNumeroEntrega
       AND lk.SistemaFuente = e.SistemaFuente
      LEFT JOIN #lips_current_pref lp
        ON lp.SapNumeroEntrega = e.SapNumeroEntrega
       AND lp.SistemaFuente = e.SistemaFuente
      LEFT JOIN [cfl].[TipoFlete] tf
        ON tf.SapCodigo = lk.SapCodigoTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc_sap
        ON cc_sap.SapCodigo = lk.SapCentroCosto
      OUTER APPLY (
        SELECT TOP 1
          p.IdProductor,
          p.CodigoProveedor,
          p.Rut,
          p.Nombre
        FROM [cfl].[Productor] p
        WHERE
          NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
          OR NULLIF(LTRIM(RTRIM(p.Rut)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
        ORDER BY
          CASE WHEN p.Activo = 1 THEN 0 ELSE 1 END,
          CASE WHEN NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '') THEN 0 ELSE 1 END,
          p.IdProductor ASC
      ) prod
      GROUP BY
        e.IdSapEntrega,
        e.SapNumeroEntrega,
        e.SistemaFuente,
        lk.SapGuiaRemision,
        lk.SapCodigoTipoFlete,
        lk.SapCentroCosto,
        lk.SapCuentaMayor,
        lk.SapDestinatario,
        prod.IdProductor,
        prod.CodigoProveedor,
        prod.Rut,
        prod.Nombre,
        lk.SapFechaSalida,
        lk.SapHoraSalida,
        lk.SapEmpresaTransporte,
        lk.SapNombreChofer,
        lk.SapPatente,
        lk.SapCarro,
        lk.SapPesoTotal,
        lk.SapPesoNeto,
        tf.IdTipoFlete,
        tf.Nombre,
        tf.IdCentroCosto,
        cc_sap.IdCentroCosto,
        e.FechaUltimaVista,
        e.FechaActualizacion;

      SELECT total = COUNT_BIG(1)
      ${baseQuery};

      SELECT
        IdSapEntrega,
        SapNumeroEntrega,
        SistemaFuente,
        SapGuiaRemision,
        SapCodigoTipoFlete,
        SapCentroCosto,
        SapCuentaMayor,
        SapDestinatario,
        IdProductor,
        ProductorCodigoProveedor,
        ProductorRut,
        ProductorNombre,
        SapFechaSalida,
        SapHoraSalida,
        SapEmpresaTransporte,
        SapNombreChofer,
        SapPatente,
        SapCarro,
        SapPesoTotal,
        SapPesoNeto,
        posiciones_total,
        cantidad_entregada_total,
        IdTipoFlete,
        tipo_flete_nombre,
        IdCentroCosto,
        Estado,
        puede_ingresar,
        motivo_no_ingreso,
        FechaUltimaVista,
        FechaActualizacion
      ${baseQuery}
      ORDER BY
        COALESCE(CAST(SapFechaSalida AS DATETIME2(0)), FechaActualizacion) DESC,
        IdSapEntrega DESC
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
        e.IdSapEntrega,
        e.SapNumeroEntrega,
        e.SistemaFuente,

        SapGuiaRemision = NULLIF(LTRIM(RTRIM(lk.SapGuiaRemision)), ''),
        SapCodigoTipoFlete = NULLIF(LTRIM(RTRIM(lk.SapCodigoTipoFlete)), ''),
        SapCentroCosto = NULLIF(LTRIM(RTRIM(lk.SapCentroCosto)), ''),
        SapCuentaMayor = NULLIF(LTRIM(RTRIM(lk.SapCuentaMayor)), ''),
        SapDestinatario = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), ''),
        IdProductor = prod.IdProductor,
        ProductorCodigoProveedor = prod.CodigoProveedor,
        ProductorRut = prod.Rut,
        ProductorNombre = prod.Nombre,
        ProductorEmail = prod.Email,
        SapFechaSalida = lk.SapFechaSalida,
        SapHoraSalida = CONVERT(VARCHAR(8), lk.SapHoraSalida, 108),
        SapEmpresaTransporte = NULLIF(LTRIM(RTRIM(lk.SapEmpresaTransporte)), ''),
        SapNombreChofer = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), ''),
        SapPatente = NULLIF(LTRIM(RTRIM(lk.SapPatente)), ''),
        SapCarro = NULLIF(LTRIM(RTRIM(lk.SapCarro)), ''),
        SapPesoTotal = lk.SapPesoTotal,
        SapPesoNeto = lk.SapPesoNeto,
        e.FechaUltimaVista,
        e.FechaActualizacion
      FROM [cfl].[SapEntrega] e
      LEFT JOIN [cfl].[VW_LikpActual] lk
        ON lk.SapNumeroEntrega = e.SapNumeroEntrega
       AND lk.SistemaFuente = e.SistemaFuente
      OUTER APPLY (
        SELECT TOP 1
          p.IdProductor,
          p.CodigoProveedor,
          p.Rut,
          p.Nombre,
          p.Email
        FROM [cfl].[Productor] p
        WHERE
          NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
          OR NULLIF(LTRIM(RTRIM(p.Rut)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
        ORDER BY
          CASE WHEN p.Activo = 1 THEN 0 ELSE 1 END,
          CASE WHEN NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '') THEN 0 ELSE 1 END,
          p.IdProductor ASC
      ) prod
      WHERE e.IdSapEntrega = @idSapEntrega;
    `);

    const cabecera = headerResult.recordset[0];
    if (!cabecera) {
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const positionsResult = await pool
      .request()
      .input("sourceSystem", sql.VarChar(50), cabecera.SistemaFuente)
      .input("sapNumeroEntrega", sql.VarChar(20), cabecera.SapNumeroEntrega)
      .query(`
        SELECT
          SapPosicion,
          SapMaterial,
          SapDenominacionMaterial,
          SapCantidadEntregada,
          SapUnidadPeso,
          SapCentro,
          SapAlmacen,
          SapPosicionSuperior,
          SapLote
        FROM [cfl].[VW_LipsActual]
        WHERE SistemaFuente = @sourceSystem
          AND SapNumeroEntrega = @sapNumeroEntrega
        ORDER BY SapPosicion ASC;
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
  const searchRaw = toNullableTrimmedString(req.query.search);
  const fechaDesdeRaw = toNullableTrimmedString(req.query.fecha_desde);
  const fechaHastaRaw = toNullableTrimmedString(req.query.fecha_hasta);

  try {
    const pool = await getPool();
    const request = pool.request();
    request.input("offset", offset);
    request.input("pageSize", pageSize);
    request.input("estadoFiltro", sql.VarChar(30), estadoFiltro ? estadoFiltro.toUpperCase() : null);
    request.input("search", sql.VarChar(100), searchRaw ? `%${searchRaw}%` : null);
    request.input("fechaDesde", sql.VarChar(10), fechaDesdeRaw || null);
    request.input("fechaHasta", sql.VarChar(10), fechaHastaRaw || null);

    const querySql = `
      ;WITH detalle_counts AS (
        SELECT
          IdCabeceraFlete,
          total_detalles = COUNT_BIG(1)
        FROM [cfl].[DetalleFlete]
        GROUP BY IdCabeceraFlete
      ),
      base AS (
      SELECT
        cf.IdCabeceraFlete,
        cf.IdFolio,
        FolioNumero = fol.FolioNumero,
        cf.Estado AS estado_original,
        cf.TipoMovimiento,
        cf.FechaSalida,
        cf.HoraSalida,
        cf.MontoAplicado,
        cf.Observaciones,
        cf.IdTipoFlete,
        cf.IdDetalleViaje,
        cf.IdMovil,
        cf.IdTarifa,
        cf.IdCuentaMayor,
        IdProductor = COALESCE(cf.IdProductor, prod_sap.IdProductor),
        ProductorCodigoProveedor = COALESCE(prod_cf.CodigoProveedor, prod_sap.CodigoProveedor),
        ProductorRut = COALESCE(prod_cf.Rut, prod_sap.Rut),
        ProductorNombre = COALESCE(prod_cf.Nombre, prod_sap.Nombre),
        ProductorEmail = COALESCE(prod_cf.Email, prod_sap.Email),
        cf.SentidoFlete,
        tf.Nombre AS tipo_flete_nombre,
        cf.IdCentroCosto,
        cc.Nombre AS centro_costo_nombre,
        det.total_detalles,
        cf.FechaCreacion,
        cf.FechaActualizacion,
        lk.FechaCreacion AS sap_updated_at,

        e.IdSapEntrega,
        SapNumeroEntrega = NULLIF(LTRIM(RTRIM(e.SapNumeroEntrega)), ''),
        e.SistemaFuente,
        SapGuiaRemision = NULLIF(LTRIM(RTRIM(lk.SapGuiaRemision)), ''),
        cf.GuiaRemision,
        cf.NumeroEntrega,
        -- numero_guia: prioridad confirmado > snapshot SAP en cabecera > live SAP > número entrega
        numero_guia = COALESCE(
          NULLIF(LTRIM(RTRIM(cf.GuiaRemision)), ''),
          NULLIF(LTRIM(RTRIM(cf.SapGuiaRemision)), ''),
          NULLIF(LTRIM(RTRIM(lk.SapGuiaRemision)), ''),
          NULLIF(LTRIM(RTRIM(cf.NumeroEntrega)), ''),
          NULLIF(LTRIM(RTRIM(cf.SapNumeroEntrega)), ''),
          NULLIF(LTRIM(RTRIM(e.SapNumeroEntrega)), '')
        ),
        SapEmpresaTransporte = NULLIF(LTRIM(RTRIM(lk.SapEmpresaTransporte)), ''),
        SapNombreChofer = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), ''),
        SapPatente = NULLIF(LTRIM(RTRIM(lk.SapPatente)), ''),
        SapCarro = NULLIF(LTRIM(RTRIM(lk.SapCarro)), ''),
        SapDestinatario = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), ''),
        IdRuta = r.IdRuta,
        ruta_nombre = NULLIF(LTRIM(RTRIM(r.NombreRuta)), ''),
        ruta_origen_nombre = NULLIF(LTRIM(RTRIM(no.Nombre)), ''),
        ruta_destino_nombre = NULLIF(LTRIM(RTRIM(nd.Nombre)), ''),
        movil_empresa = NULLIF(LTRIM(RTRIM(et.RazonSocial)), ''),
        movil_chofer_rut = NULLIF(LTRIM(RTRIM(ch.SapIdFiscal)), ''),
        movil_chofer_nombre = NULLIF(LTRIM(RTRIM(ch.SapNombre)), ''),
        movil_tipo_camion = NULLIF(LTRIM(RTRIM(tc.Nombre)), ''),
        movil_patente = NULLIF(LTRIM(RTRIM(cam.SapPatente)), ''),
        estado_lifecycle = CASE
          WHEN UPPER(ISNULL(cf.Estado, '')) = 'ANULADO' THEN 'ANULADO'
          WHEN UPPER(ISNULL(cf.Estado, '')) = 'FACTURADO' THEN 'FACTURADO'
          WHEN COALESCE(cf.IdFolio, 0) > 0
            AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
            THEN 'ASIGNADO_FOLIO'
          WHEN lk.FechaCreacion IS NOT NULL AND cf.FechaActualizacion IS NOT NULL AND lk.FechaCreacion > cf.FechaActualizacion THEN 'ACTUALIZADO'
          WHEN cf.IdTipoFlete IS NOT NULL
            AND cf.IdCentroCosto IS NOT NULL
            AND cf.IdDetalleViaje IS NOT NULL
            AND cf.IdMovil IS NOT NULL
            AND cf.IdTarifa IS NOT NULL
            AND COALESCE(det.total_detalles, 0) > 0 THEN 'COMPLETADO'
          ELSE 'EN_REVISION'
        END
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[Folio] fol ON fol.IdFolio = cf.IdFolio
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] et ON et.IdEmpresa = mv.IdEmpresaTransporte
      LEFT JOIN [cfl].[Chofer] ch ON ch.IdChofer = mv.IdChofer
      LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
      LEFT JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = cam.IdTipoCamion
      LEFT JOIN [cfl].[Productor] prod_cf ON prod_cf.IdProductor = cf.IdProductor
      LEFT JOIN [cfl].[Tarifa] tfa ON tfa.IdTarifa = cf.IdTarifa
      LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tfa.IdRuta
      LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
      LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
      LEFT JOIN detalle_counts det ON det.IdCabeceraFlete = cf.IdCabeceraFlete
      LEFT JOIN [cfl].[FleteSapEntrega] fe ON fe.IdCabeceraFlete = cf.IdCabeceraFlete
      LEFT JOIN [cfl].[SapEntrega] e ON e.IdSapEntrega = fe.IdSapEntrega
      LEFT JOIN [cfl].[VW_LikpActual] lk
        ON lk.SapNumeroEntrega = e.SapNumeroEntrega
       AND lk.SistemaFuente = e.SistemaFuente
      OUTER APPLY (
        SELECT TOP 1
          p.IdProductor,
          p.CodigoProveedor,
          p.Rut,
          p.Nombre,
          p.Email
        FROM [cfl].[Productor] p
        WHERE
          NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
          OR NULLIF(LTRIM(RTRIM(p.Rut)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
        ORDER BY
          CASE WHEN p.Activo = 1 THEN 0 ELSE 1 END,
          CASE WHEN NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '') THEN 0 ELSE 1 END,
          p.IdProductor ASC
      ) prod_sap
      )

      SELECT
        total_rows = COUNT_BIG(1) OVER(),
        IdCabeceraFlete,
        IdFolio,
        FolioNumero,
        Estado = estado_lifecycle,
        estado_original,
        TipoMovimiento,
        FechaSalida,
        HoraSalida,
        MontoAplicado,
        Observaciones,
        IdTipoFlete,
        IdDetalleViaje,
        IdMovil,
        IdTarifa,
        IdCuentaMayor,
        IdProductor,
        ProductorCodigoProveedor,
        ProductorRut,
        ProductorNombre,
        ProductorEmail,
        SentidoFlete,
        tipo_flete_nombre,
        IdCentroCosto,
        centro_costo_nombre,
        total_detalles,
        FechaCreacion,
        FechaActualizacion,
        sap_updated_at,
        IdSapEntrega,
        SapNumeroEntrega,
        SistemaFuente,
        SapGuiaRemision,
        GuiaRemision,
        NumeroEntrega,
        numero_guia,
        SapEmpresaTransporte,
        SapNombreChofer,
        SapPatente,
        SapCarro,
        SapDestinatario,
        IdRuta,
        ruta_nombre,
        ruta_origen_nombre,
        ruta_destino_nombre,
        movil_empresa,
        movil_chofer_rut,
        movil_chofer_nombre,
        movil_tipo_camion,
        movil_patente
      FROM base
      WHERE
        (
          (@estadoFiltro IS NULL AND estado_lifecycle <> 'ANULADO')
          OR estado_lifecycle = @estadoFiltro
        )
        AND (
          @search IS NULL
          OR numero_guia LIKE @search
          OR movil_empresa LIKE @search
          OR movil_chofer_nombre LIKE @search
          OR movil_patente LIKE @search
          OR tipo_flete_nombre LIKE @search
          OR ProductorNombre LIKE @search
          OR ProductorCodigoProveedor LIKE @search
          OR ProductorRut LIKE @search
        )
        AND (@fechaDesde IS NULL OR FechaSalida >= CAST(@fechaDesde AS DATE))
        AND (@fechaHasta IS NULL OR FechaSalida <= CAST(@fechaHasta AS DATE))
      ORDER BY FechaActualizacion DESC, IdCabeceraFlete DESC
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
        SELECT TOP 1 IdFolio, Bloqueado, Estado, FolioNumero
        FROM [cfl].[Folio]
        WHERE IdFolio = @idFolio;
      `);

    const folio = folioResult.recordset[0];
    if (!folio) {
      await transaction.rollback();
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    if (folio.Bloqueado === true || folio.Bloqueado === 1) {
      await transaction.rollback();
      res.status(409).json({ error: "El folio esta bloqueado" });
      return;
    }

    const targetFolioNumero = String(folio.FolioNumero || "").trim();
    const targetFolioIsDefault = targetFolioNumero === "0";

    const invalid = [];

    for (const idCabecera of cabeceraIds) {
      const rowResult = await new sql.Request(transaction)
        .input("idCabecera", sql.BigInt, idCabecera)
        .query(`
          SELECT TOP 1
            cf.IdCabeceraFlete,
            cf.Estado,
            cf.IdFolio,
            FolioNumero = f.FolioNumero
          FROM [cfl].[CabeceraFlete] cf
          LEFT JOIN [cfl].[Folio] f ON f.IdFolio = cf.IdFolio
          WHERE cf.IdCabeceraFlete = @idCabecera;
        `);

      const row = rowResult.recordset[0];
      if (!row) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "No existe" });
        continue;
      }
      const folioNumeroActual = String(row.folio_numero || "").trim();
      const folioValue = row.IdFolio === null || row.IdFolio === undefined ? 0 : Number(row.IdFolio);
      const folioEsDefault = folioNumeroActual === "0";
      const normalizedEstado = normalizeLifecycleStatus(row.Estado);
      const estadoElegible = normalizedEstado === LIFECYCLE_STATUS.COMPLETADO
        || (folioEsDefault && normalizedEstado === LIFECYCLE_STATUS.ASIGNADO_FOLIO);
      if (!estadoElegible) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: `Estado invalido: ${row.Estado}` });
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
          UPDATE [cfl].[CabeceraFlete]
          SET IdFolio = @idFolio,
              Estado = @estadoAsignado,
              FechaActualizacion = @updatedAt
          WHERE IdCabeceraFlete = @idCabecera;
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
            cf.IdCabeceraFlete,
            cf.Estado,
            cf.IdFolio,
            cf.IdCentroCosto,
            cf.FechaSalida,
            FolioNumero = f.FolioNumero
          FROM [cfl].[CabeceraFlete] cf
          LEFT JOIN [cfl].[Folio] f ON f.IdFolio = cf.IdFolio
          WHERE cf.IdCabeceraFlete = @idCabecera;
        `);

      const row = rowResult.recordset[0];
      if (!row) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "No existe" });
        continue;
      }

      const folioNumeroActual = String(row.folio_numero || "").trim();
      const folioValue = row.IdFolio === null || row.IdFolio === undefined ? 0 : Number(row.IdFolio);
      const folioEsDefault = folioNumeroActual === "0";
      const normalizedEstado = normalizeLifecycleStatus(row.Estado);
      const estadoElegible = normalizedEstado === LIFECYCLE_STATUS.COMPLETADO
        || (folioEsDefault && normalizedEstado === LIFECYCLE_STATUS.ASIGNADO_FOLIO);

      if (!estadoElegible) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: `Estado invalido: ${row.Estado}` });
        continue;
      }
      if (folioValue !== 0 && !folioEsDefault) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "Ya tiene folio asignado" });
        continue;
      }

      const idCentroCosto = Number(row.IdCentroCosto || 0);
      if (!Number.isInteger(idCentroCosto) || idCentroCosto <= 0) {
        invalid.push({ id_cabecera_flete: idCabecera, reason: "Centro de costo invalido" });
        continue;
      }
      centerCostSet.add(idCentroCosto);

      if (row.FechaSalida) {
        salidaDates.push(new Date(row.FechaSalida));
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
      SELECT TOP 1 IdTemporada
      FROM [cfl].[Temporada]
      WHERE Activa = 1
      ORDER BY CASE WHEN ISNULL(Cerrada, 0) = 0 THEN 0 ELSE 1 END, FechaInicio DESC, IdTemporada DESC;
    `);

    const idTemporada = Number(temporadaResult.recordset[0]?.IdTemporada || 0);
    if (!Number.isInteger(idTemporada) || idTemporada <= 0) {
      await transaction.rollback();
      res.status(409).json({ error: "No existe una temporada activa para crear folio" });
      return;
    }

    // Bloquea la temporada para serializar la generacion del correlativo por temporada.
    const temporadaLockResult = await new sql.Request(transaction)
      .input("idTemporada", sql.BigInt, idTemporada)
      .query(`
        SELECT TOP 1 IdTemporada
        FROM [cfl].[Temporada] WITH (UPDLOCK, HOLDLOCK)
        WHERE IdTemporada = @idTemporada;
      `);

    if (!temporadaLockResult.recordset[0]) {
      await transaction.rollback();
      res.status(409).json({ error: "La temporada activa ya no existe para crear folio" });
      return;
    }

    const nextNumberResult = await new sql.Request(transaction)
      .input("idTemporada", sql.BigInt, idTemporada)
      .query(`
        SELECT
          next_num = COALESCE(MAX(TRY_CONVERT(BIGINT, NULLIF(LTRIM(RTRIM(FolioNumero)), ''))), 0) + 1
        FROM [cfl].[Folio] WITH (UPDLOCK, HOLDLOCK)
        WHERE IdTemporada = @idTemporada
          AND TRY_CONVERT(BIGINT, NULLIF(LTRIM(RTRIM(FolioNumero)), '')) IS NOT NULL;
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
        INSERT INTO [cfl].[Folio] (
          [IdCentroCosto],
          [IdTemporada],
          [FolioNumero],
          [PeriodoDesde],
          [PeriodoHasta],
          [Estado],
          [Bloqueado],
          [FechaCreacion],
          [FechaActualizacion]
        )
        OUTPUT INSERTED.IdFolio
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

    const idFolio = Number(createFolioResult.recordset[0]?.IdFolio || 0);
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
          UPDATE [cfl].[CabeceraFlete]
          SET IdFolio = @idFolio,
              Estado = @estadoAsignado,
              FechaActualizacion = @updatedAt
          WHERE IdCabeceraFlete = @idCabecera;
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

  const canAnularByRole = hasAnyRole(auth, ["autorizador", "administrador"]);
  if (!canAnularByRole && !hasAnyPermission(auth, ["fletes.anular", "mantenedores.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para anular fletes",
      role: auth?.primaryRole || null,
    });
    return;
  }

  const motivo = toNullableTrimmedString(req.body?.motivo);
  if (!motivo) {
    res.status(400).json({ error: "Debes ingresar un motivo para anular el flete" });
    return;
  }
  const motivoFinal = motivo.slice(0, 200);
  const idUsuarioActor = parseOptionalBigInt(req.jwtPayload?.id_usuario);
  if (!idUsuarioActor) {
    res.status(401).json({ error: "Token invalido: usuario no identificado" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const current = await new sql.Request(transaction).input("idCabecera", sql.BigInt, idCabecera).query(`
      SELECT TOP 1 IdCabeceraFlete, Estado
      FROM [cfl].[CabeceraFlete]
      WHERE IdCabeceraFlete = @idCabecera;
    `);

    const row = current.recordset[0];
    if (!row) {
      await transaction.rollback();
      res.status(404).json({ error: "Cabecera de flete no encontrada" });
      return;
    }

    const normalized = normalizeLifecycleStatus(row.Estado);
    if (normalized === LIFECYCLE_STATUS.FACTURADO) {
      await transaction.rollback();
      res.status(409).json({ error: "Un flete FACTURADO no se puede anular" });
      return;
    }

    const now = new Date();

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .input("estado", sql.VarChar(20), LIFECYCLE_STATUS.ANULADO)
      .input("updatedAt", sql.DateTime2(0), now)
      .input("idUsuario", sql.BigInt, idUsuarioActor)
      .input("motivo", sql.VarChar(200), motivoFinal)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET Estado = @estado,
            FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idCabecera;

        INSERT INTO [cfl].[FleteEstadoHistorial] (
          [IdCabeceraFlete],
          [Estado],
          [FechaHora],
          [IdUsuario],
          [Motivo]
        )
        VALUES (
          @idCabecera,
          @estado,
          @updatedAt,
          @idUsuario,
          @motivo
        );
      `);

    await transaction.commit();

    res.json({
      message: "Flete anulado",
      data: {
        id_cabecera_flete: idCabecera,
        estado: LIFECYCLE_STATUS.ANULADO,
        motivo: motivoFinal,
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

router.post("/fletes/no-ingresados/:id_sap_entrega/descartar", async (req, res, next) => {
  const idSapEntrega = Number(req.params.id_sap_entrega);
  if (!Number.isInteger(idSapEntrega) || idSapEntrega <= 0) {
    res.status(400).json({ error: "id_sap_entrega invalido" });
    return;
  }

  let auth = null;
  try {
    auth = await resolveAuthContext(req);
  } catch (error) {
    next(error);
    return;
  }

  const canDescartarByRole = hasAnyRole(auth, ["autorizador", "administrador"]);
  if (!canDescartarByRole && !hasAnyPermission(auth, ["fletes.sap.descartar", "mantenedores.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para descartar ingresos SAP",
      role: auth?.primaryRole || null,
    });
    return;
  }

  const motivo = toNullableTrimmedString(req.body?.motivo);
  if (!motivo) {
    res.status(400).json({ error: "Debes ingresar un motivo para descartar la entrega SAP" });
    return;
  }
  const motivoFinal = motivo.slice(0, 200);
  const idUsuarioActor = parseOptionalBigInt(req.jwtPayload?.id_usuario);
  if (!idUsuarioActor) {
    res.status(401).json({ error: "Token invalido: usuario no identificado" });
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
        SELECT TOP 1 IdSapEntrega
        FROM [cfl].[SapEntrega]
        WHERE IdSapEntrega = @idSapEntrega;
      `);

    if (entregaResult.recordset.length === 0) {
      await transaction.rollback();
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const linkedResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 1 AS already_linked
        FROM [cfl].[FleteSapEntrega]
        WHERE IdSapEntrega = @idSapEntrega;
      `);

    if (linkedResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({
        error: "La entrega SAP ya se encuentra asociada a una cabecera de flete y no se puede descartar",
      });
      return;
    }

    const currentDiscardResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 IdSapEntregaDescarte
        FROM [cfl].[SapEntregaDescarte]
        WHERE IdSapEntrega = @idSapEntrega;
      `);

    if (currentDiscardResult.recordset.length > 0) {
      await new sql.Request(transaction)
        .input("idSapEntrega", sql.BigInt, idSapEntrega)
        .input("motivo", sql.VarChar(200), motivoFinal)
        .input("updatedAt", sql.DateTime2(0), now)
        .input("createdAt", sql.DateTime2(0), now)
        .input("createdBy", sql.BigInt, idUsuarioActor)
        .query(`
          UPDATE [cfl].[SapEntregaDescarte]
          SET Activo = 1,
              Motivo = @motivo,
              FechaCreacion = @createdAt,
              FechaActualizacion = @updatedAt,
              CreadoPor = @createdBy,
              FechaRestauracion = NULL,
              RestauradoPor = NULL
          WHERE IdSapEntrega = @idSapEntrega;
        `);
    } else {
      await new sql.Request(transaction)
        .input("idSapEntrega", sql.BigInt, idSapEntrega)
        .input("motivo", sql.VarChar(200), motivoFinal)
        .input("createdAt", sql.DateTime2(0), now)
        .input("updatedAt", sql.DateTime2(0), now)
        .input("createdBy", sql.BigInt, idUsuarioActor)
        .query(`
          INSERT INTO [cfl].[SapEntregaDescarte] (
            [IdSapEntrega],
            [Activo],
            [Motivo],
            [FechaCreacion],
            [FechaActualizacion],
            [CreadoPor]
          )
          VALUES (
            @idSapEntrega,
            1,
            @motivo,
            @createdAt,
            @updatedAt,
            @createdBy
          );
        `);
    }

    await transaction.commit();

    res.json({
      message: "Ingreso SAP descartado",
      data: {
        id_sap_entrega: idSapEntrega,
        activo: true,
        motivo: motivoFinal,
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

router.post("/fletes/no-ingresados/:id_sap_entrega/restaurar", async (req, res, next) => {
  const idSapEntrega = Number(req.params.id_sap_entrega);
  if (!Number.isInteger(idSapEntrega) || idSapEntrega <= 0) {
    res.status(400).json({ error: "id_sap_entrega invalido" });
    return;
  }

  let auth = null;
  try {
    auth = await resolveAuthContext(req);
  } catch (error) {
    next(error);
    return;
  }

  const canRestaurarByRole = hasAnyRole(auth, ["autorizador", "administrador"]);
  if (!canRestaurarByRole && !hasAnyPermission(auth, ["fletes.sap.descartar", "mantenedores.admin"])) {
    res.status(403).json({
      error: "No tienes permisos para restaurar ingresos SAP",
      role: auth?.primaryRole || null,
    });
    return;
  }

  const idUsuarioActor = parseOptionalBigInt(req.jwtPayload?.id_usuario);

  try {
    const pool = await getPool();
    const now = new Date();

    const result = await pool
      .request()
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .input("updatedAt", sql.DateTime2(0), now)
      .input("restoredAt", sql.DateTime2(0), now)
      .input("restoredBy", sql.BigInt, idUsuarioActor)
      .query(`
        UPDATE [cfl].[SapEntregaDescarte]
        SET activo = 0,
            FechaActualizacion = @updatedAt,
            restored_at = @restoredAt,
            restored_by = @restoredBy
        WHERE IdSapEntrega = @idSapEntrega
          AND activo = 1;

        SELECT @@ROWCOUNT AS affected;
      `);

    const affected = Number(result.recordset?.[0]?.affected || 0);
    if (affected === 0) {
      res.status(404).json({ error: "La entrega SAP no se encuentra descartada" });
      return;
    }

    res.json({
      message: "Ingreso SAP restaurado",
      data: {
        id_sap_entrega: idSapEntrega,
        activo: false,
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
  const idCentroCosto = parseRequiredBigInt(cabeceraIn.id_centro_costo);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const idDetalleViaje = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idFolio = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifa = parseOptionalBigInt(cabeceraIn.id_tarifa);
  const idCuentaMayor = parseOptionalBigInt(cabeceraIn.id_cuenta_mayor);
  const idProductor = parseOptionalBigInt(cabeceraIn.id_productor);
  const sentidoFlete = toNullableTrimmedString(cabeceraIn.sentido_flete);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCosto) {
    res.status(400).json({ error: "Falta id_centro_costo" });
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
      idCentroCosto,
      idDetalleViaje,
      idMovil,
      idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    const entregaResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1
          e.IdSapEntrega,
          e.SapNumeroEntrega,
          e.SistemaFuente,
          lk.SapCodigoTipoFlete,
          lk.SapCentroCosto,
          lk.SapCuentaMayor,
          lk.SapGuiaRemision,
          lk.SapDestinatario
        FROM [cfl].[SapEntrega] e
        LEFT JOIN [cfl].[VW_LikpActual] lk
          ON lk.SapNumeroEntrega = e.SapNumeroEntrega
         AND lk.SistemaFuente = e.SistemaFuente
        WHERE e.IdSapEntrega = @idSapEntrega;
      `);

    const entrega = entregaResult.recordset[0];
    if (!entrega) {
      await transaction.rollback();
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const discardedResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 1 AS is_discarded
        FROM [cfl].[SapEntregaDescarte]
        WHERE IdSapEntrega = @idSapEntrega
          AND activo = 1;
      `);

    if (discardedResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({ error: "La entrega SAP se encuentra descartada y debe restaurarse antes de crearla" });
      return;
    }

    const existsResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 1 AS already_linked
        FROM [cfl].[FleteSapEntrega]
        WHERE IdSapEntrega = @idSapEntrega;
      `);

    if (existsResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({ error: "La entrega SAP ya se encuentra asociada" });
      return;
    }

    const sapCuentaMayor = toNullableTrimmedString(entrega.sap_cuenta_mayor);
    const sapTipoFlete = toNullableTrimmedString(entrega.sap_codigo_tipo_flete);
    const sapCentroCosto = toNullableTrimmedString(entrega.sap_centro_costo);
    const tipoFleteCanonicalResult = await new sql.Request(transaction)
      .input("idTipoFlete", sql.BigInt, idTipoFlete)
      .query(`
        SELECT TOP 1 SapCodigo
        FROM [cfl].[TipoFlete]
        WHERE IdTipoFlete = @idTipoFlete;
      `);
    const tipoFleteCanonicoSug = toNullableTrimmedString(tipoFleteCanonicalResult.recordset[0]?.sap_codigo);

    const centroCostoCanonicalResult = await new sql.Request(transaction)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .query(`
        SELECT TOP 1 SapCodigo
        FROM [cfl].[CentroCosto]
        WHERE IdCentroCosto = @idCentroCosto;
      `);
    const centroCostoCanonicoSug = toNullableTrimmedString(centroCostoCanonicalResult.recordset[0]?.sap_codigo);

    let cuentaMayorCanonicaSug = null;
    if (idCuentaMayor) {
      const cuentaMayorCanonicalResult = await new sql.Request(transaction)
        .input("idCuentaMayor", sql.BigInt, idCuentaMayor)
        .query(`
          SELECT TOP 1 codigo
          FROM [cfl].[CuentaMayor]
          WHERE IdCuentaMayor = @idCuentaMayor;
        `);
      cuentaMayorCanonicaSug = toNullableTrimmedString(cuentaMayorCanonicalResult.recordset[0]?.codigo);
    }

    const sapTipoFleteSug = sapTipoFlete || tipoFleteCanonicoSug;
    const sapCentroCostoSug = sapCentroCosto || centroCostoCanonicoSug;
    const sapCuentaMayorSug = sapCuentaMayor || cuentaMayorCanonicaSug;
    // cuenta_mayor_final eliminado; la cuenta contable final se gestiona via id_cuenta_mayor (FK)
    const sapGuiaRemision = toNullableTrimmedString(entrega.sap_guia_remision);
    const idProductorResolved = await resolveProductorIdByDestinatario(
      transaction,
      idProductor,
      entrega.SapDestinatario
    );

    const insertCabeceraReq = new sql.Request(transaction);
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, idDetalleViaje);
    insertCabeceraReq.input("idFolio", sql.BigInt, idFolio);
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), entrega.sap_numero_entrega);
    insertCabeceraReq.input("sapCodigoTipoFlete", sql.Char(4), sapTipoFleteSug ? sapTipoFleteSug.slice(0, 4) : null);
    insertCabeceraReq.input("sapCentroCosto", sql.Char(10), sapCentroCostoSug ? sapCentroCostoSug.slice(0, 10) : null);
    insertCabeceraReq.input("sapCuentaMayor", sql.Char(10), sapCuentaMayorSug ? sapCuentaMayorSug.slice(0, 10) : null);
    insertCabeceraReq.input("sapGuiaRemision", sql.Char(25), sapGuiaRemision ? sapGuiaRemision.slice(0, 25) : null);
    // guia_remision y numero_entrega se rellenan cuando el operador edita el flete; null en creación
    const guiaRemisionIn = toNullableTrimmedString(cabeceraIn.guia_remision);
    const numeroEntregaIn = toNullableTrimmedString(cabeceraIn.numero_entrega);
    insertCabeceraReq.input("guiaRemision", sql.Char(25), guiaRemisionIn ? guiaRemisionIn.slice(0, 25) : null);
    insertCabeceraReq.input("numeroEntrega", sql.VarChar(20), numeroEntregaIn ? numeroEntregaIn.slice(0, 20) : null);
    insertCabeceraReq.input("idProductor", sql.BigInt, idProductorResolved);
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), tipoMovimiento);
    insertCabeceraReq.input("sentidoFlete", sql.VarChar(20), sentidoFlete ? sentidoFlete.slice(0, 20) : null);
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
    insertCabeceraReq.input("idCentroCosto", sql.BigInt, idCentroCosto);

    const cabeceraResult = await insertCabeceraReq.query(`
      INSERT INTO [cfl].[CabeceraFlete] (
        [IdDetalleViaje],
        [IdFolio],
        [SapNumeroEntrega],
        [SapCodigoTipoFlete],
        [SapCentroCosto],
        [SapCuentaMayor],
        [SapGuiaRemision],
        [GuiaRemision],
        [NumeroEntrega],
        [IdProductor],
        [TipoMovimiento],
        [SentidoFlete],
        [Estado],
        [FechaSalida],
        [HoraSalida],
        [MontoAplicado],
        [IdMovil],
        [IdTarifa],
        [Observaciones],
        [IdUsuarioCreador],
        [IdTipoFlete],
        [FechaCreacion],
        [FechaActualizacion],
        [IdCentroCosto]
      )
      OUTPUT INSERTED.IdCabeceraFlete
      VALUES (
        @idDetalleViaje,
        @idFolio,
        @sapNumeroEntrega,
        @sapCodigoTipoFlete,
        @sapCentroCosto,
        @sapCuentaMayor,
        @sapGuiaRemision,
        @guiaRemision,
        @numeroEntrega,
        @idProductor,
        @tipoMovimiento,
        @sentidoFlete,
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
        @idCentroCosto
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].IdCabeceraFlete;

    await new sql.Request(transaction)
      .input("idCabeceraFlete", sql.BigInt, idCabeceraFlete)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .input("origenDatos", sql.VarChar(10), (toNullableTrimmedString(entrega.source_system) || "SAP").slice(0, 10))
      .input("tipoRelacion", sql.VarChar(20), "PRINCIPAL")
      .input("createdAt", sql.DateTime2(0), now)
      .query(`
        INSERT INTO [cfl].[FleteSapEntrega] (
          [IdCabeceraFlete],
          [IdSapEntrega],
          [OrigenDatos],
          [TipoRelacion],
          [FechaCreacion]
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
          INSERT INTO [cfl].[DetalleFlete] (
            [IdCabeceraFlete],
            [IdEspecie],
            [Material],
            [Descripcion],
            [Cantidad],
            [Unidad],
            [Peso],
            [FechaCreacion]
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
        e.IdSapEntrega,
        e.SapNumeroEntrega,
        e.SistemaFuente,
        lk.SapCodigoTipoFlete,
        lk.SapCentroCosto,
        lk.SapCuentaMayor,
        lk.SapGuiaRemision,
        lk.SapDestinatario,
        CONVERT(VARCHAR(10), lk.SapFechaSalida, 23) AS sap_fecha_salida_iso,
        CONVERT(VARCHAR(8), lk.SapHoraSalida, 108) AS sap_hora_salida_iso
      FROM [cfl].[SapEntrega] e
      LEFT JOIN [cfl].[VW_LikpActual] lk
        ON lk.SapNumeroEntrega = e.SapNumeroEntrega
       AND lk.SistemaFuente = e.SistemaFuente
      WHERE e.IdSapEntrega = @idSapEntrega;
    `);

    const delivery = deliveryResult.recordset[0];
    if (!delivery) {
      await transaction.rollback();
      res.status(404).json({ error: "Entrega SAP no encontrada" });
      return;
    }

    const discardedResult = await new sql.Request(transaction)
      .input("idSapEntrega", sql.BigInt, idSapEntrega)
      .query(`
        SELECT TOP 1 1 AS is_discarded
        FROM [cfl].[SapEntregaDescarte]
        WHERE IdSapEntrega = @idSapEntrega
          AND activo = 1;
      `);

    if (discardedResult.recordset.length > 0) {
      await transaction.rollback();
      res.status(409).json({
        error: "La entrega SAP se encuentra descartada y debe restaurarse antes de ingresarla",
      });
      return;
    }

    const existsRequest = new sql.Request(transaction);
    existsRequest.input("idSapEntrega", sql.BigInt, idSapEntrega);
    const existsResult = await existsRequest.query(`
      SELECT TOP 1 1 AS already_linked
      FROM [cfl].[FleteSapEntrega]
      WHERE IdSapEntrega = @idSapEntrega;
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
      SELECT TOP 1 IdTipoFlete, IdCentroCosto
      FROM [cfl].[TipoFlete]
      WHERE SapCodigo = @sapCodigo
      ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, IdTipoFlete ASC;
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
    let idCentroCosto = null;

    if (sapCentroCosto) {
      const centroRequest = new sql.Request(transaction);
      centroRequest.input("sapCentroCosto", sql.VarChar(20), sapCentroCosto);
      const centroResult = await centroRequest.query(`
        SELECT TOP 1 IdCentroCosto
        FROM [cfl].[CentroCosto]
        WHERE SapCodigo = @sapCentroCosto
        ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, IdCentroCosto ASC;
      `);

      idCentroCosto = centroResult.recordset[0]?.id_centro_costo || null;
    }

    if (!idCentroCosto) {
      idCentroCosto = tipoFlete.id_centro_costo || null;
    }

    if (!idCentroCosto) {
      await transaction.rollback();
      res.status(422).json({
        error: "No se pudo resolver id_centro_costo para la cabecera de flete",
      });
      return;
    }

    const fechaSalida = delivery.sap_fecha_salida_iso || now.toISOString().slice(0, 10);
    const horaSalida = delivery.sap_hora_salida_iso || now.toISOString().slice(11, 19);
    const sapCuentaMayor = toNullableTrimmedString(delivery.sap_cuenta_mayor);

    const insertCabeceraRequest = new sql.Request(transaction);
    // cuenta_mayor_final eliminado; la cuenta contable final se gestiona via id_cuenta_mayor (FK)
    const sapGuiaRemisionIngresar = toNullableTrimmedString(delivery.sap_guia_remision);
    const idProductorResolved = await resolveProductorIdByDestinatario(
      transaction,
      null,
      delivery.SapDestinatario
    );
    insertCabeceraRequest.input("sapNumeroEntrega", sql.VarChar(20), delivery.sap_numero_entrega);
    insertCabeceraRequest.input("sapCodigoTipoFlete", sql.Char(4), sapTipoFlete.slice(0, 4));
    insertCabeceraRequest.input("sapCentroCosto", sql.Char(10), sapCentroCosto ? sapCentroCosto.slice(0, 10) : null);
    insertCabeceraRequest.input("sapCuentaMayor", sql.Char(10), sapCuentaMayor ? sapCuentaMayor.slice(0, 10) : null);
    insertCabeceraRequest.input("sapGuiaRemision", sql.Char(25), sapGuiaRemisionIngresar ? sapGuiaRemisionIngresar.slice(0, 25) : null);
    insertCabeceraRequest.input("idProductor", sql.BigInt, idProductorResolved);
    insertCabeceraRequest.input("tipoMovimiento", sql.VarChar(4), "PUSH");
    insertCabeceraRequest.input("sentidoFlete", sql.VarChar(20), null);
    insertCabeceraRequest.input("estado", sql.VarChar(20), LIFECYCLE_STATUS.EN_REVISION);
    insertCabeceraRequest.input("fechaSalida", sql.Date, fechaSalida);
    insertCabeceraRequest.input("horaSalida", sql.VarChar(8), horaSalida);
    insertCabeceraRequest.input("montoAplicado", sql.Decimal(18, 2), 0);
    insertCabeceraRequest.input("idTipoFlete", sql.BigInt, tipoFlete.id_tipo_flete);
    insertCabeceraRequest.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraRequest.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraRequest.input("idCentroCosto", sql.BigInt, idCentroCosto);

    const cabeceraResult = await insertCabeceraRequest.query(`
      INSERT INTO [cfl].[CabeceraFlete] (
        [SapNumeroEntrega],
        [SapCodigoTipoFlete],
        [SapCentroCosto],
        [SapCuentaMayor],
        [SapGuiaRemision],
        [IdProductor],
        [TipoMovimiento],
        [SentidoFlete],
        [Estado],
        [FechaSalida],
        [HoraSalida],
        [MontoAplicado],
        [IdTipoFlete],
        [FechaCreacion],
        [FechaActualizacion],
        [IdCentroCosto]
      )
      OUTPUT INSERTED.IdCabeceraFlete
      VALUES (
        @sapNumeroEntrega,
        @sapCodigoTipoFlete,
        @sapCentroCosto,
        @sapCuentaMayor,
        @sapGuiaRemision,
        @idProductor,
        @tipoMovimiento,
        @sentidoFlete,
        @estado,
        @fechaSalida,
        CAST(@horaSalida AS TIME),
        @montoAplicado,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCentroCosto
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
      INSERT INTO [cfl].[FleteSapEntrega] (
        [IdCabeceraFlete],
        [IdSapEntrega],
        [OrigenDatos],
        [TipoRelacion],
        [FechaCreacion]
      )
      OUTPUT INSERTED.IdFleteSapEntrega
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
