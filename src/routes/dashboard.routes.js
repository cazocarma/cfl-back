const express = require("express");
const { getPool, sql } = require("../db");
const { hasAnyPermission, resolveAuthzContext } = require("../authz");
const {
  clamp,
  parsePositiveInt,
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  normalizeTipoMovimiento,
} = require("../utils/parse");
const {
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
} = require("../utils/lifecycle");
const {
  resolveMovilId,
  resolveImputacionFlete,
} = require("../helpers");

const router = express.Router();

function buildMissingDeliveriesQuery(filters) {
  const whereClauses = [
    "NOT EXISTS (SELECT 1 FROM [cfl].[FleteSapEntrega] fe WHERE fe.IdSapEntrega = c.IdSapEntrega)",
    "NOT EXISTS (SELECT 1 FROM [cfl].[SapEntregaDescarte] sd WHERE sd.IdSapEntrega = c.IdSapEntrega AND sd.Activo = 1)",
    "c.SapGuiaRemision IS NOT NULL",
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
    whereClauses.push("CAST(c.SapFechaSalida AS DATE) >= CAST(@fechaDesde AS DATE)");
  }
  if (filters.fechaHasta) {
    whereClauses.push("CAST(c.SapFechaSalida AS DATE) <= CAST(@fechaHasta AS DATE)");
  }
  if (filters.estado) {
    whereClauses.push("c.Estado = @estado");
  }

  return `
    FROM #candidates c
    WHERE ${whereClauses.join(" AND ")}
  `;
}

function hasAnyRole(authzContext, roles = []) {
  if (!authzContext || !Array.isArray(authzContext.roleNames)) {
    return false;
  }

  const roleSet = new Set(
    authzContext.roleNames.map((role) =>
      String(role || "").trim().toLowerCase()
    )
  );
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

async function resolveSapImputacionContext(transaction, {
  sapTipoFlete,
  sapCentroCosto,
  sapCuentaMayor,
}) {
  const tipoCodigo = toNullableTrimmedString(sapTipoFlete);
  const centroCodigo = toNullableTrimmedString(sapCentroCosto);
  const cuentaCodigo = toNullableTrimmedString(sapCuentaMayor);

  if (!tipoCodigo) {
    return {
      idTipoFlete: null,
      idCentroCosto: null,
      idCuentaMayor: null,
      idImputacionFlete: null,
    };
  }

  const tipoResult = await new sql.Request(transaction)
    .input("sapCodigoTipoFlete", sql.VarChar(20), tipoCodigo)
    .query(`
      SELECT TOP 1
        IdTipoFlete
      FROM [cfl].[TipoFlete]
      WHERE SapCodigo = @sapCodigoTipoFlete
      ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdTipoFlete ASC;
    `);

  const idTipoFlete = Number(tipoResult.recordset[0]?.IdTipoFlete || 0) || null;
  if (!idTipoFlete) {
    return {
      idTipoFlete: null,
      idCentroCosto: null,
      idCuentaMayor: null,
      idImputacionFlete: null,
    };
  }

  let idCentroCosto = null;
  if (centroCodigo) {
    const centroResult = await new sql.Request(transaction)
      .input("sapCentroCosto", sql.VarChar(20), centroCodigo)
      .query(`
        SELECT TOP 1
          IdCentroCosto
        FROM [cfl].[CentroCosto]
        WHERE SapCodigo = @sapCentroCosto
        ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdCentroCosto ASC;
      `);
    idCentroCosto = Number(centroResult.recordset[0]?.IdCentroCosto || 0) || null;
  }

  let idCuentaMayor = null;
  if (cuentaCodigo) {
    const cuentaResult = await new sql.Request(transaction)
      .input("codigoCuentaMayor", sql.VarChar(30), cuentaCodigo)
      .query(`
        SELECT TOP 1
          IdCuentaMayor
        FROM [cfl].[CuentaMayor]
        WHERE Codigo = @codigoCuentaMayor
        ORDER BY IdCuentaMayor ASC;
      `);
    idCuentaMayor = Number(cuentaResult.recordset[0]?.IdCuentaMayor || 0) || null;
  }

  return resolveImputacionFlete(transaction, {
    idTipoFlete,
    idCentroCosto,
    idCuentaMayor,
    idImputacionFlete: null,
  });
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
      SET NOCOUNT ON;

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
        IdImputacionFlete = COALESCE(
          im_direct.IdImputacionFlete,
          CASE WHEN im_default.CntActivas = 1 THEN im_default.IdImputacionFlete ELSE NULL END
        ),
        IdCentroCosto = COALESCE(
          cc_sap.IdCentroCosto,
          im_direct.IdCentroCosto,
          CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCentroCosto ELSE NULL END
        ),
        IdCuentaMayor = COALESCE(
          cm_sap.IdCuentaMayor,
          im_direct.IdCuentaMayor,
          CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCuentaMayor ELSE NULL END
        ),
        -- Semantica: candidatos (aun no existe cabecera) => siempre DETECTADO.
        Estado = 'DETECTADO',
        puede_ingresar = CAST(
          CASE
            WHEN tf.IdTipoFlete IS NULL THEN 0
            WHEN COALESCE(
              cc_sap.IdCentroCosto,
              im_direct.IdCentroCosto,
              CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCentroCosto ELSE NULL END
            ) IS NULL THEN 0
            WHEN COALESCE(
              cm_sap.IdCuentaMayor,
              im_direct.IdCuentaMayor,
              CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCuentaMayor ELSE NULL END
            ) IS NULL THEN 0
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
          WHEN COALESCE(
            cc_sap.IdCentroCosto,
            im_direct.IdCentroCosto,
            CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCentroCosto ELSE NULL END
          ) IS NULL THEN CONCAT(
            'No se pudo resolver Centro de Costo (SapCentroCosto=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.SapCentroCosto)), ''), '(NULL)'),
            ')'
          )
          WHEN COALESCE(
            cm_sap.IdCuentaMayor,
            im_direct.IdCuentaMayor,
            CASE WHEN im_default.CntActivas = 1 THEN im_default.IdCuentaMayor ELSE NULL END
          ) IS NULL THEN CONCAT(
            'No se pudo resolver Cuenta Mayor (SapCuentaMayor=',
            COALESCE(NULLIF(LTRIM(RTRIM(lk.SapCuentaMayor)), ''), '(NULL)'),
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
      LEFT JOIN [cfl].[CuentaMayor] cm_sap
        ON cm_sap.Codigo = lk.SapCuentaMayor
      OUTER APPLY (
        SELECT TOP 1
          im.IdImputacionFlete,
          im.IdCentroCosto,
          im.IdCuentaMayor
        FROM [cfl].[ImputacionFlete] im
        WHERE im.IdTipoFlete = tf.IdTipoFlete
          AND im.IdCentroCosto = cc_sap.IdCentroCosto
          AND im.IdCuentaMayor = cm_sap.IdCuentaMayor
        ORDER BY CASE WHEN im.Activo = 1 THEN 0 ELSE 1 END, im.IdImputacionFlete ASC
      ) im_direct
      OUTER APPLY (
        SELECT
          IdImputacionFlete = MIN(im.IdImputacionFlete),
          IdCentroCosto = MIN(im.IdCentroCosto),
          IdCuentaMayor = MIN(im.IdCuentaMayor),
          CntActivas = COUNT_BIG(1)
        FROM [cfl].[ImputacionFlete] im
        WHERE im.IdTipoFlete = tf.IdTipoFlete
          AND im.Activo = 1
      ) im_default
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
        cc_sap.IdCentroCosto,
        cm_sap.IdCuentaMayor,
        im_direct.IdImputacionFlete,
        im_direct.IdCentroCosto,
        im_direct.IdCuentaMayor,
        im_default.IdImputacionFlete,
        im_default.IdCentroCosto,
        im_default.IdCuentaMayor,
        im_default.CntActivas,
        e.FechaUltimaVista,
        e.FechaActualizacion;

      SET NOCOUNT OFF;

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
        IdImputacionFlete,
        IdCentroCosto,
        IdCuentaMayor,
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
    const total = Number(result.recordsets[0]?.[0]?.total ?? 0);
    const data = result.recordsets[1] ?? [];

    res.json({
      data,
      pagination: {
        page,
        page_size: pageSize,
        total,
        total_pages: Math.ceil(total / pageSize) || 0,
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

router.get("/fletes/completados", async (req, res, next) => {
  const page = parsePositiveInt(req.query.page, 1);
  const pageSize = clamp(parsePositiveInt(req.query.page_size, 25), 1, 500);
  const offset = (page - 1) * pageSize;
  const estadoFiltro = toNullableTrimmedString(req.query.estado);
  const searchRaw = toNullableTrimmedString(req.query.search);
  const fechaDesdeRaw = toNullableTrimmedString(req.query.fecha_desde);
  const fechaHastaRaw = toNullableTrimmedString(req.query.fecha_hasta);

  // Ordenamiento seguro: whitelist de columnas permitidas
  const SORT_COLUMNS = {
    id: "IdCabeceraFlete",
    fecha: "FechaSalida",
    monto: "MontoAplicado",
    estado: "estado_lifecycle",
    tipo_flete: "tipo_flete_nombre",
    actualizado: "FechaActualizacion",
  };
  const sortByRaw = toNullableTrimmedString(req.query.sort_by);
  const sortDirRaw = toNullableTrimmedString(req.query.sort_dir);
  const sortColumn = SORT_COLUMNS[sortByRaw] || "IdCabeceraFlete";
  const sortDir = sortDirRaw === "asc" ? "ASC" : "DESC";
  const orderClause = sortColumn === "IdCabeceraFlete"
    ? `IdCabeceraFlete ${sortDir}`
    : `${sortColumn} ${sortDir}, IdCabeceraFlete DESC`;

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
          WHEN UPPER(ISNULL(cf.Estado, '')) = 'PREFACTURADO' THEN 'PREFACTURADO'
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
      ORDER BY ${orderClause}
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

router.post("/fletes/:id_cabecera_flete/anular", async (req, res, next) => {
  const idCabecera = Number(req.params.id_cabecera_flete);
  if (!Number.isInteger(idCabecera) || idCabecera <= 0) {
    res.status(400).json({ error: "id_cabecera_flete invalido" });
    return;
  }

  let authzContext = null;
  try {
    authzContext = await resolveAuthzContext(req);
  } catch (error) {
    next(error);
    return;
  }

  const canAnularByRole = hasAnyRole(authzContext, ["autorizador", "administrador"]);
  if (
    !canAnularByRole &&
    !hasAnyPermission(authzContext, ["fletes.anular", "mantenedores.admin"])
  ) {
    res.status(403).json({
      error: "No tienes permisos para anular fletes",
      role: authzContext?.primaryRole || null,
    });
    return;
  }

  const motivo = toNullableTrimmedString(req.body?.motivo);
  if (!motivo) {
    res.status(400).json({ error: "Debes ingresar un motivo para anular el flete" });
    return;
  }
  const motivoFinal = motivo.slice(0, 200);
  const idUsuarioActor = parseOptionalBigInt(req.authnClaims?.id_usuario);
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

  let authzContext = null;
  try {
    authzContext = await resolveAuthzContext(req);
  } catch (error) {
    next(error);
    return;
  }

  const canDescartarByRole = hasAnyRole(authzContext, ["autorizador", "administrador"]);
  if (
    !canDescartarByRole &&
    !hasAnyPermission(authzContext, ["fletes.sap.descartar", "mantenedores.admin"])
  ) {
    res.status(403).json({
      error: "No tienes permisos para descartar ingresos SAP",
      role: authzContext?.primaryRole || null,
    });
    return;
  }

  const motivo = toNullableTrimmedString(req.body?.motivo);
  if (!motivo) {
    res.status(400).json({ error: "Debes ingresar un motivo para descartar la entrega SAP" });
    return;
  }
  const motivoFinal = motivo.slice(0, 200);
  const idUsuarioActor = parseOptionalBigInt(req.authnClaims?.id_usuario);
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

  let authzContext = null;
  try {
    authzContext = await resolveAuthzContext(req);
  } catch (error) {
    next(error);
    return;
  }

  const canRestaurarByRole = hasAnyRole(authzContext, ["autorizador", "administrador"]);
  if (
    !canRestaurarByRole &&
    !hasAnyPermission(authzContext, ["fletes.sap.descartar", "mantenedores.admin"])
  ) {
    res.status(403).json({
      error: "No tienes permisos para restaurar ingresos SAP",
      role: authzContext?.primaryRole || null,
    });
    return;
  }

  const idUsuarioActor = parseOptionalBigInt(req.authnClaims?.id_usuario);

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
  const idCentroCostoInput = parseOptionalBigInt(cabeceraIn.id_centro_costo);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const montoExtraRaw = cabeceraIn.monto_extra;
  const montoExtra = Number.isFinite(Number(montoExtraRaw)) ? Number(montoExtraRaw) : 0;
  const idDetalleViaje = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idTarifa = parseOptionalBigInt(cabeceraIn.id_tarifa);
  const idCuentaMayorInput = parseOptionalBigInt(cabeceraIn.id_cuenta_mayor);
  const idImputacionFleteInput = parseOptionalBigInt(cabeceraIn.id_imputacion_flete);
  const idProductor = parseOptionalBigInt(cabeceraIn.id_productor);
  const sentidoFlete = toNullableTrimmedString(cabeceraIn.sentido_flete);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoInput && !idImputacionFleteInput) {
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
    const imputacion = await resolveImputacionFlete(transaction, {
      idTipoFlete,
      idCentroCosto: idCentroCostoInput,
      idCuentaMayor: idCuentaMayorInput,
      idImputacionFlete: idImputacionFleteInput,
    });
    const idCentroCosto = imputacion.idCentroCosto;
    const idCuentaMayor = imputacion.idCuentaMayor;
    const idImputacionFlete = imputacion.idImputacionFlete;
    if (!idCentroCosto) {
      await transaction.rollback();
      res.status(422).json({ error: "No se pudo resolver id_centro_costo para la cabecera de flete" });
      return;
    }

    const idMovil = await resolveMovilId(transaction, cabeceraIn, now);
    const estado = deriveLifecycleStatus({
      requestedStatus,
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
    const tipoFleteCanonicoSug = toNullableTrimmedString(tipoFleteCanonicalResult.recordset[0]?.SapCodigo);

    const centroCostoCanonicalResult = await new sql.Request(transaction)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .query(`
        SELECT TOP 1 SapCodigo
        FROM [cfl].[CentroCosto]
        WHERE IdCentroCosto = @idCentroCosto;
      `);
    const centroCostoCanonicoSug = toNullableTrimmedString(centroCostoCanonicalResult.recordset[0]?.SapCodigo);

    let cuentaMayorCanonicaSug = null;
    if (idCuentaMayor) {
      const cuentaMayorCanonicalResult = await new sql.Request(transaction)
        .input("idCuentaMayor", sql.BigInt, idCuentaMayor)
        .query(`
          SELECT TOP 1 codigo
          FROM [cfl].[CuentaMayor]
          WHERE IdCuentaMayor = @idCuentaMayor;
        `);
      cuentaMayorCanonicaSug = toNullableTrimmedString(
        cuentaMayorCanonicalResult.recordset[0]?.codigo
        || cuentaMayorCanonicalResult.recordset[0]?.Codigo
      );
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
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), entrega.sap_numero_entrega);
    insertCabeceraReq.input("sapCodigoTipoFlete", sql.Char(4), sapTipoFleteSug ? sapTipoFleteSug.slice(0, 4) : null);
    insertCabeceraReq.input("sapCentroCosto", sql.Char(10), sapCentroCostoSug ? sapCentroCostoSug.slice(0, 10) : null);
    insertCabeceraReq.input("sapCuentaMayor", sql.Char(10), sapCuentaMayorSug ? sapCuentaMayorSug.slice(0, 10) : null);
    insertCabeceraReq.input("idCuentaMayor", sql.BigInt, idCuentaMayor);
    insertCabeceraReq.input("idImputacionFlete", sql.BigInt, idImputacionFlete);
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
    insertCabeceraReq.input("montoExtra", sql.Decimal(18, 2), montoExtra);
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
        [MontoExtra],
        [IdMovil],
        [IdTarifa],
        [Observaciones],
        [IdUsuarioCreador],
        [IdTipoFlete],
        [FechaCreacion],
        [FechaActualizacion],
        [IdCuentaMayor],
        [IdImputacionFlete],
        [IdCentroCosto]
      )
      OUTPUT INSERTED.IdCabeceraFlete
      VALUES (
        @idDetalleViaje,
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
        @montoExtra,
        @idMovil,
        @idTarifa,
        @observaciones,
        @idUsuarioCreador,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCuentaMayor,
        @idImputacionFlete,
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

    const sapCentroCosto = toNullableTrimmedString(delivery.sap_centro_costo);
    const sapCuentaMayor = toNullableTrimmedString(delivery.sap_cuenta_mayor);
    const imputacion = await resolveSapImputacionContext(transaction, {
      sapTipoFlete,
      sapCentroCosto,
      sapCuentaMayor,
    });

    if (!imputacion.idTipoFlete) {
      await transaction.rollback();
      res.status(422).json({
        error: `No existe un tipo de flete configurado para sap_codigo_tipo_flete=${sapTipoFlete}`,
      });
      return;
    }

    const idCentroCosto = imputacion.idCentroCosto || null;
    const idCuentaMayor = imputacion.idCuentaMayor || null;
    const idImputacionFlete = imputacion.idImputacionFlete || null;

    if (!idCentroCosto) {
      await transaction.rollback();
      res.status(422).json({
        error: "No se pudo resolver id_centro_costo para la cabecera de flete",
      });
      return;
    }

    const fechaSalida = delivery.sap_fecha_salida_iso || now.toISOString().slice(0, 10);
    const horaSalida = delivery.sap_hora_salida_iso || now.toISOString().slice(11, 19);

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
    insertCabeceraRequest.input("idTipoFlete", sql.BigInt, imputacion.idTipoFlete);
    insertCabeceraRequest.input("idCuentaMayor", sql.BigInt, idCuentaMayor);
    insertCabeceraRequest.input("idImputacionFlete", sql.BigInt, idImputacionFlete);
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
        [IdCuentaMayor],
        [IdImputacionFlete],
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
        @idCuentaMayor,
        @idImputacionFlete,
        @createdAt,
        @updatedAt,
        @idCentroCosto
      );
    `);

    const idCabeceraFlete = Number(
      cabeceraResult.recordset[0]?.IdCabeceraFlete
      || cabeceraResult.recordset[0]?.id_cabecera_flete
      || 0
    );

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

    const idFleteSapEntrega = Number(
      bridgeResult.recordset[0]?.IdFleteSapEntrega
      || bridgeResult.recordset[0]?.id_flete_sap_entrega
      || 0
    );

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
