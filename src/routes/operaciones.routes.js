const express = require("express");
const { getPool } = require("../db");
const { clamp, parsePositiveInt } = require("../utils/parse");
const { hasAnyPermission, resolveAuthzContext } = require("../authz");

const router = express.Router();

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toNumber(value, fallback = 0) {
  const parsed = toNumberOrNull(value);
  return parsed === null ? fallback : parsed;
}

function uniqueBy(rows, keyName) {
  const seen = new Set();
  const output = [];

  for (const row of rows || []) {
    const key = row?.[keyName];
    if (key === null || key === undefined) continue;
    const normalized = String(key);
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    output.push(row);
  }

  return output;
}

const OPERACIONES_READ_PERMISSIONS = [
  "operaciones.ver",
  "facturas.ver",
  "facturas.editar",
  "facturas.conciliar",
  "planillas.ver",
  "planillas.generar",
];

function isAdmin(authzContext) {
  return String(authzContext?.primaryRole || "").toLowerCase() === "administrador";
}

function ensureCanReadOperaciones(authzContext, res) {
  if (isAdmin(authzContext) || hasAnyPermission(authzContext, OPERACIONES_READ_PERMISSIONS)) {
    return true;
  }
  res.status(403).json({ error: "No tienes permisos para consultar operaciones" });
  return false;
}

function buildPermissions(authzContext) {
  return {
    can_edit_facturas:
      hasAnyPermission(authzContext, ["facturas.editar", "facturas.conciliar"]) ||
      isAdmin(authzContext),
    can_generate_planillas:
      hasAnyPermission(authzContext, ["planillas.generar"]) ||
      isAdmin(authzContext),
  };
}

router.get("/facturas/overview", async (req, res, next) => {
  try {
    const authzContext = await resolveAuthzContext(req);
    if (!ensureCanReadOperaciones(authzContext, res)) return;
    const pool = await getPool();

    const summaryResult = await pool.request().query(`
      SELECT
        facturas_registradas = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFactura]
        ),
        fletes_prefacturado = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFlete]
          WHERE UPPER(estado) = 'PREFACTURADO'
        ),
        monto_facturado = (
          SELECT COALESCE(SUM(MontoTotal), 0)
          FROM [cfl].[CabeceraFactura]
        ),
        monto_pendiente_estimado = (
          SELECT COALESCE(SUM(COALESCE(cf.MontoAplicado, 0)), 0)
          FROM [cfl].[CabeceraFlete] cf
          WHERE UPPER(cf.estado) = 'PREFACTURADO'
        );
    `);

    const facturasResult = await pool.request().query(`
      SELECT TOP 100
        fac.IdFactura,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.FechaCreacion,
        fac.FechaActualizacion
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    res.json({
      data: {
        resumen: summaryResult.recordset[0] || {},
        facturas: facturasResult.recordset,
      },
      permissions: buildPermissions(authzContext),
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/planillas-sap/overview", async (req, res, next) => {
  try {
    const authzContext = await resolveAuthzContext(req);
    if (!ensureCanReadOperaciones(authzContext, res)) return;
    const pool = await getPool();

    // Facturas recibidas sin planilla generada
    const invoicesResult = await pool.request().query(`
      SELECT
        fac.IdFactura,
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.estado,
        fac.moneda,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        empresa_rut = emp.Rut,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        periodo_anio = YEAR(fac.FechaEmision),
        periodo_mes  = MONTH(fac.FechaEmision)
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      WHERE LOWER(fac.estado) = 'recibida'
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[PlanillaSapFactura] psf
          WHERE psf.IdFactura = fac.IdFactura
        )
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    const facturas = invoicesResult.recordset;

    // Agrupar por período (año-mes) + empresa
    const MESES = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
      'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];

    const grupoMap = new Map();
    const centrosCostoSet = new Set();

    for (const fac of facturas) {
      const anio = fac.periodo_anio;
      const mes = fac.periodo_mes;
      const idEmp = fac.id_empresa;
      const key = `${anio}-${String(mes).padStart(2, '0')}_${idEmp}`;

      if (!grupoMap.has(key)) {
        grupoMap.set(key, {
          group_key: key,
          periodo_label: `${MESES[mes] || mes} ${anio}`,
          empresa_nombre: fac.empresa_nombre,
          empresa_rut: fac.empresa_rut,
          id_empresa: idEmp,
          total_facturas: 0,
          monto_total: 0,
          facturas: [],
        });
      }

      const grupo = grupoMap.get(key);
      grupo.total_facturas++;
      grupo.monto_total += toNumber(fac.monto_total, 0);
      grupo.facturas.push(fac);
    }

    // Recopilar centros de costo únicos de los movimientos de estas facturas
    if (facturas.length > 0) {
      const ccResult = await pool.request().query(`
        SELECT DISTINCT cc.SapCodigo
        FROM [cfl].[CabeceraFlete] cf
        INNER JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
        WHERE cf.IdFactura IN (${facturas.map(f => Number(f.id_factura)).join(',')})
      `);
      for (const row of ccResult.recordset) {
        if (row.sap_codigo) centrosCostoSet.add(row.sap_codigo);
      }
    }

    const grupos = Array.from(grupoMap.values());

    res.json({
      data: {
        resumen: {
          grupos: grupos.length,
          facturas: facturas.length,
          centros_costo: centrosCostoSet.size,
          monto_total: facturas.reduce((acc, row) => acc + toNumber(row.monto_total, 0), 0),
        },
        grupos,
      },
      permissions: buildPermissions(authzContext),
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/estadisticas/overview", async (req, res, next) => {
  try {
    const authzContext = await resolveAuthzContext(req);
    if (!ensureCanReadOperaciones(authzContext, res)) return;
    const pool = await getPool();

    // KPIs globales
    const summaryResult = await pool.request().query(`
      SELECT
        total_fletes            = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete]),
        fletes_completados      = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'COMPLETADO'),
        fletes_en_revision      = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'EN_REVISION'),
        fletes_prefacturado     = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'PREFACTURADO'),
        fletes_facturados       = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'FACTURADO'),
        monto_total_fletes      = (SELECT COALESCE(SUM(MontoAplicado), 0) FROM [cfl].[CabeceraFlete]),
        facturas_registradas    = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFactura] WHERE LOWER(estado) != 'anulada'),
        facturas_borrador       = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFactura] WHERE LOWER(estado) = 'borrador'),
        facturas_recibidas      = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFactura] WHERE LOWER(estado) = 'recibida'),
        monto_facturado         = (SELECT COALESCE(SUM(MontoTotal), 0) FROM [cfl].[CabeceraFactura] WHERE LOWER(estado) != 'anulada'),
        ticket_promedio_factura = (SELECT COALESCE(AVG(CAST(MontoTotal AS DECIMAL(18,2))), 0) FROM [cfl].[CabeceraFactura] WHERE LOWER(estado) != 'anulada'),
        total_productores       = (SELECT COUNT_BIG(1) FROM [cfl].[Productor] WHERE Activo = 1),
        total_transportistas    = (SELECT COUNT_BIG(DISTINCT mv.IdEmpresaTransporte) FROM [cfl].[CabeceraFlete] cf INNER JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil);
    `);

    // Distribución por estado
    const estadosResult = await pool.request().query(`
      SELECT
        estado = UPPER(estado),
        total  = COUNT_BIG(1),
        monto  = COALESCE(SUM(COALESCE(MontoAplicado, 0)), 0)
      FROM [cfl].[CabeceraFlete]
      GROUP BY UPPER(estado)
      ORDER BY COUNT_BIG(1) DESC;
    `);

    // Top 8 transportistas
    const transportistasResult = await pool.request().query(`
      SELECT TOP 8
        empresa_nombre    = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), 'Sin transportista'),
        total_movimientos = COUNT_BIG(1),
        monto_total       = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = mv.IdEmpresaTransporte
      GROUP BY emp.IdEmpresa, emp.RazonSocial
      ORDER BY COALESCE(SUM(cf.MontoAplicado), 0) DESC;
    `);

    // Top 8 centros de costo
    const centrosResult = await pool.request().query(`
      SELECT TOP 8
        cc.SapCodigo,
        cc.nombre,
        total_movimientos = COUNT_BIG(1),
        monto_total       = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      GROUP BY cc.IdCentroCosto, cc.SapCodigo, cc.nombre
      ORDER BY COALESCE(SUM(cf.MontoAplicado), 0) DESC;
    `);

    // Top 8 productores
    const productoresResult = await pool.request().query(`
      SELECT TOP 8
        prod.CodigoProveedor,
        prod.Nombre,
        total_movimientos = COUNT_BIG(1),
        monto_total       = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[CabeceraFlete] cf
      INNER JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
      GROUP BY prod.IdProductor, prod.CodigoProveedor, prod.Nombre
      ORDER BY COALESCE(SUM(cf.MontoAplicado), 0) DESC;
    `);

    // Top tipos de flete
    const tiposFleteResult = await pool.request().query(`
      SELECT TOP 8
        tf.nombre,
        total_movimientos = COUNT_BIG(1),
        monto_total       = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[CabeceraFlete] cf
      INNER JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      GROUP BY tf.IdTipoFlete, tf.nombre
      ORDER BY COALESCE(SUM(cf.MontoAplicado), 0) DESC;
    `);

    // Timeline 12 meses (por FechaSalida)
    const timelineResult = await pool.request().query(`
      WITH movimientos AS (
        SELECT
          periodo            = CONVERT(CHAR(7), FechaSalida, 120),
          total_fletes       = COUNT_BIG(1),
          monto_movimientos  = COALESCE(SUM(MontoAplicado), 0)
        FROM [cfl].[CabeceraFlete]
        WHERE FechaSalida IS NOT NULL
        GROUP BY CONVERT(CHAR(7), FechaSalida, 120)
      ),
      facturas AS (
        SELECT
          periodo          = CONVERT(CHAR(7), FechaEmision, 120),
          total_facturas   = COUNT_BIG(1),
          monto_facturado  = COALESCE(SUM(MontoTotal), 0)
        FROM [cfl].[CabeceraFactura]
        WHERE LOWER(estado) != 'anulada'
        GROUP BY CONVERT(CHAR(7), FechaEmision, 120)
      )
      SELECT TOP 12
        periodo           = COALESCE(m.periodo, f.periodo),
        total_fletes      = ISNULL(m.total_fletes, 0),
        monto_movimientos = ISNULL(m.monto_movimientos, 0),
        total_facturas    = ISNULL(f.total_facturas, 0),
        monto_facturado   = ISNULL(f.monto_facturado, 0)
      FROM movimientos m
      FULL OUTER JOIN facturas f ON f.periodo = m.periodo
      ORDER BY COALESCE(m.periodo, f.periodo) DESC;
    `);

    // Despacho vs Retorno
    const sentidoResult = await pool.request().query(`
      SELECT
        tipo = CASE
          WHEN UPPER(TipoMovimiento) = 'PUSH' THEN 'Despacho'
          WHEN UPPER(TipoMovimiento) = 'PULL' THEN 'Retorno'
          ELSE 'Otro'
        END,
        total = COUNT_BIG(1),
        monto = COALESCE(SUM(MontoAplicado), 0)
      FROM [cfl].[CabeceraFlete]
      WHERE TipoMovimiento IS NOT NULL
      GROUP BY CASE
        WHEN UPPER(TipoMovimiento) = 'PUSH' THEN 'Despacho'
        WHEN UPPER(TipoMovimiento) = 'PULL' THEN 'Retorno'
        ELSE 'Otro'
      END;
    `);

    res.json({
      data: {
        resumen: summaryResult.recordset[0] || {},
        estados: estadosResult.recordset,
        transportistas: transportistasResult.recordset,
        centros_costo: centrosResult.recordset,
        productores: productoresResult.recordset,
        tipos_flete: tiposFleteResult.recordset,
        sentido: sentidoResult.recordset,
        timeline: [...timelineResult.recordset].reverse(),
      },
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/auditoria/overview", async (req, res, next) => {
  const limit = clamp(parsePositiveInt(req.query.limit, 80), 10, 200);

  try {
    const authzContext = await resolveAuthzContext(req);
    if (!ensureCanReadOperaciones(authzContext, res)) return;
    const pool = await getPool();

    const summaryResult = await pool.request().query(`
      SELECT
        total_registros = COUNT_BIG(1),
        registros_hoy = SUM(
          CASE
            WHEN CAST(FechaHora AS DATE) = CAST(SYSDATETIME() AS DATE) THEN 1
            ELSE 0
          END
        ),
        usuarios_7d = COUNT(DISTINCT CASE
          WHEN FechaHora >= DATEADD(DAY, -7, SYSDATETIME()) THEN IdUsuario
          ELSE NULL
        END)
      FROM [cfl].[Auditoria];
    `);

    const entidadesResult = await pool.request().query(`
      SELECT TOP 8
        entidad,
        total = COUNT_BIG(1)
      FROM [cfl].[Auditoria]
      GROUP BY entidad
      ORDER BY COUNT_BIG(1) DESC, entidad ASC;
    `);

    const accionesResult = await pool.request().query(`
      SELECT TOP 8
        accion,
        total = COUNT_BIG(1)
      FROM [cfl].[Auditoria]
      GROUP BY accion
      ORDER BY COUNT_BIG(1) DESC, accion ASC;
    `);

    const rowsResult = await pool.request().input("limit", limit).query(`
      SELECT TOP (@limit)
        aud.IdAuditoria,
        aud.IdUsuario,
        usuario = COALESCE(
          NULLIF(LTRIM(RTRIM(CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')))), ''),
          NULLIF(LTRIM(RTRIM(u.Username)), ''),
          CONCAT('Usuario #', aud.IdUsuario)
        ),
        aud.FechaHora,
        aud.accion,
        aud.entidad,
        aud.IdEntidad,
        aud.resumen,
        aud.IpEquipo
      FROM [cfl].[Auditoria] aud
      LEFT JOIN [cfl].[Usuario] u
        ON u.IdUsuario = aud.IdUsuario
      ORDER BY aud.FechaHora DESC, aud.IdAuditoria DESC;
    `);

    res.json({
      data: {
        resumen: {
          ...(summaryResult.recordset[0] || {}),
          limit,
        },
        entidades: entidadesResult.recordset,
        acciones: accionesResult.recordset,
        registros: rowsResult.recordset,
      },
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  operacionesRouter: router,
};
