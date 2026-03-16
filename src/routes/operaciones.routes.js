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

function buildPermissions(authzContext) {
  return {
    can_edit_facturas:
      hasAnyPermission(authzContext, ["facturas.editar", "facturas.conciliar"]) ||
      String(authzContext?.primaryRole || "").toLowerCase() === "administrador",
    can_generate_planillas:
      hasAnyPermission(authzContext, ["planillas.generar"]) ||
      String(authzContext?.primaryRole || "").toLowerCase() === "administrador",
  };
}

router.get("/facturas/overview", async (req, res, next) => {
  try {
    const authzContext = await resolveAuthzContext(req);
    const pool = await getPool();

    const summaryResult = await pool.request().query(`
      SELECT
        facturas_registradas = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFactura]
        ),
        folios_con_factura = (
          SELECT COUNT_BIG(DISTINCT IdFolio)
          FROM [cfl].[CabeceraFactura]
        ),
        folios_elegibles = (
          SELECT COUNT_BIG(1)
          FROM (
            SELECT cf.IdFolio
            FROM [cfl].[CabeceraFlete] cf
            INNER JOIN [cfl].[Folio] fol
              ON fol.IdFolio = cf.IdFolio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.IdFolio IS NOT NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.IdFolio
          ) eligible
        ),
        folios_pendientes_factura = (
          SELECT COUNT_BIG(1)
          FROM (
            SELECT cf.IdFolio
            FROM [cfl].[CabeceraFlete] cf
            INNER JOIN [cfl].[Folio] fol
              ON fol.IdFolio = cf.IdFolio
            LEFT JOIN [cfl].[CabeceraFactura] fac
              ON fac.IdFolio = cf.IdFolio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.IdFolio IS NOT NULL
              AND fac.IdFactura IS NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.IdFolio
          ) pending
        ),
        monto_facturado = (
          SELECT COALESCE(SUM(MontoTotal), 0)
          FROM [cfl].[CabeceraFactura]
        ),
        monto_pendiente_estimado = (
          SELECT COALESCE(SUM(pending.MontoNeto), 0)
          FROM (
            SELECT
              cf.IdFolio,
              SUM(COALESCE(cf.MontoAplicado, 0)) AS MontoNeto
            FROM [cfl].[CabeceraFlete] cf
            INNER JOIN [cfl].[Folio] fol
              ON fol.IdFolio = cf.IdFolio
            LEFT JOIN [cfl].[CabeceraFactura] fac
              ON fac.IdFolio = cf.IdFolio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.IdFolio IS NOT NULL
              AND fac.IdFactura IS NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.IdFolio
          ) pending
        );
    `);

    const facturasResult = await pool.request().query(`
      SELECT TOP 100
        fac.IdFactura,
        fac.IdFolio,
        fol.FolioNumero,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.RutaXml,
        fac.RutaPdf,
        fac.FechaCreacion,
        fac.FechaActualizacion,
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        cc.IdCentroCosto,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
        total_detalles = ISNULL(det.total_detalles, 0),
        total_movimientos = ISNULL(mov.total_movimientos, 0),
        movimientos_conciliados = ISNULL(con.total_conciliados, 0)
      FROM [cfl].[CabeceraFactura] fac
      INNER JOIN [cfl].[Folio] fol
        ON fol.IdFolio = fac.IdFolio
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = fol.IdCentroCosto
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_detalles
        FROM [cfl].[DetalleFactura] df
        WHERE df.IdFactura = fac.IdFactura
      ) det
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_movimientos
        FROM [cfl].[CabeceraFlete] cf
        WHERE cf.IdFolio = fac.IdFolio
      ) mov
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_conciliados
        FROM [cfl].[ConciliacionFacturaFlete] cff
        WHERE cff.IdFactura = fac.IdFactura
      ) con
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    const eligibleFoliosResult = await pool.request().query(`
      WITH movimientos AS (
        SELECT
          cf.IdFolio,
          cf.IdCabeceraFlete,
          cf.FechaSalida,
          cf.MontoAplicado,
          mv.IdEmpresaTransporte,
          emp.RazonSocial AS empresa_nombre
        FROM [cfl].[CabeceraFlete] cf
        INNER JOIN [cfl].[Folio] fol
          ON fol.IdFolio = cf.IdFolio
        LEFT JOIN [cfl].[Movil] mv
          ON mv.IdMovil = cf.IdMovil
        LEFT JOIN [cfl].[EmpresaTransporte] emp
          ON emp.IdEmpresa = mv.IdEmpresaTransporte
        WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
          AND cf.IdFolio IS NOT NULL
          AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
      ),
      empresa_top AS (
        SELECT
          m.IdFolio,
          m.IdEmpresaTransporte,
          empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(m.empresa_nombre)), ''), CONCAT('Empresa #', m.IdEmpresaTransporte)),
          rn = ROW_NUMBER() OVER (
            PARTITION BY m.IdFolio
            ORDER BY COUNT_BIG(1) DESC, m.IdEmpresaTransporte ASC
          )
        FROM movimientos m
        WHERE m.IdEmpresaTransporte IS NOT NULL
        GROUP BY m.IdFolio, m.IdEmpresaTransporte, m.empresa_nombre
      )
      SELECT
        m.IdFolio,
        fol.FolioNumero,
        estado_folio = fol.estado,
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        fol.bloqueado,
        fol.IdCentroCosto,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
        total_movimientos = COUNT_BIG(1),
        monto_neto_estimado = SUM(COALESCE(m.MontoAplicado, 0)),
        fecha_primer_movimiento = MIN(m.FechaSalida),
        fecha_ultimo_movimiento = MAX(m.FechaSalida),
        empresa.IdEmpresaTransporte AS IdEmpresa,
        empresa.empresa_nombre,
        fac.IdFactura AS id_factura_existente,
        fac.NumeroFactura AS numero_factura_existente,
        fac.estado AS estado_factura_existente
      FROM movimientos m
      INNER JOIN [cfl].[Folio] fol
        ON fol.IdFolio = m.IdFolio
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = fol.IdCentroCosto
      LEFT JOIN empresa_top empresa
        ON empresa.IdFolio = m.IdFolio
       AND empresa.rn = 1
      LEFT JOIN [cfl].[CabeceraFactura] fac
        ON fac.IdFolio = m.IdFolio
      GROUP BY
        m.IdFolio,
        fol.FolioNumero,
        fol.estado,
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        fol.bloqueado,
        fol.IdCentroCosto,
        cc.nombre,
        cc.SapCodigo,
        empresa.IdEmpresaTransporte,
        empresa.empresa_nombre,
        fac.IdFactura,
        fac.NumeroFactura,
        fac.estado
      ORDER BY MAX(m.FechaSalida) DESC, m.IdFolio DESC;
    `);

    res.json({
      data: {
        resumen: summaryResult.recordset[0] || {},
        facturas: facturasResult.recordset,
        folios_disponibles: eligibleFoliosResult.recordset,
      },
      permissions: buildPermissions(authzContext),
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/facturas/folios/:idFolio", async (req, res, next) => {
  const idFolio = parsePositiveInt(req.params.idFolio, 0);
  if (!idFolio) {
    res.status(400).json({ error: "idFolio invalido" });
    return;
  }

  try {
    const authzContext = await resolveAuthzContext(req);
    const pool = await getPool();

    const folioResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT TOP 1
        fol.IdFolio,
        fol.FolioNumero,
        fol.estado,
        fol.bloqueado,
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        fol.IdCentroCosto,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
        fol.FechaCreacion,
        fol.FechaActualizacion
      FROM [cfl].[Folio] fol
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = fol.IdCentroCosto
      WHERE fol.IdFolio = @idFolio;
    `);

    const folio = folioResult.recordset[0] || null;
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    const facturaResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT TOP 1
        fac.IdFactura,
        fac.IdFolio,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.RutaXml,
        fac.RutaPdf,
        fac.FechaCreacion,
        fac.FechaActualizacion
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      WHERE fac.IdFolio = @idFolio
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    const movimientosResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.NumeroEntrega,
        cf.SapNumeroEntrega,
        cf.FechaSalida,
        cf.MontoAplicado,
        cf.estado,
        cf.TipoMovimiento,
        ruta = COALESCE(
          r.NombreRuta,
          CASE
            WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
              THEN CONCAT(COALESCE(no.nombre, 'Origen'), ' -> ', COALESCE(nd.nombre, 'Destino'))
            ELSE 'Ruta sin definir'
          END
        ),
        transportista = COALESCE(
          NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''),
          'Transportista sin definir'
        ),
        conciliado = CASE WHEN cff.IdConciliacion IS NULL THEN 0 ELSE 1 END
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[Tarifa] tf
        ON tf.IdTarifa = cf.IdTarifa
      LEFT JOIN [cfl].[Ruta] r
        ON r.IdRuta = tf.IdRuta
      LEFT JOIN [cfl].[NodoLogistico] no
        ON no.IdNodo = r.IdOrigenNodo
      LEFT JOIN [cfl].[NodoLogistico] nd
        ON nd.IdNodo = r.IdDestinoNodo
      LEFT JOIN [cfl].[Movil] mv
        ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = mv.IdEmpresaTransporte
      LEFT JOIN [cfl].[ConciliacionFacturaFlete] cff
        ON cff.IdCabeceraFlete = cf.IdCabeceraFlete
      WHERE cf.IdFolio = @idFolio
      ORDER BY cf.FechaSalida ASC, cf.IdCabeceraFlete ASC;
    `);

    const existingFactura = facturaResult.recordset[0] || null;

    if (existingFactura) {
      const detailResult = await pool
        .request()
        .input("idFactura", Number(existingFactura.id_factura))
        .query(`
          SELECT
            IdFacturaDetalle,
            MontoLinea,
            detalle
          FROM [cfl].[DetalleFactura]
          WHERE IdFactura = @idFactura
          ORDER BY IdFacturaDetalle ASC;
        `);

      res.json({
        data: {
          source: "database",
          folio,
          cabecera: existingFactura,
          detalle: detailResult.recordset,
          movimientos: movimientosResult.recordset,
          resumen: {
            total_movimientos: movimientosResult.recordset.length,
            total_detalles: detailResult.recordset.length,
            monto_neto: existingFactura.monto_neto,
            monto_iva: existingFactura.monto_iva,
            monto_total: existingFactura.monto_total,
          },
        },
        permissions: buildPermissions(authzContext),
        generated_at: new Date().toISOString(),
      });
      return;
    }

    const elegibleMovimientos = movimientosResult.recordset.filter(
      (row) => String(row?.estado || "").toUpperCase() === "ASIGNADO_FOLIO"
    );

    const montoNeto = elegibleMovimientos.reduce(
      (acc, row) => acc + toNumber(row?.monto_aplicado, 0),
      0
    );
    const montoIva = Math.round(montoNeto * 0.19 * 100) / 100;
    const montoTotal = Math.round((montoNeto + montoIva) * 100) / 100;

    const empresaMap = new Map();
    for (const row of elegibleMovimientos) {
      const name = String(row?.transportista || "").trim();
      if (!name) continue;
      empresaMap.set(name, (empresaMap.get(name) || 0) + 1);
    }
    const empresaNombre =
      Array.from(empresaMap.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || null;

    const draftDetails = elegibleMovimientos.map((row) => {
      const entrega = String(row?.numero_entrega || row?.sap_numero_entrega || "").trim();
      const entregaLabel = entrega ? `Entrega ${entrega}` : `Flete ${row.id_cabecera_flete}`;
      const ruta = String(row?.ruta || "Ruta sin definir").trim();
      const movimiento = String(row?.tipo_movimiento || "").trim() || "Movimiento";

      return {
        id_factura_detalle: null,
        monto_linea: row?.monto_aplicado ?? 0,
        detalle: `${entregaLabel} | ${movimiento} | ${ruta}`,
        id_cabecera_flete: row?.id_cabecera_flete ?? null,
      };
    });

    res.json({
      data: {
        source: "draft",
        folio,
        cabecera: {
          id_factura: null,
          id_folio: folio.id_folio,
          id_empresa: null,
          empresa_nombre: empresaNombre,
          numero_factura: `PEND-${folio.folio_numero}`,
          fecha_emision: new Date().toISOString(),
          moneda: "CLP",
          monto_neto: montoNeto,
          monto_iva: montoIva,
          monto_total: montoTotal,
          estado: "BORRADOR",
          ruta_xml: null,
          ruta_pdf: null,
        },
        detalle: draftDetails,
        movimientos: elegibleMovimientos,
        resumen: {
          total_movimientos: elegibleMovimientos.length,
          total_detalles: draftDetails.length,
          monto_neto: montoNeto,
          monto_iva: montoIva,
          monto_total: montoTotal,
        },
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
    const pool = await getPool();

    const invoicesResult = await pool.request().query(`
      SELECT
        fac.IdFactura,
        fac.IdFolio,
        fol.FolioNumero,
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.estado,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        fol.IdCentroCosto,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.SapCodigo
      FROM [cfl].[CabeceraFactura] fac
      INNER JOIN [cfl].[Folio] fol
        ON fol.IdFolio = fac.IdFolio
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = fol.IdCentroCosto
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    const groups = new Map();

    for (const invoice of invoicesResult.recordset) {
      const start =
        invoice.periodo_desde instanceof Date
          ? invoice.periodo_desde.toISOString().slice(0, 10)
          : String(invoice.periodo_desde || "").slice(0, 10);
      const end =
        invoice.periodo_hasta instanceof Date
          ? invoice.periodo_hasta.toISOString().slice(0, 10)
          : String(invoice.periodo_hasta || "").slice(0, 10);
      const centroCostoId = String(invoice.id_centro_costo || "0");
      const key = `${centroCostoId}:${start}:${end}`;

      if (!groups.has(key)) {
        groups.set(key, {
          group_key: key,
          periodo_desde: invoice.periodo_desde,
          periodo_hasta: invoice.periodo_hasta,
          periodo_label: start && end ? `${start} al ${end}` : start || end || "Sin periodo",
          id_centro_costo: invoice.id_centro_costo,
          centro_costo: invoice.centro_costo,
          centro_costo_codigo: invoice.centro_costo_codigo,
          total_facturas: 0,
          total_folios: 0,
          monto_neto: 0,
          monto_iva: 0,
          monto_total: 0,
          empresas: new Set(),
          facturas: [],
        });
      }

      const group = groups.get(key);
      group.total_facturas += 1;
      group.monto_neto += toNumber(invoice.monto_neto, 0);
      group.monto_iva += toNumber(invoice.monto_iva, 0);
      group.monto_total += toNumber(invoice.monto_total, 0);
      if (invoice.empresa_nombre) {
        group.empresas.add(String(invoice.empresa_nombre));
      }
      group.facturas.push(invoice);
    }

    const normalizedGroups = Array.from(groups.values()).map((group) => {
      group.total_folios = uniqueBy(group.facturas, "id_folio").length;
      return {
        ...group,
        empresas: Array.from(group.empresas.values()),
      };
    });

    res.json({
      data: {
        resumen: {
          grupos: normalizedGroups.length,
          facturas: invoicesResult.recordset.length,
          centros_costo: uniqueBy(invoicesResult.recordset, "id_centro_costo").length,
          monto_total: normalizedGroups.reduce(
            (acc, group) => acc + toNumber(group.monto_total, 0),
            0
          ),
        },
        grupos: normalizedGroups,
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
    const pool = await getPool();

    const summaryResult = await pool.request().query(`
      SELECT
        total_fletes = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete]),
        fletes_en_revision = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFlete]
          WHERE UPPER(estado) = 'EN_REVISION'
        ),
        fletes_asignado_folio = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFlete]
          WHERE UPPER(estado) = 'ASIGNADO_FOLIO'
        ),
        fletes_facturados = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFlete]
          WHERE UPPER(estado) = 'FACTURADO'
        ),
        folios_abiertos = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[Folio]
          WHERE UPPER(estado) = 'ABIERTO'
        ),
        facturas_registradas = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFactura]
        ),
        monto_facturado = (
          SELECT COALESCE(SUM(MontoTotal), 0)
          FROM [cfl].[CabeceraFactura]
        ),
        ticket_promedio_factura = (
          SELECT COALESCE(AVG(CAST(MontoTotal AS DECIMAL(18, 2))), 0)
          FROM [cfl].[CabeceraFactura]
        );
    `);

    const estadosResult = await pool.request().query(`
      SELECT
        estado = UPPER(estado),
        total = COUNT_BIG(1),
        monto = COALESCE(SUM(COALESCE(MontoAplicado, 0)), 0)
      FROM [cfl].[CabeceraFlete]
      GROUP BY UPPER(estado)
      ORDER BY COUNT_BIG(1) DESC, estado ASC;
    `);

    const transportistasResult = await pool.request().query(`
      SELECT TOP 6
        emp.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), 'Sin transportista'),
        total_movimientos = COUNT_BIG(1),
        MontoTotal = COALESCE(SUM(COALESCE(cf.MontoAplicado, 0)), 0)
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[Movil] mv
        ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = mv.IdEmpresaTransporte
      GROUP BY emp.IdEmpresa, emp.RazonSocial
      ORDER BY COALESCE(SUM(COALESCE(cf.MontoAplicado, 0)), 0) DESC, COUNT_BIG(1) DESC;
    `);

    const centrosResult = await pool.request().query(`
      SELECT TOP 6
        cc.IdCentroCosto,
        cc.SapCodigo,
        cc.nombre,
        total_movimientos = COUNT_BIG(1),
        MontoTotal = COALESCE(SUM(COALESCE(cf.MontoAplicado, 0)), 0)
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = cf.IdCentroCosto
      GROUP BY cc.IdCentroCosto, cc.SapCodigo, cc.nombre
      ORDER BY COALESCE(SUM(COALESCE(cf.MontoAplicado, 0)), 0) DESC, COUNT_BIG(1) DESC;
    `);

    const timelineResult = await pool.request().query(`
      WITH movimientos AS (
        SELECT
          periodo = CONVERT(CHAR(7), FechaCreacion, 120),
          total_fletes = COUNT_BIG(1),
          monto_movimientos = COALESCE(SUM(COALESCE(MontoAplicado, 0)), 0)
        FROM [cfl].[CabeceraFlete]
        GROUP BY CONVERT(CHAR(7), FechaCreacion, 120)
      ),
      facturas AS (
        SELECT
          periodo = CONVERT(CHAR(7), FechaEmision, 120),
          total_facturas = COUNT_BIG(1),
          monto_facturado = COALESCE(SUM(COALESCE(MontoTotal, 0)), 0)
        FROM [cfl].[CabeceraFactura]
        GROUP BY CONVERT(CHAR(7), FechaEmision, 120)
      )
      SELECT TOP 6
        periodo = COALESCE(m.periodo, f.periodo),
        total_fletes = ISNULL(m.total_fletes, 0),
        monto_movimientos = ISNULL(m.monto_movimientos, 0),
        total_facturas = ISNULL(f.total_facturas, 0),
        monto_facturado = ISNULL(f.monto_facturado, 0)
      FROM movimientos m
      FULL OUTER JOIN facturas f
        ON f.periodo = m.periodo
      ORDER BY COALESCE(m.periodo, f.periodo) DESC;
    `);

    res.json({
      data: {
        resumen: summaryResult.recordset[0] || {},
        estados: estadosResult.recordset,
        transportistas: transportistasResult.recordset,
        centros_costo: centrosResult.recordset,
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
