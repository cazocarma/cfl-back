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
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.IdFolio IS NOT NULL
              AND cf.IdFolio NOT IN (
                SELECT ff.IdFolio FROM [cfl].[FacturaFolio] ff
                INNER JOIN [cfl].[CabeceraFactura] fac2 ON fac2.IdFactura = ff.IdFactura
                WHERE LOWER(fac2.estado) != 'anulada'
              )
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
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.IdFolio IS NOT NULL
              AND cf.IdFolio NOT IN (
                SELECT ff.IdFolio FROM [cfl].[FacturaFolio] ff
                INNER JOIN [cfl].[CabeceraFactura] fac2 ON fac2.IdFactura = ff.IdFactura
                WHERE LOWER(fac2.estado) != 'anulada'
              )
              AND ISNULL(LTRIM(RTRIM(CAST(fol.FolioNumero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.IdFolio
          ) pending
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
        fac.FechaActualizacion,
        total_folios = (
          SELECT COUNT_BIG(1) FROM [cfl].[FacturaFolio] ff2
          WHERE ff2.IdFactura = fac.IdFactura
        ),
        total_movimientos = (
          SELECT COUNT_BIG(1) FROM [cfl].[FacturaFolio] ff3
          INNER JOIN [cfl].[CabeceraFlete] cf3 ON cf3.IdFolio = ff3.IdFolio
          WHERE ff3.IdFactura = fac.IdFactura
        )
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
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
      OUTER APPLY (
        SELECT TOP 1 fac2.IdFactura, fac2.NumeroFactura, fac2.estado
        FROM [cfl].[FacturaFolio] ff2
        INNER JOIN [cfl].[CabeceraFactura] fac2 ON fac2.IdFactura = ff2.IdFactura
        WHERE ff2.IdFolio = m.IdFolio AND LOWER(fac2.estado) != 'anulada'
        ORDER BY fac2.IdFactura DESC
      ) fac
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
      FROM [cfl].[FacturaFolio] ff
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = ff.IdFactura
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      WHERE ff.IdFolio = @idFolio AND LOWER(fac.estado) != 'anulada'
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
      res.json({
        data: {
          source: "database",
          folio,
          cabecera: existingFactura,
          detalle: [],
          movimientos: movimientosResult.recordset,
          resumen: {
            total_movimientos: movimientosResult.recordset.length,
            total_detalles: 0,
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

    // Query a nivel de folio con montos calculados desde CabeceraFlete
    // para evitar duplicación cuando una factura tiene múltiples folios
    const invoicesResult = await pool.request().query(`
      SELECT
        fac.IdFactura,
        ff.IdFolio,
        fol.FolioNumero,
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.estado,
        fac.moneda,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        fol.IdCentroCosto,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
        folio_monto_neto = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[CabeceraFactura] fac
      INNER JOIN [cfl].[FacturaFolio] ff
        ON ff.IdFactura = fac.IdFactura
      INNER JOIN [cfl].[Folio] fol
        ON fol.IdFolio = ff.IdFolio
      LEFT JOIN [cfl].[CabeceraFlete] cf
        ON cf.IdFolio = fol.IdFolio
      LEFT JOIN [cfl].[CentroCosto] cc
        ON cc.IdCentroCosto = fol.IdCentroCosto
      LEFT JOIN [cfl].[EmpresaTransporte] emp
        ON emp.IdEmpresa = fac.IdEmpresa
      WHERE LOWER(fac.estado) != 'anulada'
      GROUP BY
        fac.IdFactura, ff.IdFolio, fol.FolioNumero,
        fac.NumeroFactura, fac.FechaEmision, fac.estado, fac.moneda,
        fac.IdEmpresa, emp.RazonSocial,
        fol.PeriodoDesde, fol.PeriodoHasta, fol.IdCentroCosto,
        cc.nombre, cc.SapCodigo
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    const groups = new Map();

    for (const row of invoicesResult.recordset) {
      const start =
        row.periodo_desde instanceof Date
          ? row.periodo_desde.toISOString().slice(0, 10)
          : String(row.periodo_desde || "").slice(0, 10);
      const end =
        row.periodo_hasta instanceof Date
          ? row.periodo_hasta.toISOString().slice(0, 10)
          : String(row.periodo_hasta || "").slice(0, 10);
      const centroCostoId = String(row.id_centro_costo || "0");
      const key = `${centroCostoId}:${start}:${end}`;

      if (!groups.has(key)) {
        groups.set(key, {
          group_key: key,
          periodo_desde: row.periodo_desde,
          periodo_hasta: row.periodo_hasta,
          periodo_label: start && end ? `${start} al ${end}` : start || end || "Sin periodo",
          id_centro_costo: row.id_centro_costo,
          centro_costo: row.centro_costo,
          centro_costo_codigo: row.centro_costo_codigo,
          total_facturas: 0,
          total_folios: 0,
          monto_neto: 0,
          monto_total: 0,
          empresas: new Set(),
          _factura_ids: new Set(),
          facturas: [],
        });
      }

      const group = groups.get(key);
      const folioNeto = toNumber(row.folio_monto_neto, 0);
      group.monto_neto += folioNeto;
      group.monto_total += folioNeto;
      group.total_folios += 1;

      // Contar facturas únicas (no duplicar por cada folio)
      if (!group._factura_ids.has(row.id_factura)) {
        group._factura_ids.add(row.id_factura);
        group.total_facturas += 1;
      }

      if (row.empresa_nombre) {
        group.empresas.add(String(row.empresa_nombre));
      }
      group.facturas.push(row);
    }

    const normalizedGroups = Array.from(groups.values()).map((group) => {
      const { _factura_ids, ...rest } = group;
      return {
        ...rest,
        empresas: Array.from(group.empresas.values()),
      };
    });

    res.json({
      data: {
        resumen: {
          grupos: normalizedGroups.length,
          facturas: uniqueBy(invoicesResult.recordset, "id_factura").length,
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

    // KPIs globales
    const summaryResult = await pool.request().query(`
      SELECT
        total_fletes            = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete]),
        fletes_completados      = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'COMPLETADO'),
        fletes_en_revision      = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'EN_REVISION'),
        fletes_asignado_folio   = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'ASIGNADO_FOLIO'),
        fletes_facturados       = (SELECT COUNT_BIG(1) FROM [cfl].[CabeceraFlete] WHERE UPPER(estado) = 'FACTURADO'),
        monto_total_fletes      = (SELECT COALESCE(SUM(MontoAplicado), 0) FROM [cfl].[CabeceraFlete]),
        folios_abiertos         = (SELECT COUNT_BIG(1) FROM [cfl].[Folio] WHERE UPPER(estado) = 'ABIERTO'),
        total_folios            = (SELECT COUNT_BIG(1) FROM [cfl].[Folio]),
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
