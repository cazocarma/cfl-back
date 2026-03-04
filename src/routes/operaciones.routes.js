const express = require("express");
const { getPool } = require("../db");
const { clamp, parsePositiveInt } = require("../helpers");
const { hasAnyPermission, resolveAuthContext } = require("../authz");

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

function buildPermissions(auth) {
  return {
    can_edit_facturas:
      hasAnyPermission(auth, ["facturas.editar", "facturas.conciliar"]) ||
      String(auth?.primaryRole || "").toLowerCase() === "administrador",
    can_generate_planillas:
      hasAnyPermission(auth, ["planillas.generar"]) ||
      String(auth?.primaryRole || "").toLowerCase() === "administrador",
  };
}

router.get("/facturas/overview", async (req, res, next) => {
  try {
    const auth = await resolveAuthContext(req);
    const pool = await getPool();

    const summaryResult = await pool.request().query(`
      SELECT
        facturas_registradas = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_cabecera_factura]
        ),
        folios_con_factura = (
          SELECT COUNT_BIG(DISTINCT id_folio)
          FROM [cfl].[CFL_cabecera_factura]
        ),
        folios_elegibles = (
          SELECT COUNT_BIG(1)
          FROM (
            SELECT cf.id_folio
            FROM [cfl].[CFL_cabecera_flete] cf
            INNER JOIN [cfl].[CFL_folio] fol
              ON fol.id_folio = cf.id_folio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.id_folio IS NOT NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.folio_numero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.id_folio
          ) eligible
        ),
        folios_pendientes_factura = (
          SELECT COUNT_BIG(1)
          FROM (
            SELECT cf.id_folio
            FROM [cfl].[CFL_cabecera_flete] cf
            INNER JOIN [cfl].[CFL_folio] fol
              ON fol.id_folio = cf.id_folio
            LEFT JOIN [cfl].[CFL_cabecera_factura] fac
              ON fac.id_folio = cf.id_folio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.id_folio IS NOT NULL
              AND fac.id_factura IS NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.folio_numero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.id_folio
          ) pending
        ),
        monto_facturado = (
          SELECT COALESCE(SUM(monto_total), 0)
          FROM [cfl].[CFL_cabecera_factura]
        ),
        monto_pendiente_estimado = (
          SELECT COALESCE(SUM(pending.monto_neto), 0)
          FROM (
            SELECT
              cf.id_folio,
              SUM(COALESCE(cf.monto_aplicado, 0)) AS monto_neto
            FROM [cfl].[CFL_cabecera_flete] cf
            INNER JOIN [cfl].[CFL_folio] fol
              ON fol.id_folio = cf.id_folio
            LEFT JOIN [cfl].[CFL_cabecera_factura] fac
              ON fac.id_folio = cf.id_folio
            WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
              AND cf.id_folio IS NOT NULL
              AND fac.id_factura IS NULL
              AND ISNULL(LTRIM(RTRIM(CAST(fol.folio_numero AS NVARCHAR(50)))), '') <> '0'
            GROUP BY cf.id_folio
          ) pending
        );
    `);

    const facturasResult = await pool.request().query(`
      SELECT TOP 100
        fac.id_factura,
        fac.id_folio,
        fol.folio_numero,
        fac.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), CONCAT('Empresa #', fac.id_empresa)),
        fac.numero_factura,
        fac.fecha_emision,
        fac.moneda,
        fac.monto_neto,
        fac.monto_iva,
        fac.monto_total,
        fac.estado,
        fac.ruta_xml,
        fac.ruta_pdf,
        fac.created_at,
        fac.updated_at,
        fol.periodo_desde,
        fol.periodo_hasta,
        cc.id_centro_costo,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.sap_codigo,
        total_detalles = ISNULL(det.total_detalles, 0),
        total_movimientos = ISNULL(mov.total_movimientos, 0),
        movimientos_conciliados = ISNULL(con.total_conciliados, 0)
      FROM [cfl].[CFL_cabecera_factura] fac
      INNER JOIN [cfl].[CFL_folio] fol
        ON fol.id_folio = fac.id_folio
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp
        ON emp.id_empresa = fac.id_empresa
      LEFT JOIN [cfl].[CFL_centro_costo] cc
        ON cc.id_centro_costo = fol.id_centro_costo
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_detalles
        FROM [cfl].[CFL_detalle_factura] df
        WHERE df.id_factura = fac.id_factura
      ) det
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_movimientos
        FROM [cfl].[CFL_cabecera_flete] cf
        WHERE cf.id_folio = fac.id_folio
      ) mov
      OUTER APPLY (
        SELECT COUNT_BIG(1) AS total_conciliados
        FROM [cfl].[CFL_conciliacion_factura_flete] cff
        WHERE cff.id_factura = fac.id_factura
      ) con
      ORDER BY fac.fecha_emision DESC, fac.id_factura DESC;
    `);

    const eligibleFoliosResult = await pool.request().query(`
      WITH movimientos AS (
        SELECT
          cf.id_folio,
          cf.id_cabecera_flete,
          cf.fecha_salida,
          cf.monto_aplicado,
          mv.id_empresa_transporte,
          emp.razon_social AS empresa_nombre
        FROM [cfl].[CFL_cabecera_flete] cf
        INNER JOIN [cfl].[CFL_folio] fol
          ON fol.id_folio = cf.id_folio
        LEFT JOIN [cfl].[CFL_movil] mv
          ON mv.id_movil = cf.id_movil
        LEFT JOIN [cfl].[CFL_empresa_transporte] emp
          ON emp.id_empresa = mv.id_empresa_transporte
        WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
          AND cf.id_folio IS NOT NULL
          AND ISNULL(LTRIM(RTRIM(CAST(fol.folio_numero AS NVARCHAR(50)))), '') <> '0'
      ),
      empresa_top AS (
        SELECT
          m.id_folio,
          m.id_empresa_transporte,
          empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(m.empresa_nombre)), ''), CONCAT('Empresa #', m.id_empresa_transporte)),
          rn = ROW_NUMBER() OVER (
            PARTITION BY m.id_folio
            ORDER BY COUNT_BIG(1) DESC, m.id_empresa_transporte ASC
          )
        FROM movimientos m
        WHERE m.id_empresa_transporte IS NOT NULL
        GROUP BY m.id_folio, m.id_empresa_transporte, m.empresa_nombre
      )
      SELECT
        m.id_folio,
        fol.folio_numero,
        estado_folio = fol.estado,
        fol.periodo_desde,
        fol.periodo_hasta,
        fol.bloqueado,
        fol.id_centro_costo,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.sap_codigo,
        total_movimientos = COUNT_BIG(1),
        monto_neto_estimado = SUM(COALESCE(m.monto_aplicado, 0)),
        fecha_primer_movimiento = MIN(m.fecha_salida),
        fecha_ultimo_movimiento = MAX(m.fecha_salida),
        empresa.id_empresa_transporte AS id_empresa,
        empresa.empresa_nombre,
        fac.id_factura AS id_factura_existente,
        fac.numero_factura AS numero_factura_existente,
        fac.estado AS estado_factura_existente
      FROM movimientos m
      INNER JOIN [cfl].[CFL_folio] fol
        ON fol.id_folio = m.id_folio
      LEFT JOIN [cfl].[CFL_centro_costo] cc
        ON cc.id_centro_costo = fol.id_centro_costo
      LEFT JOIN empresa_top empresa
        ON empresa.id_folio = m.id_folio
       AND empresa.rn = 1
      LEFT JOIN [cfl].[CFL_cabecera_factura] fac
        ON fac.id_folio = m.id_folio
      GROUP BY
        m.id_folio,
        fol.folio_numero,
        fol.estado,
        fol.periodo_desde,
        fol.periodo_hasta,
        fol.bloqueado,
        fol.id_centro_costo,
        cc.nombre,
        cc.sap_codigo,
        empresa.id_empresa_transporte,
        empresa.empresa_nombre,
        fac.id_factura,
        fac.numero_factura,
        fac.estado
      ORDER BY MAX(m.fecha_salida) DESC, m.id_folio DESC;
    `);

    res.json({
      data: {
        resumen: summaryResult.recordset[0] || {},
        facturas: facturasResult.recordset,
        folios_disponibles: eligibleFoliosResult.recordset,
      },
      permissions: buildPermissions(auth),
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
    const auth = await resolveAuthContext(req);
    const pool = await getPool();

    const folioResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT TOP 1
        fol.id_folio,
        fol.folio_numero,
        fol.estado,
        fol.bloqueado,
        fol.periodo_desde,
        fol.periodo_hasta,
        fol.id_centro_costo,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.sap_codigo,
        fol.created_at,
        fol.updated_at
      FROM [cfl].[CFL_folio] fol
      LEFT JOIN [cfl].[CFL_centro_costo] cc
        ON cc.id_centro_costo = fol.id_centro_costo
      WHERE fol.id_folio = @idFolio;
    `);

    const folio = folioResult.recordset[0] || null;
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    const facturaResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT TOP 1
        fac.id_factura,
        fac.id_folio,
        fac.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), CONCAT('Empresa #', fac.id_empresa)),
        fac.numero_factura,
        fac.fecha_emision,
        fac.moneda,
        fac.monto_neto,
        fac.monto_iva,
        fac.monto_total,
        fac.estado,
        fac.ruta_xml,
        fac.ruta_pdf,
        fac.created_at,
        fac.updated_at
      FROM [cfl].[CFL_cabecera_factura] fac
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp
        ON emp.id_empresa = fac.id_empresa
      WHERE fac.id_folio = @idFolio
      ORDER BY fac.fecha_emision DESC, fac.id_factura DESC;
    `);

    const movimientosResult = await pool.request().input("idFolio", idFolio).query(`
      SELECT
        cf.id_cabecera_flete,
        cf.numero_entrega,
        cf.sap_numero_entrega,
        cf.fecha_salida,
        cf.monto_aplicado,
        cf.estado,
        cf.tipo_movimiento,
        ruta = COALESCE(
          r.nombre_ruta,
          CASE
            WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
              THEN CONCAT(COALESCE(no.nombre, 'Origen'), ' -> ', COALESCE(nd.nombre, 'Destino'))
            ELSE 'Ruta sin definir'
          END
        ),
        transportista = COALESCE(
          NULLIF(LTRIM(RTRIM(emp.razon_social)), ''),
          'Transportista sin definir'
        ),
        conciliado = CASE WHEN cff.id_conciliacion IS NULL THEN 0 ELSE 1 END
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_tarifa] tf
        ON tf.id_tarifa = cf.id_tarifa
      LEFT JOIN [cfl].[CFL_ruta] r
        ON r.id_ruta = tf.id_ruta
      LEFT JOIN [cfl].[CFL_nodo_logistico] no
        ON no.id_nodo = r.id_origen_nodo
      LEFT JOIN [cfl].[CFL_nodo_logistico] nd
        ON nd.id_nodo = r.id_destino_nodo
      LEFT JOIN [cfl].[CFL_movil] mv
        ON mv.id_movil = cf.id_movil
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp
        ON emp.id_empresa = mv.id_empresa_transporte
      LEFT JOIN [cfl].[CFL_conciliacion_factura_flete] cff
        ON cff.id_cabecera_flete = cf.id_cabecera_flete
      WHERE cf.id_folio = @idFolio
      ORDER BY cf.fecha_salida ASC, cf.id_cabecera_flete ASC;
    `);

    const existingFactura = facturaResult.recordset[0] || null;

    if (existingFactura) {
      const detailResult = await pool
        .request()
        .input("idFactura", Number(existingFactura.id_factura))
        .query(`
          SELECT
            id_factura_detalle,
            monto_linea,
            detalle
          FROM [cfl].[CFL_detalle_factura]
          WHERE id_factura = @idFactura
          ORDER BY id_factura_detalle ASC;
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
        permissions: buildPermissions(auth),
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
      permissions: buildPermissions(auth),
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

router.get("/planillas-sap/overview", async (req, res, next) => {
  try {
    const auth = await resolveAuthContext(req);
    const pool = await getPool();

    const invoicesResult = await pool.request().query(`
      SELECT
        fac.id_factura,
        fac.id_folio,
        fol.folio_numero,
        fac.numero_factura,
        fac.fecha_emision,
        fac.estado,
        fac.moneda,
        fac.monto_neto,
        fac.monto_iva,
        fac.monto_total,
        fac.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), CONCAT('Empresa #', fac.id_empresa)),
        fol.periodo_desde,
        fol.periodo_hasta,
        fol.id_centro_costo,
        centro_costo = cc.nombre,
        centro_costo_codigo = cc.sap_codigo
      FROM [cfl].[CFL_cabecera_factura] fac
      INNER JOIN [cfl].[CFL_folio] fol
        ON fol.id_folio = fac.id_folio
      LEFT JOIN [cfl].[CFL_centro_costo] cc
        ON cc.id_centro_costo = fol.id_centro_costo
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp
        ON emp.id_empresa = fac.id_empresa
      ORDER BY fac.fecha_emision DESC, fac.id_factura DESC;
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
      permissions: buildPermissions(auth),
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
        total_fletes = (SELECT COUNT_BIG(1) FROM [cfl].[CFL_cabecera_flete]),
        fletes_en_revision = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_cabecera_flete]
          WHERE UPPER(estado) = 'EN_REVISION'
        ),
        fletes_asignado_folio = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_cabecera_flete]
          WHERE UPPER(estado) = 'ASIGNADO_FOLIO'
        ),
        fletes_facturados = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_cabecera_flete]
          WHERE UPPER(estado) = 'FACTURADO'
        ),
        folios_abiertos = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_folio]
          WHERE UPPER(estado) = 'ABIERTO'
        ),
        facturas_registradas = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_cabecera_factura]
        ),
        monto_facturado = (
          SELECT COALESCE(SUM(monto_total), 0)
          FROM [cfl].[CFL_cabecera_factura]
        ),
        ticket_promedio_factura = (
          SELECT COALESCE(AVG(CAST(monto_total AS DECIMAL(18, 2))), 0)
          FROM [cfl].[CFL_cabecera_factura]
        );
    `);

    const estadosResult = await pool.request().query(`
      SELECT
        estado = UPPER(estado),
        total = COUNT_BIG(1),
        monto = COALESCE(SUM(COALESCE(monto_aplicado, 0)), 0)
      FROM [cfl].[CFL_cabecera_flete]
      GROUP BY UPPER(estado)
      ORDER BY COUNT_BIG(1) DESC, estado ASC;
    `);

    const transportistasResult = await pool.request().query(`
      SELECT TOP 6
        emp.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), 'Sin transportista'),
        total_movimientos = COUNT_BIG(1),
        monto_total = COALESCE(SUM(COALESCE(cf.monto_aplicado, 0)), 0)
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_movil] mv
        ON mv.id_movil = cf.id_movil
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp
        ON emp.id_empresa = mv.id_empresa_transporte
      GROUP BY emp.id_empresa, emp.razon_social
      ORDER BY COALESCE(SUM(COALESCE(cf.monto_aplicado, 0)), 0) DESC, COUNT_BIG(1) DESC;
    `);

    const centrosResult = await pool.request().query(`
      SELECT TOP 6
        cc.id_centro_costo,
        cc.sap_codigo,
        cc.nombre,
        total_movimientos = COUNT_BIG(1),
        monto_total = COALESCE(SUM(COALESCE(cf.monto_aplicado, 0)), 0)
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_centro_costo] cc
        ON cc.id_centro_costo = cf.id_centro_costo
      GROUP BY cc.id_centro_costo, cc.sap_codigo, cc.nombre
      ORDER BY COALESCE(SUM(COALESCE(cf.monto_aplicado, 0)), 0) DESC, COUNT_BIG(1) DESC;
    `);

    const timelineResult = await pool.request().query(`
      WITH movimientos AS (
        SELECT
          periodo = CONVERT(CHAR(7), created_at, 120),
          total_fletes = COUNT_BIG(1),
          monto_movimientos = COALESCE(SUM(COALESCE(monto_aplicado, 0)), 0)
        FROM [cfl].[CFL_cabecera_flete]
        GROUP BY CONVERT(CHAR(7), created_at, 120)
      ),
      facturas AS (
        SELECT
          periodo = CONVERT(CHAR(7), fecha_emision, 120),
          total_facturas = COUNT_BIG(1),
          monto_facturado = COALESCE(SUM(COALESCE(monto_total, 0)), 0)
        FROM [cfl].[CFL_cabecera_factura]
        GROUP BY CONVERT(CHAR(7), fecha_emision, 120)
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
            WHEN CAST(fecha_hora AS DATE) = CAST(SYSDATETIME() AS DATE) THEN 1
            ELSE 0
          END
        ),
        usuarios_7d = COUNT(DISTINCT CASE
          WHEN fecha_hora >= DATEADD(DAY, -7, SYSDATETIME()) THEN id_usuario
          ELSE NULL
        END)
      FROM [cfl].[CFL_auditoria];
    `);

    const entidadesResult = await pool.request().query(`
      SELECT TOP 8
        entidad,
        total = COUNT_BIG(1)
      FROM [cfl].[CFL_auditoria]
      GROUP BY entidad
      ORDER BY COUNT_BIG(1) DESC, entidad ASC;
    `);

    const accionesResult = await pool.request().query(`
      SELECT TOP 8
        accion,
        total = COUNT_BIG(1)
      FROM [cfl].[CFL_auditoria]
      GROUP BY accion
      ORDER BY COUNT_BIG(1) DESC, accion ASC;
    `);

    const rowsResult = await pool.request().input("limit", limit).query(`
      SELECT TOP (@limit)
        aud.id_auditoria,
        aud.id_usuario,
        usuario = COALESCE(
          NULLIF(LTRIM(RTRIM(CONCAT(COALESCE(u.nombre, ''), ' ', COALESCE(u.apellido, '')))), ''),
          NULLIF(LTRIM(RTRIM(u.username)), ''),
          CONCAT('Usuario #', aud.id_usuario)
        ),
        aud.fecha_hora,
        aud.accion,
        aud.entidad,
        aud.id_entidad,
        aud.resumen,
        aud.ip_equipo
      FROM [cfl].[CFL_auditoria] aud
      LEFT JOIN [cfl].[CFL_usuario] u
        ON u.id_usuario = aud.id_usuario
      ORDER BY aud.fecha_hora DESC, aud.id_auditoria DESC;
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
