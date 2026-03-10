'use strict';

const express = require('express');
const { getPool, sql } = require('../db');
const { resolveAuthContext, hasAnyPermission } = require('../authz');
const { parsePositiveInt } = require('../helpers');

const router = express.Router();

// ---------------------------------------------------------------------------
// Utilidades internas
// ---------------------------------------------------------------------------

function toN(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function calcMontos(movimientos) {
  const montoNeto = movimientos.reduce((s, m) => s + toN(m.monto_aplicado), 0);
  const montoIva = Math.round(montoNeto * 0.19 * 100) / 100;
  const montoTotal = Math.round((montoNeto + montoIva) * 100) / 100;
  return { montoNeto, montoIva, montoTotal };
}

/** Verifica que el usuario tenga permiso de facturación o sea administrador. */
async function checkFacturacionPerm(req) {
  const auth = await resolveAuthContext(req);
  const isAdmin = String(auth?.primaryRole || '').toLowerCase() === 'administrador';
  const hasPerm = hasAnyPermission(auth, ['facturacion', 'facturas.editar', 'facturas.generar']);
  return { auth, allowed: isAdmin || hasPerm };
}

/**
 * Calcula la agrupación de folios según criterio y devuelve el detalle de
 * grupos que se generarán (usado tanto en preview como en generar).
 */
async function computePreview(pool, idEmpresa, idsFolio, criterio) {
  if (!idsFolio.length) return [];

  // Datos de folios con CC y tipo_flete primario
  const folReq = pool.request().input('idEmpresa', sql.BigInt, idEmpresa);
  idsFolio.forEach((id, i) => folReq.input(`fol${i}`, sql.BigInt, id));
  const inClause = idsFolio.map((_, i) => `@fol${i}`).join(',');

  const folioData = await folReq.query(`
    SELECT
      f.id_folio,
      f.folio_numero,
      f.id_centro_costo,
      cc.nombre  AS centro_costo,
      cc.sap_codigo AS centro_costo_codigo,
      primary_tipo_flete_id = (
        SELECT TOP 1 cf2.id_tipo_flete
        FROM [cfl].[CFL_cabecera_flete] cf2
        WHERE cf2.id_folio = f.id_folio
          AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
        GROUP BY cf2.id_tipo_flete
        ORDER BY COUNT(*) DESC
      ),
      primary_tipo_flete_nombre = (
        SELECT TOP 1 tf2.nombre
        FROM [cfl].[CFL_cabecera_flete] cf2
        INNER JOIN [cfl].[CFL_tipo_flete] tf2 ON tf2.id_tipo_flete = cf2.id_tipo_flete
        WHERE cf2.id_folio = f.id_folio
          AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
        GROUP BY cf2.id_tipo_flete, tf2.nombre
        ORDER BY COUNT(*) DESC
      )
    FROM [cfl].[CFL_folio] f
    LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = f.id_centro_costo
    WHERE f.id_folio IN (${inClause});
  `);

  // Movimientos elegibles de esos folios
  const movReq = pool.request();
  idsFolio.forEach((id, i) => movReq.input(`fol${i}`, sql.BigInt, id));

  const movData = await movReq.query(`
    SELECT
      cf.id_cabecera_flete,
      cf.id_folio,
      cf.sap_numero_entrega,
      cf.numero_entrega,
      cf.guia_remision,
      cf.tipo_movimiento,
      cf.fecha_salida,
      cf.monto_aplicado,
      cf.id_tipo_flete,
      tf.nombre  AS tipo_flete_nombre,
      tf.sap_codigo AS tipo_flete_codigo,
      cf.id_centro_costo,
      cc.nombre  AS centro_costo,
      cc.sap_codigo AS centro_costo_codigo,
      ruta = COALESCE(r.nombre_ruta,
        CASE WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
          THEN CONCAT(COALESCE(no.nombre,'Origen'), ' -> ', COALESCE(nd.nombre,'Destino'))
          ELSE NULL END
      ),
      empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), NULL)
    FROM [cfl].[CFL_cabecera_flete] cf
    LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
    LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo
    LEFT JOIN [cfl].[CFL_movil] mv ON mv.id_movil = cf.id_movil
    LEFT JOIN [cfl].[CFL_empresa_transporte] emp ON emp.id_empresa = mv.id_empresa_transporte
    LEFT JOIN [cfl].[CFL_tarifa] tar ON tar.id_tarifa = cf.id_tarifa
    LEFT JOIN [cfl].[CFL_ruta] r ON r.id_ruta = tar.id_ruta
    LEFT JOIN [cfl].[CFL_nodo_logistico] no ON no.id_nodo = r.id_origen_nodo
    LEFT JOIN [cfl].[CFL_nodo_logistico] nd ON nd.id_nodo = r.id_destino_nodo
    WHERE cf.id_folio IN (${inClause})
      AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
    ORDER BY cf.fecha_salida, cf.id_cabecera_flete;
  `);

  const folioMap = new Map(folioData.recordset.map(f => [Number(f.id_folio), f]));
  const movsByFolio = new Map();
  for (const m of movData.recordset) {
    const key = Number(m.id_folio);
    if (!movsByFolio.has(key)) movsByFolio.set(key, []);
    movsByFolio.get(key).push(m);
  }

  // Agrupar folios por criterio
  const groups = new Map();
  for (const folioId of idsFolio) {
    const folio = folioMap.get(folioId);
    if (!folio) continue;

    let groupKey, groupLabel;
    if (criterio === 'centro_costo') {
      groupKey = String(folio.id_centro_costo || 'sin-cc');
      groupLabel = folio.centro_costo
        ? `${folio.centro_costo_codigo || ''} · ${folio.centro_costo}`
        : 'Sin Centro de Costo';
    } else {
      groupKey = String(folio.primary_tipo_flete_id || 'sin-tf');
      groupLabel = folio.primary_tipo_flete_nombre || 'Sin Tipo de Flete';
    }

    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        grupo_clave: groupKey,
        grupo_label: groupLabel,
        ids_folio: [],
        folios: [],
        movimientos: [],
      });
    }
    const g = groups.get(groupKey);
    g.ids_folio.push(folioId);
    g.folios.push(folio);
    g.movimientos.push(...(movsByFolio.get(folioId) || []));
  }

  return Array.from(groups.values()).map(g => {
    const { montoNeto, montoIva, montoTotal } = calcMontos(g.movimientos);
    return {
      ...g,
      monto_neto: montoNeto,
      monto_iva: montoIva,
      monto_total: montoTotal,
      cantidad_movimientos: g.movimientos.length,
    };
  });
}

/** Carga una factura completa (cabecera + folios + movimientos). */
async function fetchFactura(pool, idFactura) {
  const facResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        fac.id_factura,
        fac.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), CONCAT('Empresa #', fac.id_empresa)),
        emp.rut AS empresa_rut,
        fac.numero_factura,
        fac.fecha_emision,
        fac.moneda,
        fac.monto_neto,
        fac.monto_iva,
        fac.monto_total,
        fac.estado,
        fac.criterio_agrupacion,
        fac.observaciones,
        fac.created_at,
        fac.updated_at
      FROM [cfl].[CFL_cabecera_factura] fac
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp ON emp.id_empresa = fac.id_empresa
      WHERE fac.id_factura = @idFactura;
    `);

  if (!facResult.recordset[0]) return null;
  const factura = facResult.recordset[0];

  // Folios asociados via bridge
  const foliosResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        ff.id_factura_folio,
        ff.id_folio,
        fol.folio_numero,
        fol.estado AS estado_folio,
        fol.id_centro_costo,
        cc.nombre  AS centro_costo,
        cc.sap_codigo AS centro_costo_codigo,
        fol.periodo_desde,
        fol.periodo_hasta,
        total_movimientos       = COUNT_BIG(cf.id_cabecera_flete),
        monto_total_movimientos = COALESCE(SUM(cf.monto_aplicado), 0)
      FROM [cfl].[CFL_factura_folio] ff
      INNER JOIN [cfl].[CFL_folio] fol ON fol.id_folio = ff.id_folio
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = fol.id_centro_costo
      LEFT JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_folio = ff.id_folio
      WHERE ff.id_factura = @idFactura
      GROUP BY
        ff.id_factura_folio, ff.id_folio, fol.folio_numero, fol.estado,
        fol.id_centro_costo, cc.nombre, cc.sap_codigo,
        fol.periodo_desde, fol.periodo_hasta
      ORDER BY ff.id_factura_folio;
    `);

  // Movimientos de todos los folios de esta factura
  const movResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        cf.id_cabecera_flete,
        cf.id_folio,
        fol.folio_numero,
        cf.sap_numero_entrega,
        cf.numero_entrega,
        cf.guia_remision,
        cf.tipo_movimiento,
        cf.estado,
        cf.fecha_salida,
        cf.monto_aplicado,
        tipo_flete_nombre  = tf.nombre,
        tipo_flete_codigo  = tf.sap_codigo,
        centro_costo       = cc.nombre,
        centro_costo_codigo = cc.sap_codigo,
        ruta = COALESCE(r.nombre_ruta,
          CASE WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
            THEN CONCAT(COALESCE(no.nombre,'Origen'), ' -> ', COALESCE(nd.nombre,'Destino'))
            ELSE NULL END
        ),
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), NULL)
      FROM [cfl].[CFL_factura_folio] ff
      INNER JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_folio = ff.id_folio
      INNER JOIN [cfl].[CFL_folio] fol ON fol.id_folio = cf.id_folio
      LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo
      LEFT JOIN [cfl].[CFL_movil] mv ON mv.id_movil = cf.id_movil
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp ON emp.id_empresa = mv.id_empresa_transporte
      LEFT JOIN [cfl].[CFL_tarifa] tar ON tar.id_tarifa = cf.id_tarifa
      LEFT JOIN [cfl].[CFL_ruta] r ON r.id_ruta = tar.id_ruta
      LEFT JOIN [cfl].[CFL_nodo_logistico] no ON no.id_nodo = r.id_origen_nodo
      LEFT JOIN [cfl].[CFL_nodo_logistico] nd ON nd.id_nodo = r.id_destino_nodo
      WHERE ff.id_factura = @idFactura
      ORDER BY cf.fecha_salida, cf.id_cabecera_flete;
    `);

  return { ...factura, folios: foliosResult.recordset, movimientos: movResult.recordset };
}

// ===========================================================================
// RUTAS
// IMPORTANTE: rutas estáticas ANTES de /:id
// ===========================================================================

// ---------------------------------------------------------------------------
// GET /facturas/empresas-elegibles
// Empresas con al menos un folio en estado ASIGNADO_FOLIO sin factura activa
// ---------------------------------------------------------------------------
router.get('/empresas-elegibles', async (req, res, next) => {
  try {
    const pool = await getPool();

    const result = await pool.request().query(`
      SELECT DISTINCT
        et.id_empresa,
        et.rut,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(et.razon_social)), ''), CONCAT('Empresa #', et.id_empresa)),
        et.sap_codigo,
        folios_disponibles = (
          SELECT COUNT(DISTINCT f2.id_folio)
          FROM [cfl].[CFL_folio] f2
          INNER JOIN [cfl].[CFL_cabecera_flete] cf2 ON cf2.id_folio = f2.id_folio
          INNER JOIN [cfl].[CFL_movil] mv2 ON mv2.id_movil = cf2.id_movil
          WHERE mv2.id_empresa_transporte = et.id_empresa
            AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            AND f2.id_folio NOT IN (
              SELECT ff2.id_folio
              FROM [cfl].[CFL_factura_folio] ff2
              INNER JOIN [cfl].[CFL_cabecera_factura] fac2 ON fac2.id_factura = ff2.id_factura
              WHERE LOWER(fac2.estado) != 'anulada'
            )
        )
      FROM [cfl].[CFL_empresa_transporte] et
      INNER JOIN [cfl].[CFL_movil] mv ON mv.id_empresa_transporte = et.id_empresa
      INNER JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_movil = mv.id_movil
      INNER JOIN [cfl].[CFL_folio] f ON f.id_folio = cf.id_folio
      WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
        AND et.activo = 1
        AND f.id_folio NOT IN (
          SELECT ff.id_folio
          FROM [cfl].[CFL_factura_folio] ff
          INNER JOIN [cfl].[CFL_cabecera_factura] fac ON fac.id_factura = ff.id_factura
          WHERE LOWER(fac.estado) != 'anulada'
        )
      ORDER BY empresa_nombre;
    `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/folios-elegibles?id_empresa=X
// Folios elegibles para una empresa (usados en Paso 2 del wizard)
// ---------------------------------------------------------------------------
router.get('/folios-elegibles', async (req, res, next) => {
  const idEmpresa = parsePositiveInt(req.query.id_empresa, 0);
  if (!idEmpresa) {
    res.status(400).json({ error: 'id_empresa requerido' });
    return;
  }

  try {
    const pool = await getPool();

    const result = await pool.request()
      .input('idEmpresa', sql.BigInt, idEmpresa)
      .query(`
        WITH eligible_folios AS (
          SELECT DISTINCT
            f.id_folio
          FROM [cfl].[CFL_folio] f
          INNER JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_folio = f.id_folio
          INNER JOIN [cfl].[CFL_movil] mv ON mv.id_movil = cf.id_movil
          WHERE mv.id_empresa_transporte = @idEmpresa
            AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
            AND f.id_folio NOT IN (
              SELECT ff.id_folio
              FROM [cfl].[CFL_factura_folio] ff
              INNER JOIN [cfl].[CFL_cabecera_factura] fac ON fac.id_factura = ff.id_factura
              WHERE LOWER(fac.estado) != 'anulada'
            )
        )
        SELECT
          f.id_folio,
          f.folio_numero,
          f.estado AS estado_folio,
          f.id_centro_costo,
          cc.nombre  AS centro_costo,
          cc.sap_codigo AS centro_costo_codigo,
          f.periodo_desde,
          f.periodo_hasta,
          total_movimientos       = COUNT_BIG(cf.id_cabecera_flete),
          monto_neto_estimado     = COALESCE(SUM(cf.monto_aplicado), 0),
          primary_tipo_flete_id = (
            SELECT TOP 1 cf2.id_tipo_flete
            FROM [cfl].[CFL_cabecera_flete] cf2
            WHERE cf2.id_folio = f.id_folio AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            GROUP BY cf2.id_tipo_flete ORDER BY COUNT(*) DESC
          ),
          primary_tipo_flete_nombre = (
            SELECT TOP 1 tf2.nombre
            FROM [cfl].[CFL_cabecera_flete] cf2
            INNER JOIN [cfl].[CFL_tipo_flete] tf2 ON tf2.id_tipo_flete = cf2.id_tipo_flete
            WHERE cf2.id_folio = f.id_folio AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            GROUP BY cf2.id_tipo_flete, tf2.nombre ORDER BY COUNT(*) DESC
          )
        FROM eligible_folios ef
        INNER JOIN [cfl].[CFL_folio] f ON f.id_folio = ef.id_folio
        LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = f.id_centro_costo
        LEFT JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_folio = f.id_folio AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
        GROUP BY
          f.id_folio, f.folio_numero, f.estado, f.id_centro_costo,
          cc.nombre, cc.sap_codigo, f.periodo_desde, f.periodo_hasta
        ORDER BY f.folio_numero;
      `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/preview
// Calcula cuántas facturas se generarán antes de confirmar
// ---------------------------------------------------------------------------
router.post('/preview', async (req, res, next) => {
  const { id_empresa, ids_folio, criterio } = req.body || {};

  if (!id_empresa || !Array.isArray(ids_folio) || ids_folio.length === 0) {
    res.status(400).json({ error: 'Faltan id_empresa o ids_folio' });
    return;
  }
  if (criterio !== 'centro_costo' && criterio !== 'tipo_flete') {
    res.status(400).json({ error: 'criterio debe ser "centro_costo" o "tipo_flete"' });
    return;
  }

  try {
    const pool = await getPool();
    const grupos = await computePreview(pool, Number(id_empresa), ids_folio.map(Number), criterio);
    res.json({
      data: {
        criterio,
        cantidad_facturas: grupos.length,
        grupos,
      },
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/generar
// Confirma generación: persiste facturas y marca fletes como FACTURADO
// ---------------------------------------------------------------------------
router.post('/generar', async (req, res, next) => {
  const { id_empresa, ids_folio, criterio } = req.body || {};

  if (!id_empresa || !Array.isArray(ids_folio) || ids_folio.length === 0) {
    res.status(400).json({ error: 'Faltan id_empresa o ids_folio' });
    return;
  }
  if (criterio !== 'centro_costo' && criterio !== 'tipo_flete') {
    res.status(400).json({ error: 'criterio debe ser "centro_costo" o "tipo_flete"' });
    return;
  }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) {
    res.status(403).json({ error: 'Sin permiso de facturación' });
    return;
  }

  let transaction;
  try {
    const pool = await getPool();
    // Calcular grupos fuera de transacción (read-only)
    const grupos = await computePreview(pool, Number(id_empresa), ids_folio.map(Number), criterio);

    if (!grupos.length) {
      res.status(422).json({ error: 'No hay grupos de movimientos elegibles para los folios seleccionados' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();
    const createdIds = [];

    for (const grupo of grupos) {
      const { montoNeto, montoIva, montoTotal } = calcMontos(grupo.movimientos);

      // 1. Insertar cabecera con número temporal
      const insertFac = await new sql.Request(transaction)
        .input('idEmpresa',  sql.BigInt,       Number(id_empresa))
        .input('numTemp',    sql.VarChar(40),   'PEND')
        .input('fechaEm',    sql.DateTime2(0),  now)
        .input('moneda',     sql.Char(3),       'CLP')
        .input('montoNeto',  sql.Decimal(18,2), montoNeto)
        .input('montoIva',   sql.Decimal(18,2), montoIva)
        .input('montoTotal', sql.Decimal(18,2), montoTotal)
        .input('estado',     sql.VarChar(20),   'borrador')
        .input('criterio',   sql.VarChar(30),   criterio)
        .input('createdAt',  sql.DateTime2(0),  now)
        .input('updatedAt',  sql.DateTime2(0),  now)
        .query(`
          INSERT INTO [cfl].[CFL_cabecera_factura]
            (id_empresa, numero_factura, fecha_emision, moneda, monto_neto, monto_iva,
             monto_total, estado, criterio_agrupacion, created_at, updated_at)
          OUTPUT INSERTED.id_factura
          VALUES
            (@idEmpresa, @numTemp, @fechaEm, @moneda, @montoNeto, @montoIva,
             @montoTotal, @estado, @criterio, @createdAt, @updatedAt);
        `);

      const idFactura = Number(insertFac.recordset[0].id_factura);

      // 2. Actualizar número definitivo: INT-XXXXXX
      const numeroFactura = `INT-${String(idFactura).padStart(6, '0')}`;
      await new sql.Request(transaction)
        .input('idFactura',      sql.BigInt,    idFactura)
        .input('numeroFactura',  sql.VarChar(40), numeroFactura)
        .input('updatedAt',      sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CFL_cabecera_factura]
          SET numero_factura = @numeroFactura, updated_at = @updatedAt
          WHERE id_factura = @idFactura;
        `);

      createdIds.push(idFactura);

      // 3. Bridge folio → factura
      for (const folioId of grupo.ids_folio) {
        await new sql.Request(transaction)
          .input('idFactura', sql.BigInt, idFactura)
          .input('idFolio',   sql.BigInt, folioId)
          .input('createdAt', sql.DateTime2(0), now)
          .query(`
            INSERT INTO [cfl].[CFL_factura_folio] (id_factura, id_folio, created_at)
            VALUES (@idFactura, @idFolio, @createdAt);
          `);
      }

      // 4. Marcar fletes como FACTURADO
      const folioParts = grupo.ids_folio.map((_, i) => `@ff${i}`).join(',');
      const updReq = new sql.Request(transaction);
      updReq.input('updatedAt', sql.DateTime2(0), now);
      grupo.ids_folio.forEach((fId, i) => updReq.input(`ff${i}`, sql.BigInt, fId));
      await updReq.query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET estado = 'FACTURADO', updated_at = @updatedAt
        WHERE id_folio IN (${folioParts})
          AND UPPER(estado) = 'ASIGNADO_FOLIO';
      `);
    }

    await transaction.commit();
    res.status(201).json({ data: { ids_factura: createdIds, total: createdIds.length } });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas — lista con filtros opcionales
// ---------------------------------------------------------------------------
router.get('/', async (req, res, next) => {
  try {
    const pool = await getPool();
    const { empresa, estado, desde, hasta } = req.query;

    let where = 'WHERE 1=1';
    const request = pool.request();

    if (empresa) {
      request.input('empresa', sql.BigInt, Number(empresa));
      where += ' AND fac.id_empresa = @empresa';
    }
    if (estado) {
      request.input('estado', sql.VarChar(20), String(estado).toLowerCase());
      where += ' AND LOWER(fac.estado) = @estado';
    }
    if (desde) {
      request.input('desde', sql.Date, String(desde));
      where += ' AND CAST(fac.fecha_emision AS DATE) >= @desde';
    }
    if (hasta) {
      request.input('hasta', sql.Date, String(hasta));
      where += ' AND CAST(fac.fecha_emision AS DATE) <= @hasta';
    }

    const result = await request.query(`
      SELECT
        fac.id_factura,
        fac.id_empresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.razon_social)), ''), CONCAT('Empresa #', fac.id_empresa)),
        fac.numero_factura,
        fac.fecha_emision,
        fac.moneda,
        fac.monto_neto,
        fac.monto_iva,
        fac.monto_total,
        fac.estado,
        fac.criterio_agrupacion,
        fac.observaciones,
        fac.created_at,
        fac.updated_at,
        cantidad_folios = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CFL_factura_folio] ff
          WHERE ff.id_factura = fac.id_factura
        )
      FROM [cfl].[CFL_cabecera_factura] fac
      LEFT JOIN [cfl].[CFL_empresa_transporte] emp ON emp.id_empresa = fac.id_empresa
      ${where}
      ORDER BY fac.fecha_emision DESC, fac.id_factura DESC;
    `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id
// ---------------------------------------------------------------------------
router.get('/:id', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) {
      res.status(404).json({ error: 'Factura no encontrada' });
      return;
    }
    res.json({ data: factura });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/:id/folios — agregar folios a factura en Borrador
// ---------------------------------------------------------------------------
router.post('/:id/folios', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const { ids_folio } = req.body || {};
  if (!Array.isArray(ids_folio) || ids_folio.length === 0) {
    res.status(400).json({ error: 'ids_folio requerido' });
    return;
  }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se pueden agregar folios a facturas en estado Borrador' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    for (const folioId of ids_folio.map(Number)) {
      await new sql.Request(transaction)
        .input('idFactura', sql.BigInt, idFactura)
        .input('idFolio',   sql.BigInt, folioId)
        .input('createdAt', sql.DateTime2(0), now)
        .query(`
          IF NOT EXISTS (
            SELECT 1 FROM [cfl].[CFL_factura_folio]
            WHERE id_factura = @idFactura AND id_folio = @idFolio
          )
          INSERT INTO [cfl].[CFL_factura_folio] (id_factura, id_folio, created_at)
          VALUES (@idFactura, @idFolio, @createdAt);
        `);

      // Marcar fletes de este folio como FACTURADO
      await new sql.Request(transaction)
        .input('idFolio',    sql.BigInt, folioId)
        .input('updatedAt',  sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CFL_cabecera_flete]
          SET estado = 'FACTURADO', updated_at = @updatedAt
          WHERE id_folio = @idFolio AND UPPER(estado) = 'ASIGNADO_FOLIO';
        `);
    }

    // Recalcular montos de la factura
    await recalcularMontos(transaction, idFactura, now);

    await transaction.commit();
    res.json({ message: 'Folios agregados correctamente' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// DELETE /facturas/:id/folios/:folio_id — quitar folio de factura Borrador
// ---------------------------------------------------------------------------
router.delete('/:id/folios/:folio_id', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  const idFolio   = parsePositiveInt(req.params.folio_id, 0);
  if (!idFactura || !idFolio) {
    res.status(400).json({ error: 'Parámetros inválidos' });
    return;
  }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se pueden quitar folios de facturas en estado Borrador' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, idFactura)
      .input('idFolio',   sql.BigInt, idFolio)
      .query(`
        DELETE FROM [cfl].[CFL_factura_folio]
        WHERE id_factura = @idFactura AND id_folio = @idFolio;
      `);

    // Devolver fletes al estado ASIGNADO_FOLIO
    await new sql.Request(transaction)
      .input('idFolio',   sql.BigInt, idFolio)
      .input('updatedAt', sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET estado = 'ASIGNADO_FOLIO', updated_at = @updatedAt
        WHERE id_folio = @idFolio AND UPPER(estado) = 'FACTURADO';
      `);

    await recalcularMontos(transaction, idFactura, now);

    await transaction.commit();
    res.json({ message: 'Folio quitado correctamente' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PUT /facturas/:id — editar cabecera (solo Borrador)
// ---------------------------------------------------------------------------
router.put('/:id', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  const { observaciones, criterio_agrupacion } = req.body || {};

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se puede editar una factura en estado Borrador' });
      return;
    }

    const now = new Date();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    await new sql.Request(transaction)
      .input('idFactura',          sql.BigInt,    idFactura)
      .input('observaciones',      sql.VarChar(500), observaciones ?? null)
      .input('criterioAgrupacion', sql.VarChar(30),  criterio_agrupacion ?? factura.criterio_agrupacion)
      .input('updatedAt',          sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_factura]
        SET observaciones      = @observaciones,
            criterio_agrupacion = @criterioAgrupacion,
            updated_at          = @updatedAt
        WHERE id_factura = @idFactura;
      `);

    await transaction.commit();
    res.json({ message: 'Factura actualizada' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PATCH /facturas/:id/estado — transición de estado
// Body: { estado: 'emitida' | 'anulada' }
// ---------------------------------------------------------------------------
router.patch('/:id/estado', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const nuevoEstado = String(req.body?.estado || '').toLowerCase();
  if (nuevoEstado !== 'emitida' && nuevoEstado !== 'anulada') {
    res.status(400).json({ error: 'estado debe ser "emitida" o "anulada"' });
    return;
  }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }

    // Validar transición de estado
    if (nuevoEstado === 'emitida' && factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se puede emitir una factura en estado Borrador' });
      return;
    }
    if (nuevoEstado === 'anulada' && factura.estado === 'anulada') {
      res.status(409).json({ error: 'La factura ya está anulada' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    await new sql.Request(transaction)
      .input('idFactura',  sql.BigInt,    idFactura)
      .input('estado',     sql.VarChar(20), nuevoEstado)
      .input('updatedAt',  sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_factura]
        SET estado = @estado, updated_at = @updatedAt
        WHERE id_factura = @idFactura;
      `);

    // Si se anula: devolver fletes de todos los folios a ASIGNADO_FOLIO
    if (nuevoEstado === 'anulada') {
      const folioIds = factura.folios.map(f => f.id_folio);
      if (folioIds.length) {
        const inParts = folioIds.map((_, i) => `@af${i}`).join(',');
        const updReq = new sql.Request(transaction);
        updReq.input('updatedAt', sql.DateTime2(0), now);
        folioIds.forEach((fId, i) => updReq.input(`af${i}`, sql.BigInt, fId));
        await updReq.query(`
          UPDATE [cfl].[CFL_cabecera_flete]
          SET estado = 'ASIGNADO_FOLIO', updated_at = @updatedAt
          WHERE id_folio IN (${inParts})
            AND UPPER(estado) = 'FACTURADO';
        `);
      }
    }

    await transaction.commit();
    res.json({ message: `Factura ${nuevoEstado} exitosamente` });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id/export/excel
// ---------------------------------------------------------------------------
router.get('/:id/export/excel', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) { res.status(400).json({ error: 'id_factura inválido' }); return; }

  try {
    const ExcelJS = require('exceljs');
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }

    const wb = new ExcelJS.Workbook();
    wb.creator = 'CFL System';
    wb.created = new Date();

    // Hoja 1 — Cabecera de factura
    const shCab = wb.addWorksheet('Cabecera');
    shCab.columns = [
      { header: 'N° Factura',          key: 'numero_factura',     width: 20 },
      { header: 'Empresa',             key: 'empresa_nombre',     width: 30 },
      { header: 'RUT',                 key: 'empresa_rut',        width: 15 },
      { header: 'Fecha Emisión',       key: 'fecha_emision',      width: 18 },
      { header: 'Moneda',              key: 'moneda',             width: 8  },
      { header: 'Criterio Agrupación', key: 'criterio_agrupacion',width: 20 },
      { header: 'Monto Neto',          key: 'monto_neto',         width: 15 },
      { header: 'IVA',                 key: 'monto_iva',          width: 15 },
      { header: 'Monto Total',         key: 'monto_total',        width: 15 },
      { header: 'Estado',              key: 'estado',             width: 12 },
      { header: 'Observaciones',       key: 'observaciones',      width: 40 },
    ];
    shCab.getRow(1).font = { bold: true };
    shCab.addRow({
      numero_factura:      factura.numero_factura,
      empresa_nombre:      factura.empresa_nombre,
      empresa_rut:         factura.empresa_rut,
      fecha_emision:       factura.fecha_emision,
      moneda:              factura.moneda,
      criterio_agrupacion: factura.criterio_agrupacion || '-',
      monto_neto:          Number(factura.monto_neto),
      monto_iva:           Number(factura.monto_iva),
      monto_total:         Number(factura.monto_total),
      estado:              factura.estado,
      observaciones:       factura.observaciones || '',
    });

    // Hoja 2 — Detalle de movimientos
    const shDet = wb.addWorksheet('Movimientos');
    shDet.columns = [
      { header: 'N° Guía',          key: 'guia_remision',        width: 15 },
      { header: 'Entrega SAP',      key: 'sap_numero_entrega',   width: 15 },
      { header: 'Folio',            key: 'folio_numero',         width: 14 },
      { header: 'Tipo Flete',       key: 'tipo_flete_nombre',    width: 22 },
      { header: 'Centro de Costo',  key: 'centro_costo',         width: 22 },
      { header: 'Ruta',             key: 'ruta',                 width: 30 },
      { header: 'Empresa Transp.',  key: 'empresa_nombre',       width: 25 },
      { header: 'Fecha Salida',     key: 'fecha_salida',         width: 14 },
      { header: 'Monto',            key: 'monto_aplicado',       width: 15 },
    ];
    shDet.getRow(1).font = { bold: true };

    let total = 0;
    for (const m of factura.movimientos) {
      shDet.addRow({
        guia_remision:      m.guia_remision || m.sap_numero_entrega || '-',
        sap_numero_entrega: m.sap_numero_entrega || '-',
        folio_numero:       m.folio_numero || '-',
        tipo_flete_nombre:  m.tipo_flete_nombre || '-',
        centro_costo:       m.centro_costo || '-',
        ruta:               m.ruta || '-',
        empresa_nombre:     m.empresa_nombre || '-',
        fecha_salida:       m.fecha_salida,
        monto_aplicado:     Number(m.monto_aplicado) || 0,
      });
      total += Number(m.monto_aplicado) || 0;
    }

    // Fila de total
    const totRow = shDet.addRow({ centro_costo: 'TOTAL', monto_aplicado: total });
    totRow.font = { bold: true };

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="factura-${factura.numero_factura}.xlsx"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id/export/pdf
// ---------------------------------------------------------------------------
router.get('/:id/export/pdf', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) { res.status(400).json({ error: 'id_factura inválido' }); return; }

  try {
    const PDFDocument = require('pdfkit');
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Factura no encontrada' }); return; }

    const doc = new PDFDocument({ size: 'A4', margin: 50 });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="factura-${factura.numero_factura}.pdf"`);
    doc.pipe(res);

    const CLPFormat = (n) =>
      `$${Number(n || 0).toLocaleString('es-CL', { maximumFractionDigits: 0 })}`;
    const dateFormat = (d) =>
      d ? new Date(d).toLocaleDateString('es-CL') : '-';

    // Encabezado
    doc.fontSize(18).font('Helvetica-Bold').text('CFL — Control de Fletes', 50, 50);
    doc.fontSize(14).text(`Factura ${factura.numero_factura}`, { align: 'right' });
    doc.moveDown(0.5);
    doc.fontSize(10).font('Helvetica');

    // Datos cabecera
    doc.text(`Empresa: ${factura.empresa_nombre}  |  RUT: ${factura.empresa_rut || '-'}`);
    doc.text(`Fecha emisión: ${dateFormat(factura.fecha_emision)}  |  Estado: ${factura.estado}`);
    doc.text(`Criterio agrupación: ${factura.criterio_agrupacion || '-'}  |  Moneda: ${factura.moneda}`);
    if (factura.observaciones) doc.text(`Observaciones: ${factura.observaciones}`);

    doc.moveDown();
    doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke();
    doc.moveDown(0.5);

    // Tabla de movimientos
    doc.font('Helvetica-Bold').fontSize(9);
    const cols = [50, 130, 200, 280, 360, 450];
    doc.text('N° Guía',     cols[0], doc.y, { width: 75, continued: true });
    doc.text('Folio',       cols[1], doc.y, { width: 65, continued: true });
    doc.text('Tipo Flete',  cols[2], doc.y, { width: 75, continued: true });
    doc.text('Centro Costo',cols[3], doc.y, { width: 75, continued: true });
    doc.text('Fecha',       cols[4], doc.y, { width: 80, continued: true });
    doc.text('Monto',       cols[5], doc.y, { width: 90, align: 'right' });
    doc.moveDown(0.3);
    doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke();
    doc.moveDown(0.3);

    doc.font('Helvetica').fontSize(8);
    for (const m of factura.movimientos) {
      const y = doc.y;
      doc.text(m.guia_remision || m.sap_numero_entrega || '-', cols[0], y, { width: 75, continued: true });
      doc.text(m.folio_numero || '-',                          cols[1], y, { width: 65, continued: true });
      doc.text(m.tipo_flete_nombre || '-',                     cols[2], y, { width: 75, continued: true });
      doc.text(m.centro_costo || '-',                          cols[3], y, { width: 75, continued: true });
      doc.text(dateFormat(m.fecha_salida),                     cols[4], y, { width: 80, continued: true });
      doc.text(CLPFormat(m.monto_aplicado),                    cols[5], y, { width: 90, align: 'right' });
      doc.moveDown(0.4);

      // Nueva página si hace falta
      if (doc.y > 720) { doc.addPage(); }
    }

    // Totales
    doc.moveDown(0.5);
    doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke();
    doc.moveDown(0.5);
    doc.font('Helvetica-Bold').fontSize(9);
    doc.text(`Monto Neto: ${CLPFormat(factura.monto_neto)}`, { align: 'right' });
    doc.text(`IVA (19%): ${CLPFormat(factura.monto_iva)}`,   { align: 'right' });
    doc.text(`Total: ${CLPFormat(factura.monto_total)}`,     { align: 'right' });

    // Pie de página
    doc.fontSize(8).font('Helvetica').text(
      `Generado el ${new Date().toLocaleDateString('es-CL')} — Documento informativo, no oficial`,
      50, 780, { align: 'center', width: 495 }
    );

    doc.end();
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// Helpers de transacción
// ---------------------------------------------------------------------------

/** Recalcula y actualiza los montos de una factura en base a sus movimientos actuales. */
async function recalcularMontos(transaction, idFactura, now) {
  const res = await new sql.Request(transaction)
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT COALESCE(SUM(cf.monto_aplicado), 0) AS monto_neto
      FROM [cfl].[CFL_factura_folio] ff
      INNER JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_folio = ff.id_folio
      WHERE ff.id_factura = @idFactura;
    `);

  const montoNeto  = toN(res.recordset[0]?.monto_neto);
  const montoIva   = Math.round(montoNeto * 0.19 * 100) / 100;
  const montoTotal = Math.round((montoNeto + montoIva) * 100) / 100;

  await new sql.Request(transaction)
    .input('idFactura',  sql.BigInt,      idFactura)
    .input('montoNeto',  sql.Decimal(18,2), montoNeto)
    .input('montoIva',   sql.Decimal(18,2), montoIva)
    .input('montoTotal', sql.Decimal(18,2), montoTotal)
    .input('updatedAt',  sql.DateTime2(0),  now)
    .query(`
      UPDATE [cfl].[CFL_cabecera_factura]
      SET monto_neto  = @montoNeto,
          monto_iva   = @montoIva,
          monto_total = @montoTotal,
          updated_at  = @updatedAt
      WHERE id_factura = @idFactura;
    `);
}

module.exports = { facturasRouter: router };
