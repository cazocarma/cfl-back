'use strict';

const express = require('express');
const { getPool, sql } = require('../db');
const { parsePositiveInt } = require('../utils/parse');
const { validate } = require('../middleware/validate.middleware');
const { requirePermission } = require('../middleware/authz.middleware');
const {
  previewBody,
  generarBody,
  agregarMovimientosBody,
  actualizarFacturaBody,
  cambiarEstadoBody,
  idParam,
} = require('../schemas/facturas.schemas');
const {
  toN,
  calcMontos,
  buildInClause,
  buildMovimientosQuery,
  updateFletesEstado,
  IVA_RATE,
} = require('../services/factura-queries');
const { generatePreFacturaPdf } = require('../services/factura-pdf');

const router = express.Router();

/**
 * Calcula el detalle de cada grupo de movimientos que se generará como factura.
 * Usado tanto en preview como en generar.
 *
 * @param {import('mssql').ConnectionPool} pool
 * @param {number} idEmpresa
 * @param {Array<{ ids_cabecera_flete: number[] }>} grupos
 * @returns {Promise<Array>}
 */
async function computePreview(pool, idEmpresa, grupos) {
  if (!grupos.length) return [];

  const result = [];

  for (const grupo of grupos) {
    const ids = grupo.ids_cabecera_flete.map(Number);
    if (!ids.length) continue;

    const req = pool.request().input('idEmpresa', sql.BigInt, idEmpresa);
    const inClause = buildInClause(req, ids, 'cf');

    // Verify movements belong to empresa, are COMPLETADO, and have no factura
    const movData = await req.query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.SapNumeroEntrega,
        cf.NumeroEntrega,
        cf.GuiaRemision,
        cf.TipoMovimiento,
        FechaSalida = CONVERT(VARCHAR(10), cf.FechaSalida, 23),
        cf.MontoAplicado,
        cf.MontoExtra,
        cf.IdTipoFlete,
        tf.nombre  AS tipo_flete_nombre,
        tf.SapCodigo AS tipo_flete_codigo,
        cf.IdCentroCosto,
        cc.nombre  AS centro_costo,
        cc.SapCodigo AS centro_costo_codigo,
        cf.IdTemporada,
        temporada_codigo = t.Codigo,
        temporada_nombre = t.Nombre,
        ruta = COALESCE(r.NombreRuta,
          CASE WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
            THEN CONCAT(COALESCE(no.nombre,'Origen'), ' -> ', COALESCE(nd.nombre,'Destino'))
            ELSE NULL END
        ),
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), NULL),
        empresa_rut    = emp.Rut,
        chofer_nombre  = ch.SapNombre,
        chofer_rut     = ch.SapIdFiscal,
        camion_patente = cam.SapPatente,
        camion_carro   = cam.SapCarro,
        tipo_camion    = tc.Nombre
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[Temporada] t ON t.IdTemporada = cf.IdTemporada
      LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = mv.IdEmpresaTransporte
      LEFT JOIN [cfl].[Chofer] ch ON ch.IdChofer = mv.IdChofer
      LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
      LEFT JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = cam.IdTipoCamion
      LEFT JOIN [cfl].[Tarifa] tar ON tar.IdTarifa = cf.IdTarifa
      LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tar.IdRuta
      LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
      LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
      WHERE cf.IdCabeceraFlete IN (${inClause})
        AND mv.IdEmpresaTransporte = @idEmpresa
        AND UPPER(cf.estado) = 'COMPLETADO'
        AND cf.IdFactura IS NULL
      ORDER BY cf.FechaSalida, cf.IdCabeceraFlete;
    `);

    const movimientos = movData.recordset;
    const { montoNeto, montoIva, montoTotal } = calcMontos(movimientos);

    result.push({
      ids_cabecera_flete: movimientos.map(m => m.id_cabecera_flete),
      movimientos,
      monto_neto: montoNeto,
      monto_iva: montoIva,
      monto_total: montoTotal,
      cantidad_movimientos: movimientos.length,
    });
  }

  return result;
}

/**
 * Lee el estado de una factura dentro de una transaccion con lock exclusivo.
 * Previene race conditions TOCTOU al garantizar que ningun otro proceso
 * pueda modificar la fila entre la lectura y la escritura posterior.
 *
 * @param {import('mssql').Transaction} transaction - Transaccion activa
 * @param {number} idFactura
 * @returns {Promise<{id_factura: number, estado: string} | null>}
 */
async function lockFacturaEstado(transaction, idFactura) {
  const result = await new sql.Request(transaction)
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT IdFactura, estado
      FROM [cfl].[CabeceraFactura] WITH (XLOCK, ROWLOCK)
      WHERE IdFactura = @idFactura;
    `);
  return result.recordset[0] || null;
}

/** Carga una factura completa (cabecera + movimientos). */
async function fetchFactura(pool, idFactura) {
  const facResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        fac.IdFactura,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        emp.rut AS empresa_rut,
        fac.NumeroFactura,
        FechaEmision = CONVERT(VARCHAR(10), fac.FechaEmision, 23),
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.CriterioAgrupacion,
        fac.Observaciones,
        fac.NumeroFacturaRecibida,
        fac.FechaCreacion,
        fac.FechaActualizacion
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      WHERE fac.IdFactura = @idFactura;
    `);

  if (!facResult.recordset[0]) return null;
  const factura = facResult.recordset[0];

  // Movimientos directamente via CabeceraFlete.IdFactura
  const movResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.SapNumeroEntrega,
        cf.NumeroEntrega,
        cf.GuiaRemision,
        cf.TipoMovimiento,
        cf.SentidoFlete,
        cf.estado,
        FechaSalida = CONVERT(VARCHAR(10), cf.FechaSalida, 23),
        cf.MontoAplicado,
        cf.MontoExtra,
        cf.IdTipoFlete,
        tipo_flete_nombre  = tf.nombre,
        tipo_flete_codigo  = tf.SapCodigo,
        cf.IdCentroCosto,
        centro_costo       = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
        cf.IdTemporada,
        temporada_codigo   = t.Codigo,
        temporada_nombre   = t.Nombre,
        ruta_nombre        = r.NombreRuta,
        origen_nombre      = no.nombre,
        destino_nombre     = nd.nombre,
        cuenta_mayor_codigo = cm.Codigo,
        cuenta_mayor_nombre = cm.Glosa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), NULL),
        empresa_rut    = emp.Rut,
        chofer_nombre  = ch.SapNombre,
        chofer_rut     = ch.SapIdFiscal,
        camion_patente = cam.SapPatente,
        camion_carro   = cam.SapCarro,
        tipo_camion    = tc.Nombre,
        detalle_viaje  = dv.Descripcion,
        productor_nombre = prod.Nombre,
        productor_codigo = prod.CodigoProveedor
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = cf.IdCuentaMayor
      LEFT JOIN [cfl].[Temporada] t ON t.IdTemporada = cf.IdTemporada
      LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = mv.IdEmpresaTransporte
      LEFT JOIN [cfl].[Chofer] ch ON ch.IdChofer = mv.IdChofer
      LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
      LEFT JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = cam.IdTipoCamion
      LEFT JOIN [cfl].[Tarifa] tar ON tar.IdTarifa = cf.IdTarifa
      LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tar.IdRuta
      LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
      LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
      LEFT JOIN [cfl].[DetalleViaje] dv ON dv.IdDetalleViaje = cf.IdDetalleViaje
      LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
      WHERE cf.IdFactura = @idFactura
      ORDER BY cf.FechaSalida, cf.IdCabeceraFlete;
    `);

  // Detalles de materiales/especies por cada movimiento
  const detResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        df.IdCabeceraFlete,
        df.Material,
        df.Descripcion,
        df.Cantidad,
        df.Unidad,
        especie_glosa = esp.Glosa
      FROM [cfl].[DetalleFlete] df
      INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdCabeceraFlete = df.IdCabeceraFlete
      LEFT JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
      WHERE cf.IdFactura = @idFactura
      ORDER BY df.IdCabeceraFlete, df.IdDetalleFlete;
    `);

  // Agrupar detalles por IdCabeceraFlete
  const detallesPorFlete = {};
  for (const d of detResult.recordset) {
    if (!detallesPorFlete[d.IdCabeceraFlete]) detallesPorFlete[d.IdCabeceraFlete] = [];
    detallesPorFlete[d.IdCabeceraFlete].push(d);
  }

  // Adjuntar detalles a cada movimiento, agrupando por material+especie
  const movimientos = movResult.recordset.map(m => ({
    ...m,
    detalles: consolidarDetalles(detallesPorFlete[m.IdCabeceraFlete] || []),
  }));

  return { ...factura, movimientos };
}

/**
 * Agrupa detalles por Material + Especie, sumando cantidades.
 * Si un grupo no tiene especie pero otros sí, hereda la especie más frecuente.
 */
function consolidarDetalles(detalles) {
  if (detalles.length <= 1) return detalles;

  const groups = new Map();
  for (const d of detalles) {
    const mat = (d.Material || d.Descripcion || '').trim();
    const esp = (d.especie_glosa || '').trim();
    const key = `${mat}|${esp}`;

    if (!groups.has(key)) {
      groups.set(key, {
        ...d,
        Cantidad: Number(d.Cantidad) || 0,
        _count: 1,
      });
    } else {
      const g = groups.get(key);
      g.Cantidad += Number(d.Cantidad) || 0;
      g._count++;
    }
  }

  // Si hay grupos sin especie, asignar la especie más frecuente del mismo flete
  const especieCounts = {};
  for (const g of groups.values()) {
    const esp = (g.especie_glosa || '').trim();
    if (esp) especieCounts[esp] = (especieCounts[esp] || 0) + g._count;
  }
  const especieMasFrecuente = Object.entries(especieCounts)
    .sort((a, b) => b[1] - a[1])[0]?.[0] || null;

  const result = [];
  for (const g of groups.values()) {
    if (!g.especie_glosa && especieMasFrecuente) {
      g.especie_glosa = especieMasFrecuente;
    }
    delete g._count;
    result.push(g);
  }
  return result;
}

// ===========================================================================
// RUTAS
// IMPORTANTE: rutas estáticas ANTES de /:id
// ===========================================================================

// ---------------------------------------------------------------------------
// GET /facturas/empresas-elegibles
// Empresas con al menos un movimiento en estado COMPLETADO sin factura asignada
// ---------------------------------------------------------------------------
router.get('/empresas-elegibles', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  try {
    const pool = await getPool();

    const result = await pool.request().query(`
      SELECT DISTINCT
        et.IdEmpresa,
        et.rut,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(et.RazonSocial)), ''), CONCAT('Empresa #', et.IdEmpresa)),
        et.SapCodigo,
        movimientos_disponibles = (
          SELECT COUNT_BIG(cf2.IdCabeceraFlete)
          FROM [cfl].[CabeceraFlete] cf2
          INNER JOIN [cfl].[Movil] mv2 ON mv2.IdMovil = cf2.IdMovil
          WHERE mv2.IdEmpresaTransporte = et.IdEmpresa
            AND UPPER(cf2.estado) = 'COMPLETADO'
            AND cf2.IdFactura IS NULL
        )
      FROM [cfl].[EmpresaTransporte] et
      INNER JOIN [cfl].[Movil] mv ON mv.IdEmpresaTransporte = et.IdEmpresa
      INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdMovil = mv.IdMovil
      WHERE UPPER(cf.estado) = 'COMPLETADO'
        AND cf.IdFactura IS NULL
        AND et.activo = 1
      ORDER BY empresa_nombre;
    `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/periodos-con-movimientos?id_empresa=X
// Meses con movimientos elegibles para una empresa transportista
// ---------------------------------------------------------------------------
router.get('/periodos-con-movimientos', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
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
        SELECT
          YEAR(cf.FechaSalida)  AS anio,
          MONTH(cf.FechaSalida) AS mes,
          COUNT_BIG(cf.IdCabeceraFlete) AS total_movimientos,
          COALESCE(SUM(cf.MontoAplicado + COALESCE(cf.MontoExtra, 0)), 0) AS monto_neto
        FROM [cfl].[CabeceraFlete] cf
        INNER JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
        WHERE mv.IdEmpresaTransporte = @idEmpresa
          AND UPPER(cf.estado) = 'COMPLETADO'
          AND cf.IdFactura IS NULL
          AND cf.FechaSalida IS NOT NULL
        GROUP BY YEAR(cf.FechaSalida), MONTH(cf.FechaSalida)
        ORDER BY anio DESC, mes DESC;
      `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/movimientos-elegibles?id_empresa=X&desde=Y&hasta=Z
// Movimientos COMPLETADO sin factura para una empresa, en rango de fechas
// ---------------------------------------------------------------------------
router.get('/movimientos-elegibles', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  const idEmpresa = parsePositiveInt(req.query.id_empresa, 0);
  if (!idEmpresa) {
    res.status(400).json({ error: 'id_empresa requerido' });
    return;
  }

  try {
    const pool = await getPool();
    const request = pool.request().input('idEmpresa', sql.BigInt, idEmpresa);

    let periodoFilter = '';
    if (req.query.desde) {
      request.input('desde', sql.Date, new Date(req.query.desde));
      periodoFilter += ' AND cf.FechaSalida >= @desde';
    }
    if (req.query.hasta) {
      request.input('hasta', sql.Date, new Date(req.query.hasta));
      periodoFilter += ' AND cf.FechaSalida <= @hasta';
    }

    const result = await request.query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.GuiaRemision,
        cf.SapNumeroEntrega,
        cf.IdTipoFlete,
        tf.nombre  AS tipo_flete_nombre,
        cf.IdCentroCosto,
        cc.nombre  AS centro_costo,
        cc.SapCodigo AS centro_costo_codigo,
        cf.IdTemporada,
        temporada_codigo = t.Codigo,
        temporada_nombre = t.Nombre,
        FechaSalida = CONVERT(VARCHAR(10), cf.FechaSalida, 23),
        cf.MontoAplicado,
        cf.MontoExtra
      FROM [cfl].[CabeceraFlete] cf
      INNER JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[Temporada] t ON t.IdTemporada = cf.IdTemporada
      WHERE mv.IdEmpresaTransporte = @idEmpresa
        AND UPPER(cf.estado) = 'COMPLETADO'
        AND cf.IdFactura IS NULL
        ${periodoFilter}
      ORDER BY cf.FechaSalida, cf.IdCabeceraFlete;
    `);

    const movimientos = result.recordset;

    // Auto-group by tipo_flete for UI convenience
    const grupoMap = new Map();
    for (const m of movimientos) {
      const key = m.id_tipo_flete || 0;
      if (!grupoMap.has(key)) {
        grupoMap.set(key, {
          tipo_flete_id: m.id_tipo_flete,
          tipo_flete_nombre: m.tipo_flete_nombre || 'Sin Tipo de Flete',
          ids_cabecera_flete: [],
        });
      }
      grupoMap.get(key).ids_cabecera_flete.push(m.id_cabecera_flete);
    }

    res.json({
      data: {
        movimientos,
        grupos_sugeridos: Array.from(grupoMap.values()),
      },
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/preview
// Calcula cuántas facturas se generarán antes de confirmar
// ---------------------------------------------------------------------------
router.post('/preview', requirePermission("facturas.editar"), validate({ body: previewBody }), async (req, res, next) => {
  const { id_empresa, grupos } = req.body;

  if (!id_empresa || !Array.isArray(grupos) || grupos.length === 0) {
    res.status(400).json({ error: 'Faltan id_empresa o grupos' });
    return;
  }

  try {
    const pool = await getPool();
    const gruposResult = await computePreview(pool, Number(id_empresa), grupos);
    res.json({
      data: {
        cantidad_facturas: gruposResult.length,
        grupos: gruposResult,
      },
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/generar
// Confirma generación: persiste facturas y marca fletes como PREFACTURADO
// ---------------------------------------------------------------------------
router.post('/generar', requirePermission("facturas.editar"), validate({ body: generarBody }), async (req, res, next) => {
  const { id_empresa, grupos } = req.body;

  if (!id_empresa || !Array.isArray(grupos) || grupos.length === 0) {
    res.status(400).json({ error: 'Faltan id_empresa o grupos' });
    return;
  }

  let transaction;
  try {
    const pool = await getPool();
    // Calcular grupos fuera de transacción (read-only)
    const gruposPreview = await computePreview(pool, Number(id_empresa), grupos);

    if (!gruposPreview.length) {
      res.status(422).json({ error: 'No hay movimientos elegibles para los fletes seleccionados' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();

    // Obtener código de temporada activa para el sufijo del número de pre factura
    const tempResult = await new sql.Request(transaction).query(`
      SELECT TOP 1 Codigo
      FROM [cfl].[Temporada]
      WHERE Activa = 1
      ORDER BY FechaInicio DESC, IdTemporada DESC;
    `);
    const temporadaCodigo = tempResult.recordset[0]?.codigo || '';

    const now = new Date();
    const createdIds = [];

    for (const grupo of gruposPreview) {
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
        .input('createdAt',  sql.DateTime2(0),  now)
        .input('updatedAt',  sql.DateTime2(0),  now)
        .query(`
          INSERT INTO [cfl].[CabeceraFactura]
            (IdEmpresa, NumeroFactura, FechaEmision, moneda, MontoNeto, MontoIva,
             MontoTotal, estado, FechaCreacion, FechaActualizacion)
          OUTPUT INSERTED.IdFactura
          VALUES
            (@idEmpresa, @numTemp, @fechaEm, @moneda, @montoNeto, @montoIva,
             @montoTotal, @estado, @createdAt, @updatedAt);
        `);

      const idFactura = Number(insertFac.recordset[0].id_factura);

      // 2. Actualizar número definitivo: INT-XXXXXX-TTTT
      const sufijoTemp = temporadaCodigo ? `-${temporadaCodigo}` : '';
      const numeroFactura = `INT-${String(idFactura).padStart(6, '0')}${sufijoTemp}`;
      await new sql.Request(transaction)
        .input('idFactura',      sql.BigInt,    idFactura)
        .input('numeroFactura',  sql.VarChar(40), numeroFactura)
        .input('updatedAt',      sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CabeceraFactura]
          SET NumeroFactura = @numeroFactura, FechaActualizacion = @updatedAt
          WHERE IdFactura = @idFactura;
        `);

      createdIds.push(idFactura);

      // 3. Set IdFactura and Estado='PREFACTURADO' on all fletes in this group
      const fleteIds = grupo.ids_cabecera_flete.map(Number);
      if (fleteIds.length) {
        const setReq = new sql.Request(transaction);
        setReq.input('idFactura', sql.BigInt, idFactura);
        setReq.input('updatedAt', sql.DateTime2(0), now);
        const inFragment = buildInClause(setReq, fleteIds, 'gf');

        const updateResult = await setReq.query(`
          UPDATE [cfl].[CabeceraFlete]
          SET IdFactura = @idFactura,
              estado = 'PREFACTURADO',
              FechaActualizacion = @updatedAt
          WHERE IdCabeceraFlete IN (${inFragment})
            AND UPPER(estado) = 'COMPLETADO'
            AND IdFactura IS NULL;
        `);

        if (updateResult.rowsAffected[0] !== fleteIds.length) {
          await transaction.rollback();
          res.status(409).json({
            error: 'Algunos fletes ya no estan disponibles (fueron tomados por otra factura o cambiaron de estado)',
          });
          return;
        }
      }
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
router.get('/', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  try {
    const pool = await getPool();
    const { empresa, estado, desde, hasta } = req.query;

    let where = 'WHERE 1=1';
    const request = pool.request();

    if (empresa) {
      request.input('empresa', sql.BigInt, Number(empresa));
      where += ' AND fac.IdEmpresa = @empresa';
    }
    if (estado) {
      request.input('estado', sql.VarChar(20), String(estado).toLowerCase());
      where += ' AND LOWER(fac.estado) = @estado';
    }
    if (desde) {
      request.input('desde', sql.Date, String(desde));
      where += ' AND CAST(fac.FechaEmision AS DATE) >= @desde';
    }
    if (hasta) {
      request.input('hasta', sql.Date, String(hasta));
      where += ' AND CAST(fac.FechaEmision AS DATE) <= @hasta';
    }

    const result = await request.query(`
      SELECT
        fac.IdFactura,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fac.NumeroFactura,
        FechaEmision = CONVERT(VARCHAR(10), fac.FechaEmision, 23),
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.CriterioAgrupacion,
        fac.Observaciones,
        fac.NumeroFacturaRecibida,
        fac.FechaCreacion,
        fac.FechaActualizacion,
        cantidad_movimientos = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[CabeceraFlete] cf
          WHERE cf.IdFactura = fac.IdFactura
        ),
        centro_costos = (
          SELECT STRING_AGG(src.label, ', ') WITHIN GROUP (ORDER BY src.label)
          FROM (
            SELECT DISTINCT CONCAT(cc2.SapCodigo, ' - ', cc2.Nombre) AS label
            FROM [cfl].[CabeceraFlete] cf2
            INNER JOIN [cfl].[CentroCosto] cc2 ON cc2.IdCentroCosto = cf2.IdCentroCosto
            WHERE cf2.IdFactura = fac.IdFactura
          ) src
        )
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      ${where}
      ORDER BY fac.FechaEmision DESC, fac.IdFactura DESC;
    `);

    res.json({ data: result.recordset });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id
// ---------------------------------------------------------------------------
router.get('/:id', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) {
      res.status(404).json({ error: 'Pre factura no encontrada' });
      return;
    }
    res.json({ data: factura });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /facturas/:id/movimientos — agregar movimientos a factura en Borrador
// ---------------------------------------------------------------------------
router.post('/:id/movimientos', requirePermission("facturas.editar"), validate({ params: idParam, body: agregarMovimientosBody }), async (req, res, next) => {
  const idFactura = req.params.id;
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const { ids_cabecera_flete } = req.body || {};
  if (!Array.isArray(ids_cabecera_flete) || ids_cabecera_flete.length === 0) {
    res.status(400).json({ error: 'ids_cabecera_flete requerido' });
    return;
  }

  let transaction;
  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const factura = await lockFacturaEstado(transaction, idFactura);
    if (!factura) {
      await transaction.rollback();
      res.status(404).json({ error: 'Pre factura no encontrada' });
      return;
    }
    if (factura.estado !== 'borrador') {
      await transaction.rollback();
      res.status(409).json({ error: 'Solo se pueden agregar movimientos a pre facturas en estado Borrador' });
      return;
    }

    const now = new Date();
    const fleteIds = ids_cabecera_flete.map(Number);
    const setReq = new sql.Request(transaction);
    setReq.input('idFactura', sql.BigInt, idFactura);
    setReq.input('updatedAt', sql.DateTime2(0), now);
    const inFragment = buildInClause(setReq, fleteIds, 'af');

    await setReq.query(`
      UPDATE [cfl].[CabeceraFlete]
      SET IdFactura = @idFactura,
          estado = 'PREFACTURADO',
          FechaActualizacion = @updatedAt
      WHERE IdCabeceraFlete IN (${inFragment})
        AND UPPER(estado) = 'COMPLETADO'
        AND IdFactura IS NULL;
    `);

    await recalcularMontos(transaction, idFactura, now);

    await transaction.commit();
    res.json({ message: 'Movimientos agregados correctamente' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// DELETE /facturas/:id/movimientos/:id_flete — quitar movimiento de factura Borrador
// ---------------------------------------------------------------------------
router.delete('/:id/movimientos/:id_flete', requirePermission("facturas.editar"), async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  const idFlete   = parsePositiveInt(req.params.id_flete, 0);
  if (!idFactura || !idFlete) {
    res.status(400).json({ error: 'Parámetros inválidos' });
    return;
  }

  let transaction;
  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const factura = await lockFacturaEstado(transaction, idFactura);
    if (!factura) {
      await transaction.rollback();
      res.status(404).json({ error: 'Pre factura no encontrada' });
      return;
    }
    if (factura.estado !== 'borrador') {
      await transaction.rollback();
      res.status(409).json({ error: 'Solo se pueden quitar movimientos de pre facturas en estado Borrador' });
      return;
    }

    const now = new Date();
    await new sql.Request(transaction)
      .input('idFlete',   sql.BigInt, idFlete)
      .input('idFactura', sql.BigInt, idFactura)
      .input('updatedAt', sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET IdFactura = NULL,
            estado = 'COMPLETADO',
            FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idFlete
          AND IdFactura = @idFactura;
      `);

    await recalcularMontos(transaction, idFactura, now);

    await transaction.commit();
    res.json({ message: 'Movimiento quitado correctamente' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PUT /facturas/:id — editar cabecera (solo Borrador)
// ---------------------------------------------------------------------------
router.put('/:id', requirePermission("facturas.editar", "facturas.conciliar"), validate({ params: idParam, body: actualizarFacturaBody }), async (req, res, next) => {
  const idFactura = req.params.id;
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const { observaciones, criterio_agrupacion } = req.body || {};

  let transaction;
  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const factura = await lockFacturaEstado(transaction, idFactura);
    if (!factura) {
      await transaction.rollback();
      res.status(404).json({ error: 'Pre factura no encontrada' });
      return;
    }
    if (factura.estado !== 'borrador') {
      await transaction.rollback();
      res.status(409).json({ error: 'Solo se puede editar una pre factura en estado Borrador' });
      return;
    }

    const now = new Date();
    await new sql.Request(transaction)
      .input('idFactura',          sql.BigInt,    idFactura)
      .input('observaciones',      sql.VarChar(500), observaciones ?? null)
      .input('criterioAgrupacion', sql.VarChar(30),  criterio_agrupacion ?? null)
      .input('updatedAt',          sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CabeceraFactura]
        SET Observaciones      = @observaciones,
            CriterioAgrupacion = COALESCE(@criterioAgrupacion, CriterioAgrupacion),
            FechaActualizacion = @updatedAt
        WHERE IdFactura = @idFactura;
      `);

    await transaction.commit();
    res.json({ message: 'Pre factura actualizada' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PATCH /facturas/:id/estado — transición de estado
// Body: { estado: 'anulada' | 'recibida' }
// ---------------------------------------------------------------------------
router.patch('/:id/estado', requirePermission("facturas.editar"), validate({ params: idParam, body: cambiarEstadoBody }), async (req, res, next) => {
  const idFactura = req.params.id;
  if (!idFactura) {
    res.status(400).json({ error: 'id_factura inválido' });
    return;
  }

  const nuevoEstado = String(req.body?.estado || '').toLowerCase();
  if (!['anulada', 'recibida'].includes(nuevoEstado)) {
    res.status(400).json({ error: 'estado debe ser "anulada" o "recibida"' });
    return;
  }

  const numeroFacturaRecibida = req.body?.numero_factura_recibida || null;

  // Si se marca como recibida, el número de factura recibida es obligatorio
  if (nuevoEstado === 'recibida' && !numeroFacturaRecibida) {
    res.status(400).json({ error: 'numero_factura_recibida es obligatorio al marcar como recibida' });
    return;
  }

  let transaction;
  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const factura = await lockFacturaEstado(transaction, idFactura);
    if (!factura) {
      await transaction.rollback();
      res.status(404).json({ error: 'Pre factura no encontrada' });
      return;
    }

    if (nuevoEstado === 'recibida' && factura.estado !== 'borrador') {
      await transaction.rollback();
      res.status(409).json({ error: 'Solo se puede marcar como recibida una pre factura en estado Borrador' });
      return;
    }
    if (nuevoEstado === 'anulada' && (factura.estado === 'anulada' || factura.estado === 'recibida')) {
      await transaction.rollback();
      res.status(409).json({ error: 'La pre factura no se puede anular desde su estado actual' });
      return;
    }

    const now = new Date();

    const updateReq = new sql.Request(transaction)
      .input('idFactura',  sql.BigInt,    idFactura)
      .input('estado',     sql.VarChar(20), nuevoEstado)
      .input('updatedAt',  sql.DateTime2(0), now);

    if (nuevoEstado === 'recibida') {
      updateReq.input('numFacRecibida', sql.NVarChar(60), numeroFacturaRecibida);
      await updateReq.query(`
        UPDATE [cfl].[CabeceraFactura]
        SET estado = @estado,
            NumeroFacturaRecibida = @numFacRecibida,
            FechaActualizacion = @updatedAt
        WHERE IdFactura = @idFactura;
      `);
    } else {
      await updateReq.query(`
        UPDATE [cfl].[CabeceraFactura]
        SET estado = @estado, FechaActualizacion = @updatedAt
        WHERE IdFactura = @idFactura;
      `);
    }

    // Leer fletes vinculados dentro de la transaccion
    const movResult = await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, idFactura)
      .query(`
        SELECT IdCabeceraFlete
        FROM [cfl].[CabeceraFlete]
        WHERE IdFactura = @idFactura;
      `);
    const fleteIds = movResult.recordset.map(m => m.id_cabecera_flete ?? m.IdCabeceraFlete);

    if (nuevoEstado === 'anulada' && fleteIds.length) {
      const revertReq = new sql.Request(transaction);
      revertReq.input('updatedAt', sql.DateTime2(0), now);
      const inFragment = buildInClause(revertReq, fleteIds, 'an');

      await revertReq.query(`
        UPDATE [cfl].[CabeceraFlete]
        SET IdFactura = NULL,
            estado = 'COMPLETADO',
            FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete IN (${inFragment});
      `);
    }

    if (nuevoEstado === 'recibida' && fleteIds.length) {
      await updateFletesEstado(transaction, fleteIds, 'PREFACTURADO', 'FACTURADO', now);
    }

    await transaction.commit();
    res.json({ message: `Pre factura ${nuevoEstado} exitosamente` });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id/export/excel
// ---------------------------------------------------------------------------
router.get('/:id/export/excel', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) { res.status(400).json({ error: 'id_factura inválido' }); return; }

  try {
    const ExcelJS = require('exceljs');
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }

    const wb = new ExcelJS.Workbook();
    wb.creator = 'CFL System';
    wb.created = new Date();

    // Hoja 1 — Cabecera de pre factura
    const shCab = wb.addWorksheet('Cabecera');
    // Centros de costo desde los movimientos (valores únicos)
    const ccCodigos = [...new Set(
      (factura.movimientos || []).map(m => m.centro_costo_codigo).filter(Boolean)
    )].join(', ') || '-';

    shCab.columns = [
      { header: 'N° Pre Factura',     key: 'numero_factura', width: 20 },
      { header: 'Empresa',           key: 'empresa_nombre', width: 30 },
      { header: 'RUT',               key: 'empresa_rut',    width: 15 },
      { header: 'Fecha Emisión',     key: 'fecha_emision',  width: 18 },
      { header: 'Moneda',            key: 'moneda',         width: 8  },
      { header: 'Cód. Centro Costo', key: 'cc_codigo',      width: 20 },
      { header: 'Monto Neto',        key: 'monto_neto',     width: 15 },
      { header: 'IVA',               key: 'monto_iva',      width: 15 },
      { header: 'Monto Total',       key: 'monto_total',    width: 15 },
      { header: 'Estado',            key: 'estado',         width: 12 },
      { header: 'Observaciones',     key: 'observaciones',  width: 40 },
    ];
    shCab.getRow(1).font = { bold: true };
    shCab.addRow({
      numero_factura: factura.numero_factura,
      empresa_nombre: factura.empresa_nombre,
      empresa_rut:    factura.empresa_rut,
      fecha_emision:  factura.fecha_emision,
      moneda:         factura.moneda,
      cc_codigo:      ccCodigos,
      monto_neto:     Number(factura.monto_neto),
      monto_iva:      Number(factura.monto_iva),
      monto_total:    Number(factura.monto_total),
      estado:         factura.estado,
      observaciones:  factura.observaciones || '',
    });

    // Hoja 2 — Detalle de movimientos
    const shDet = wb.addWorksheet('Movimientos');
    shDet.columns = [
      { header: 'N° Guía',          key: 'guia_remision',        width: 15 },
      { header: 'Entrega SAP',      key: 'sap_numero_entrega',   width: 15 },
      { header: 'Tipo Flete',       key: 'tipo_flete_nombre',    width: 22 },
      { header: 'Centro de Costo',  key: 'centro_costo',         width: 22 },
      { header: 'Ruta',             key: 'ruta',                 width: 30 },
      { header: 'Empresa Transp.',  key: 'empresa_nombre',       width: 25 },
      { header: 'RUT Empresa',      key: 'empresa_rut',          width: 14 },
      { header: 'Chofer',           key: 'chofer_nombre',        width: 25 },
      { header: 'RUT Chofer',       key: 'chofer_rut',           width: 14 },
      { header: 'Patente',          key: 'camion_patente',       width: 12 },
      { header: 'Tipo Camión',      key: 'tipo_camion',          width: 18 },
      { header: 'Fecha Salida',     key: 'fecha_salida',         width: 14 },
      { header: 'Monto Aplicado',   key: 'monto_aplicado',       width: 15 },
      { header: 'Monto Extra',      key: 'monto_extra',          width: 15 },
      { header: 'Monto Total',      key: 'monto_total_linea',    width: 15 },
    ];
    shDet.getRow(1).font = { bold: true };

    let total = 0;
    for (const m of factura.movimientos) {
      const aplicado = Number(m.monto_aplicado) || 0;
      const extra = Number(m.monto_extra) || 0;
      const lineaTotal = aplicado + extra;
      shDet.addRow({
        guia_remision:      m.guia_remision || m.sap_numero_entrega || '-',
        sap_numero_entrega: m.sap_numero_entrega || '-',
        tipo_flete_nombre:  m.tipo_flete_nombre || '-',
        centro_costo:       m.centro_costo || '-',
        ruta:               m.ruta || '-',
        empresa_nombre:     m.empresa_nombre || '-',
        empresa_rut:        m.empresa_rut || '-',
        chofer_nombre:      m.chofer_nombre || '-',
        chofer_rut:         m.chofer_rut || '-',
        camion_patente:     m.camion_patente || '-',
        tipo_camion:        m.tipo_camion || '-',
        fecha_salida:       m.fecha_salida,
        monto_aplicado:     aplicado,
        monto_extra:        extra,
        monto_total_linea:  lineaTotal,
      });
      total += lineaTotal;
    }

    // Fila de total
    const totRow = shDet.addRow({ centro_costo: 'TOTAL', monto_total_linea: total });
    totRow.font = { bold: true };

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="pre-factura-${factura.numero_factura}.xlsx"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /facturas/:id/export/pdf
// ---------------------------------------------------------------------------
router.get('/:id/export/pdf', requirePermission("facturas.ver", "facturas.editar", "facturas.conciliar"), async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) { res.status(400).json({ error: 'id_factura inválido' }); return; }

  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="pre-factura-${factura.numero_factura}.pdf"`);
    generatePreFacturaPdf(factura, res);
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
      SELECT COALESCE(SUM(cf.MontoAplicado + COALESCE(cf.MontoExtra, 0)), 0) AS MontoNeto
      FROM [cfl].[CabeceraFlete] cf
      WHERE cf.IdFactura = @idFactura;
    `);

  const montoNeto  = toN(res.recordset[0]?.monto_neto);
  const montoIva   = Math.round(montoNeto * IVA_RATE * 100) / 100;
  const montoTotal = Math.round((montoNeto + montoIva) * 100) / 100;

  await new sql.Request(transaction)
    .input('idFactura',  sql.BigInt,      idFactura)
    .input('montoNeto',  sql.Decimal(18,2), montoNeto)
    .input('montoIva',   sql.Decimal(18,2), montoIva)
    .input('montoTotal', sql.Decimal(18,2), montoTotal)
    .input('updatedAt',  sql.DateTime2(0),  now)
    .query(`
      UPDATE [cfl].[CabeceraFactura]
      SET MontoNeto  = @montoNeto,
          MontoIva   = @montoIva,
          MontoTotal = @montoTotal,
          FechaActualizacion  = @updatedAt
      WHERE IdFactura = @idFactura;
    `);
}

module.exports = { facturasRouter: router };
