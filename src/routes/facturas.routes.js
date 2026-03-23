'use strict';

const express = require('express');
const { getPool, sql } = require('../db');
const { resolveAuthzContext, hasAnyPermission } = require('../authz');
const { parsePositiveInt } = require('../utils/parse');
const { validate } = require('../middleware/validate.middleware');
const {
  previewBody,
  generarBody,
  agregarFoliosBody,
  actualizarFacturaBody,
  cambiarEstadoBody,
  idParam,
} = require('../schemas/facturas.schemas');
const {
  toN,
  calcMontos,
  buildInClause,
  buildFolioExclusionFilter,
  buildMovimientosQuery,
  updateFletesEstado,
  IVA_RATE,
} = require('../services/factura-queries');
const { generatePreFacturaPdf } = require('../services/factura-pdf');

const router = express.Router();

/** Verifica que el usuario tenga permiso de facturación o sea administrador. */
async function checkFacturacionPerm(req) {
  const authzContext = await resolveAuthzContext(req);
  const isAdmin = String(authzContext?.primaryRole || '').toLowerCase() === 'administrador';
  const hasPerm = hasAnyPermission(authzContext, ['facturacion', 'facturas.editar', 'facturas.generar']);
  return { authzContext, allowed: isAdmin || hasPerm };
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
      f.IdFolio,
      f.FolioNumero,
      f.IdCentroCosto,
      cc.nombre  AS centro_costo,
      cc.SapCodigo AS centro_costo_codigo,
      primary_tipo_flete_id = (
        SELECT TOP 1 cf2.IdTipoFlete
        FROM [cfl].[CabeceraFlete] cf2
        WHERE cf2.IdFolio = f.IdFolio
          AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
        GROUP BY cf2.IdTipoFlete
        ORDER BY COUNT(*) DESC
      ),
      primary_tipo_flete_nombre = (
        SELECT TOP 1 tf2.nombre
        FROM [cfl].[CabeceraFlete] cf2
        INNER JOIN [cfl].[TipoFlete] tf2 ON tf2.IdTipoFlete = cf2.IdTipoFlete
        WHERE cf2.IdFolio = f.IdFolio
          AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
        GROUP BY cf2.IdTipoFlete, tf2.nombre
        ORDER BY COUNT(*) DESC
      )
    FROM [cfl].[Folio] f
    LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = f.IdCentroCosto
    WHERE f.IdFolio IN (${inClause});
  `);

  // Movimientos elegibles de esos folios
  const movReq = pool.request();
  idsFolio.forEach((id, i) => movReq.input(`fol${i}`, sql.BigInt, id));

  const movData = await movReq.query(`
    SELECT
      cf.IdCabeceraFlete,
      cf.IdFolio,
      fol.FolioNumero,
      cf.SapNumeroEntrega,
      cf.NumeroEntrega,
      cf.GuiaRemision,
      cf.TipoMovimiento,
      cf.FechaSalida,
      cf.MontoAplicado,
      cf.IdTipoFlete,
      tf.nombre  AS tipo_flete_nombre,
      tf.SapCodigo AS tipo_flete_codigo,
      cf.IdCentroCosto,
      cc.nombre  AS centro_costo,
      cc.SapCodigo AS centro_costo_codigo,
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
    LEFT JOIN [cfl].[Folio] fol ON fol.IdFolio = cf.IdFolio
    LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
    LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
    LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
    LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = mv.IdEmpresaTransporte
    LEFT JOIN [cfl].[Chofer] ch ON ch.IdChofer = mv.IdChofer
    LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
    LEFT JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = cam.IdTipoCamion
    LEFT JOIN [cfl].[Tarifa] tar ON tar.IdTarifa = cf.IdTarifa
    LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tar.IdRuta
    LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
    LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
    WHERE cf.IdFolio IN (${inClause})
      AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
    ORDER BY cf.FechaSalida, cf.IdCabeceraFlete;
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
        fac.IdFactura,
        fac.IdEmpresa,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        emp.rut AS empresa_rut,
        fac.NumeroFactura,
        fac.FechaEmision,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.CriterioAgrupacion,
        fac.Observaciones,
        fac.FechaCreacion,
        fac.FechaActualizacion
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      WHERE fac.IdFactura = @idFactura;
    `);

  if (!facResult.recordset[0]) return null;
  const factura = facResult.recordset[0];

  // Folios asociados via bridge
  const foliosResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        ff.IdFacturaFolio,
        ff.IdFolio,
        fol.FolioNumero,
        fol.estado AS estado_folio,
        fol.IdCentroCosto,
        cc.nombre    AS centro_costo,
        cc.SapCodigo AS centro_costo_codigo,
        fol.IdCuentaMayor,
        cm.Codigo    AS cuenta_mayor_codigo,
        fol.PeriodoDesde,
        fol.PeriodoHasta,
        total_movimientos       = COUNT_BIG(cf.IdCabeceraFlete),
        monto_total_movimientos = COALESCE(SUM(cf.MontoAplicado), 0)
      FROM [cfl].[FacturaFolio] ff
      INNER JOIN [cfl].[Folio] fol ON fol.IdFolio = ff.IdFolio
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = fol.IdCentroCosto
      LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = fol.IdCuentaMayor
      LEFT JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = ff.IdFolio
      WHERE ff.IdFactura = @idFactura
      GROUP BY
        ff.IdFacturaFolio, ff.IdFolio, fol.FolioNumero, fol.estado,
        fol.IdCentroCosto, cc.nombre, cc.SapCodigo,
        fol.IdCuentaMayor, cm.Codigo,
        fol.PeriodoDesde, fol.PeriodoHasta
      ORDER BY ff.IdFacturaFolio;
    `);

  // Movimientos de todos los folios de esta factura
  const movResult = await pool.request()
    .input('idFactura', sql.BigInt, idFactura)
    .query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.IdFolio,
        fol.FolioNumero,
        cf.SapNumeroEntrega,
        cf.NumeroEntrega,
        cf.GuiaRemision,
        cf.TipoMovimiento,
        cf.estado,
        cf.FechaSalida,
        cf.MontoAplicado,
        tipo_flete_nombre  = tf.nombre,
        tipo_flete_codigo  = tf.SapCodigo,
        centro_costo       = cc.nombre,
        centro_costo_codigo = cc.SapCodigo,
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
        tipo_camion    = tc.Nombre,
        detalle_viaje  = dv.Descripcion,
        productor_nombre = prod.Nombre,
        productor_codigo = prod.CodigoProveedor
      FROM [cfl].[FacturaFolio] ff
      INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = ff.IdFolio
      INNER JOIN [cfl].[Folio] fol ON fol.IdFolio = cf.IdFolio
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
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
      WHERE ff.IdFactura = @idFactura
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
      INNER JOIN [cfl].[FacturaFolio] ff ON ff.IdFolio = cf.IdFolio
      LEFT JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
      WHERE ff.IdFactura = @idFactura
      ORDER BY df.IdCabeceraFlete, df.IdDetalleFlete;
    `);

  // Agrupar detalles por IdCabeceraFlete
  const detallesPorFlete = {};
  for (const d of detResult.recordset) {
    if (!detallesPorFlete[d.IdCabeceraFlete]) detallesPorFlete[d.IdCabeceraFlete] = [];
    detallesPorFlete[d.IdCabeceraFlete].push(d);
  }

  // Adjuntar detalles a cada movimiento
  const movimientos = movResult.recordset.map(m => ({
    ...m,
    detalles: detallesPorFlete[m.IdCabeceraFlete] || [],
  }));

  return { ...factura, folios: foliosResult.recordset, movimientos };
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
        et.IdEmpresa,
        et.rut,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(et.RazonSocial)), ''), CONCAT('Empresa #', et.IdEmpresa)),
        et.SapCodigo,
        folios_disponibles = (
          SELECT COUNT(DISTINCT f2.IdFolio)
          FROM [cfl].[Folio] f2
          INNER JOIN [cfl].[CabeceraFlete] cf2 ON cf2.IdFolio = f2.IdFolio
          INNER JOIN [cfl].[Movil] mv2 ON mv2.IdMovil = cf2.IdMovil
          WHERE mv2.IdEmpresaTransporte = et.IdEmpresa
            AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            AND ${buildFolioExclusionFilter('f2')}
        )
      FROM [cfl].[EmpresaTransporte] et
      INNER JOIN [cfl].[Movil] mv ON mv.IdEmpresaTransporte = et.IdEmpresa
      INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdMovil = mv.IdMovil
      INNER JOIN [cfl].[Folio] f ON f.IdFolio = cf.IdFolio
      WHERE UPPER(cf.estado) = 'ASIGNADO_FOLIO'
        AND et.activo = 1
        AND ${buildFolioExclusionFilter('f')}
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
router.get('/periodos-con-movimientos', async (req, res, next) => {
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
          COALESCE(SUM(cf.MontoAplicado), 0) AS monto_neto
        FROM [cfl].[CabeceraFlete] cf
        INNER JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
        INNER JOIN [cfl].[Folio] f ON f.IdFolio = cf.IdFolio
        WHERE mv.IdEmpresaTransporte = @idEmpresa
          AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
          AND ${buildFolioExclusionFilter('f')}
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
// GET /facturas/folios-elegibles?id_empresa=X
// Folios elegibles para una empresa (usados en Paso 2 del wizard)
// ---------------------------------------------------------------------------
router.get('/folios-elegibles', async (req, res, next) => {
  const idEmpresa = parsePositiveInt(req.query.id_empresa, 0);
  if (!idEmpresa) {
    res.status(400).json({ error: 'id_empresa requerido' });
    return;
  }

  const { desde, hasta } = req.query;

  try {
    const pool = await getPool();

    const request = pool.request().input('idEmpresa', sql.BigInt, idEmpresa);
    let periodoFilter = '';
    if (desde) {
      request.input('periodoDesde', sql.Date, new Date(desde));
      periodoFilter += ' AND f.PeriodoHasta >= @periodoDesde';
    }
    if (hasta) {
      request.input('periodoHasta', sql.Date, new Date(hasta));
      periodoFilter += ' AND f.PeriodoDesde <= @periodoHasta';
    }

    const result = await request.query(`
        WITH eligible_folios AS (
          SELECT DISTINCT
            f.IdFolio
          FROM [cfl].[Folio] f
          INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = f.IdFolio
          INNER JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
          WHERE mv.IdEmpresaTransporte = @idEmpresa
            AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
            AND ${buildFolioExclusionFilter('f')}
            ${periodoFilter}
        )
        SELECT
          f.IdFolio,
          f.FolioNumero,
          f.estado AS estado_folio,
          f.IdCentroCosto,
          cc.nombre  AS centro_costo,
          cc.SapCodigo AS centro_costo_codigo,
          f.PeriodoDesde,
          f.PeriodoHasta,
          total_movimientos       = COUNT_BIG(cf.IdCabeceraFlete),
          monto_neto_estimado     = COALESCE(SUM(cf.MontoAplicado), 0),
          primary_tipo_flete_id = (
            SELECT TOP 1 cf2.IdTipoFlete
            FROM [cfl].[CabeceraFlete] cf2
            WHERE cf2.IdFolio = f.IdFolio AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            GROUP BY cf2.IdTipoFlete ORDER BY COUNT(*) DESC
          ),
          primary_tipo_flete_nombre = (
            SELECT TOP 1 tf2.nombre
            FROM [cfl].[CabeceraFlete] cf2
            INNER JOIN [cfl].[TipoFlete] tf2 ON tf2.IdTipoFlete = cf2.IdTipoFlete
            WHERE cf2.IdFolio = f.IdFolio AND UPPER(cf2.estado) = 'ASIGNADO_FOLIO'
            GROUP BY cf2.IdTipoFlete, tf2.nombre ORDER BY COUNT(*) DESC
          )
        FROM eligible_folios ef
        INNER JOIN [cfl].[Folio] f ON f.IdFolio = ef.IdFolio
        LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = f.IdCentroCosto
        LEFT JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = f.IdFolio AND UPPER(cf.estado) = 'ASIGNADO_FOLIO'
        GROUP BY
          f.IdFolio, f.FolioNumero, f.estado, f.IdCentroCosto,
          cc.nombre, cc.SapCodigo, f.PeriodoDesde, f.PeriodoHasta
        ORDER BY f.FolioNumero;
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
router.post('/preview', validate({ body: previewBody }), async (req, res, next) => {
  const { id_empresa, ids_folio, criterio } = req.body;

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
router.post('/generar', validate({ body: generarBody }), async (req, res, next) => {
  const { id_empresa, ids_folio, criterio } = req.body;

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
    res.status(403).json({ error: 'Sin permiso de pre facturación' });
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
          INSERT INTO [cfl].[CabeceraFactura]
            (IdEmpresa, NumeroFactura, FechaEmision, moneda, MontoNeto, MontoIva,
             MontoTotal, estado, CriterioAgrupacion, FechaCreacion, FechaActualizacion)
          OUTPUT INSERTED.IdFactura
          VALUES
            (@idEmpresa, @numTemp, @fechaEm, @moneda, @montoNeto, @montoIva,
             @montoTotal, @estado, @criterio, @createdAt, @updatedAt);
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

      // 3. Bridge folio → factura
      for (const folioId of grupo.ids_folio) {
        await new sql.Request(transaction)
          .input('idFactura', sql.BigInt, idFactura)
          .input('idFolio',   sql.BigInt, folioId)
          .input('createdAt', sql.DateTime2(0), now)
          .query(`
            INSERT INTO [cfl].[FacturaFolio] (IdFactura, IdFolio, FechaCreacion)
            VALUES (@idFactura, @idFolio, @createdAt);
          `);
      }

      // 4. Marcar fletes como FACTURADO
      await updateFletesEstado(transaction, grupo.ids_folio, 'ASIGNADO_FOLIO', 'FACTURADO', now);
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
        fac.FechaEmision,
        fac.moneda,
        fac.MontoNeto,
        fac.MontoIva,
        fac.MontoTotal,
        fac.estado,
        fac.CriterioAgrupacion,
        fac.Observaciones,
        fac.FechaCreacion,
        fac.FechaActualizacion,
        cantidad_folios = (
          SELECT COUNT_BIG(1)
          FROM [cfl].[FacturaFolio] ff
          WHERE ff.IdFactura = fac.IdFactura
        ),
        centro_costos = (
          SELECT STRING_AGG(src.label, ', ') WITHIN GROUP (ORDER BY src.label)
          FROM (
            SELECT DISTINCT CONCAT(cc2.SapCodigo, ' - ', cc2.Nombre) AS label
            FROM [cfl].[FacturaFolio] ff2
            INNER JOIN [cfl].[Folio] fol2 ON fol2.IdFolio = ff2.IdFolio
            INNER JOIN [cfl].[CentroCosto] cc2 ON cc2.IdCentroCosto = fol2.IdCentroCosto
            WHERE ff2.IdFactura = fac.IdFactura
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
      res.status(404).json({ error: 'Pre factura no encontrada' });
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
router.post('/:id/folios', validate({ params: idParam, body: agregarFoliosBody }), async (req, res, next) => {
  const idFactura = req.params.id;
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
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se pueden agregar folios a pre facturas en estado Borrador' });
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
            SELECT 1 FROM [cfl].[FacturaFolio]
            WHERE IdFactura = @idFactura AND IdFolio = @idFolio
          )
          INSERT INTO [cfl].[FacturaFolio] (IdFactura, IdFolio, FechaCreacion)
          VALUES (@idFactura, @idFolio, @createdAt);
        `);

      // Marcar fletes de este folio como FACTURADO
      await new sql.Request(transaction)
        .input('idFolio',    sql.BigInt, folioId)
        .input('updatedAt',  sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[CabeceraFlete]
          SET estado = 'FACTURADO', FechaActualizacion = @updatedAt
          WHERE IdFolio = @idFolio AND UPPER(estado) = 'ASIGNADO_FOLIO';
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
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se pueden quitar folios de pre facturas en estado Borrador' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, idFactura)
      .input('idFolio',   sql.BigInt, idFolio)
      .query(`
        DELETE FROM [cfl].[FacturaFolio]
        WHERE IdFactura = @idFactura AND IdFolio = @idFolio;
      `);

    // Devolver fletes al estado ASIGNADO_FOLIO
    await updateFletesEstado(transaction, [idFolio], 'FACTURADO', 'ASIGNADO_FOLIO', now);

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
router.put('/:id', validate({ params: idParam, body: actualizarFacturaBody }), async (req, res, next) => {
  const idFactura = req.params.id;
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
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se puede editar una pre factura en estado Borrador' });
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
        UPDATE [cfl].[CabeceraFactura]
        SET Observaciones      = @observaciones,
            CriterioAgrupacion = @criterioAgrupacion,
            FechaActualizacion          = @updatedAt
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
// DELETE /facturas/:id — eliminar factura en Borrador definitivamente
// Devuelve todos los fletes a ASIGNADO_FOLIO
// ---------------------------------------------------------------------------
router.delete('/:id', async (req, res, next) => {
  const idFactura = parsePositiveInt(req.params.id, 0);
  if (!idFactura) { res.status(400).json({ error: 'id_factura inválido' }); return; }

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }
    if (factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se pueden eliminar pre facturas en estado Borrador' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    // Devolver fletes a ASIGNADO_FOLIO
    const folioIds = factura.folios.map(f => f.id_folio);
    await updateFletesEstado(transaction, folioIds, 'FACTURADO', 'ASIGNADO_FOLIO', now);

    // Eliminar registros bridge
    await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, idFactura)
      .query(`DELETE FROM [cfl].[FacturaFolio] WHERE IdFactura = @idFactura;`);

    // Eliminar cabecera
    await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, idFactura)
      .query(`DELETE FROM [cfl].[CabeceraFactura] WHERE IdFactura = @idFactura;`);

    await transaction.commit();
    res.json({ message: 'Pre factura eliminada exitosamente' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// PATCH /facturas/:id/estado — transición de estado
// Body: { estado: 'anulada' | 'recibida' }
// ---------------------------------------------------------------------------
router.patch('/:id/estado', validate({ params: idParam, body: cambiarEstadoBody }), async (req, res, next) => {
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

  const { allowed } = await checkFacturacionPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();
    const factura = await fetchFactura(pool, idFactura);
    if (!factura) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }

    // Validar transición de estado
    if (nuevoEstado === 'recibida' && factura.estado !== 'borrador') {
      res.status(409).json({ error: 'Solo se puede marcar como recibida una pre factura en estado Borrador' });
      return;
    }
    if (nuevoEstado === 'anulada' && (factura.estado === 'anulada' || factura.estado === 'recibida')) {
      res.status(409).json({ error: 'La pre factura no se puede anular desde su estado actual' });
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
        UPDATE [cfl].[CabeceraFactura]
        SET estado = @estado, FechaActualizacion = @updatedAt
        WHERE IdFactura = @idFactura;
      `);

    // Si se anula: devolver fletes de todos los folios a ASIGNADO_FOLIO
    if (nuevoEstado === 'anulada') {
      const folioIds = factura.folios.map(f => f.id_folio);
      await updateFletesEstado(transaction, folioIds, 'FACTURADO', 'ASIGNADO_FOLIO', now);
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
router.get('/:id/export/excel', async (req, res, next) => {
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
    // Agregar centros de costo y cuentas mayor desde los folios (valores únicos)
    const ccCodigos = [...new Set(
      (factura.folios || []).map(f => f.centro_costo_codigo).filter(Boolean)
    )].join(', ') || '-';
    const cmCodigos = [...new Set(
      (factura.folios || []).map(f => f.cuenta_mayor_codigo).filter(Boolean)
    )].join(', ') || '-';

    shCab.columns = [
      { header: 'N° Pre Factura',     key: 'numero_factura', width: 20 },
      { header: 'Empresa',           key: 'empresa_nombre', width: 30 },
      { header: 'RUT',               key: 'empresa_rut',    width: 15 },
      { header: 'Fecha Emisión',     key: 'fecha_emision',  width: 18 },
      { header: 'Moneda',            key: 'moneda',         width: 8  },
      { header: 'Cód. Centro Costo', key: 'cc_codigo',      width: 20 },
      { header: 'Cód. Cuenta Mayor', key: 'cm_codigo',      width: 20 },
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
      cm_codigo:      cmCodigos,
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
      { header: 'Folio',            key: 'folio_numero',         width: 14 },
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
        empresa_rut:        m.empresa_rut || '-',
        chofer_nombre:      m.chofer_nombre || '-',
        chofer_rut:         m.chofer_rut || '-',
        camion_patente:     m.camion_patente || '-',
        tipo_camion:        m.tipo_camion || '-',
        fecha_salida:       m.fecha_salida,
        monto_aplicado:     Number(m.monto_aplicado) || 0,
      });
      total += Number(m.monto_aplicado) || 0;
    }

    // Fila de total
    const totRow = shDet.addRow({ centro_costo: 'TOTAL', monto_aplicado: total });
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
router.get('/:id/export/pdf', async (req, res, next) => {
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
      SELECT COALESCE(SUM(cf.MontoAplicado), 0) AS MontoNeto
      FROM [cfl].[FacturaFolio] ff
      INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = ff.IdFolio
      WHERE ff.IdFactura = @idFactura;
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
