'use strict';

const express = require('express');
const { getPool, sql } = require('../db');
const { resolveAuthzContext, hasAnyPermission } = require('../authz');
const { parsePositiveInt } = require('../utils/parse');
const { validate } = require('../middleware/validate.middleware');
const { generarBody, cambiarEstadoBody, idParam, agregarFacturasBody } = require('../schemas/planillas-sap.schemas');
const { generateSapExcel } = require('../services/planilla-sap-export');

const router = express.Router();

const MESES = ['', 'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
  'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function checkPlanillasPerm(req) {
  const authzContext = await resolveAuthzContext(req);
  const isAdmin = String(authzContext?.primaryRole || '').toLowerCase() === 'administrador';
  const hasPerm = hasAnyPermission(authzContext, ['planillas.generar', 'facturacion']);
  return { authzContext, allowed: isAdmin || hasPerm };
}

async function fetchPlanilla(pool, idPlanilla) {
  const hdr = await pool.request()
    .input('id', sql.BigInt, idPlanilla)
    .query(`
      SELECT p.*
      FROM [cfl].[PlanillaSap] p
      WHERE p.IdPlanillaSap = @id;
    `);
  if (!hdr.recordset[0]) return null;
  const planilla = hdr.recordset[0];

  // Facturas vinculadas via tabla puente
  const facs = await pool.request()
    .input('id', sql.BigInt, idPlanilla)
    .query(`
      SELECT psf.IdFactura, fac.NumeroFactura, fac.NumeroFacturaRecibida, fac.FechaEmision,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa))
      FROM [cfl].[PlanillaSapFactura] psf
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = psf.IdFactura
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      WHERE psf.IdPlanillaSap = @id
      ORDER BY fac.NumeroFactura;
    `);
  planilla.facturas = facs.recordset;

  // Período derivado de la primera factura vinculada
  if (facs.recordset.length > 0) {
    const firstDate = new Date(facs.recordset[0].fecha_emision);
    const mes = firstDate.getMonth() + 1;
    const anio = firstDate.getFullYear();
    planilla.periodo_label = `${MESES[mes] || mes} ${anio}`;
  } else {
    planilla.periodo_label = '';
  }

  // Resumen empresa
  if (facs.recordset.length === 1) {
    planilla.empresa_nombre = facs.recordset[0].empresa_nombre;
  } else if (facs.recordset.length > 1) {
    planilla.empresa_nombre = [...new Set(facs.recordset.map(f => f.empresa_nombre))].join(', ');
  } else {
    planilla.empresa_nombre = '';
  }

  const docs = await pool.request()
    .input('id', sql.BigInt, idPlanilla)
    .query(`
      SELECT * FROM [cfl].[PlanillaSapDocumento]
      WHERE IdPlanillaSap = @id ORDER BY NumeroDocumento;
    `);

  const lineas = await pool.request()
    .input('id', sql.BigInt, idPlanilla)
    .query(`
      SELECT l.* FROM [cfl].[PlanillaSapLinea] l
      INNER JOIN [cfl].[PlanillaSapDocumento] d ON d.IdPlanillaSapDocumento = l.IdPlanillaSapDocumento
      WHERE d.IdPlanillaSap = @id ORDER BY d.NumeroDocumento, l.NumeroLinea;
    `);

  // Group lineas by document
  const lineaMap = {};
  for (const l of lineas.recordset) {
    const key = l.id_planilla_sap_documento;
    if (!lineaMap[key]) lineaMap[key] = [];
    lineaMap[key].push(l);
  }

  planilla.documentos = docs.recordset.map(d => ({
    ...d,
    lineas: lineaMap[d.id_planilla_sap_documento] || [],
  }));

  return planilla;
}

/**
 * Regenera documentos y líneas SAP para una planilla existente en estado 'generada'.
 * Se usa al agregar/quitar pre facturas.
 */
async function regenerateDocuments(transaction, idPlanilla) {
  // Read planilla header for config
  const hdr = await new sql.Request(transaction)
    .input('id', sql.BigInt, idPlanilla)
    .query(`SELECT * FROM [cfl].[PlanillaSap] WHERE IdPlanillaSap = @id;`);
  if (!hdr.recordset[0]) throw new Error('Planilla no encontrada');
  const planilla = hdr.recordset[0];

  // Read existing OC assignments from current credit lines (keyed by CodigoProveedor)
  const existingOc = await new sql.Request(transaction)
    .input('id', sql.BigInt, idPlanilla)
    .query(`
      SELECT DISTINCT l.CodigoProveedor, l.OrdenCompra, l.PosicionOC
      FROM [cfl].[PlanillaSapLinea] l
      INNER JOIN [cfl].[PlanillaSapDocumento] d ON d.IdPlanillaSapDocumento = l.IdPlanillaSapDocumento
      WHERE d.IdPlanillaSap = @id AND l.ClaveContabilizacion = '29'
        AND l.OrdenCompra IS NOT NULL;
    `);
  const ocMap = {};
  for (const row of existingOc.recordset) {
    if (row.codigo_proveedor) {
      ocMap[row.codigo_proveedor] = {
        orden_compra: row.orden_compra,
        posicion_oc: row.posicion_oc || '10',
      };
    }
  }

  // Delete existing lines then documents
  await new sql.Request(transaction).input('id', sql.BigInt, idPlanilla).query(`
    DELETE l FROM [cfl].[PlanillaSapLinea] l
    INNER JOIN [cfl].[PlanillaSapDocumento] d ON d.IdPlanillaSapDocumento = l.IdPlanillaSapDocumento
    WHERE d.IdPlanillaSap = @id;
  `);
  await new sql.Request(transaction).input('id', sql.BigInt, idPlanilla).query(`
    DELETE FROM [cfl].[PlanillaSapDocumento] WHERE IdPlanillaSap = @id;
  `);

  // Get current linked facturas
  const facIds = await new sql.Request(transaction)
    .input('id', sql.BigInt, idPlanilla)
    .query(`SELECT IdFactura FROM [cfl].[PlanillaSapFactura] WHERE IdPlanillaSap = @id;`);
  const facturaIds = facIds.recordset.map(r => Number(r.id_factura));

  if (facturaIds.length === 0) {
    // No facturas left — zero out totals
    await new sql.Request(transaction)
      .input('id', sql.BigInt, idPlanilla)
      .input('updatedAt', sql.DateTime2(0), new Date())
      .query(`
        UPDATE [cfl].[PlanillaSap]
        SET TotalLineas = 0, TotalDocumentos = 0, MontoTotal = 0, FechaActualizacion = @updatedAt
        WHERE IdPlanillaSap = @id;
      `);
    return;
  }

  // Fetch all movements from linked facturas
  const movRequest = new sql.Request(transaction);
  const facParams = facturaIds.map((fid, i) => {
    movRequest.input(`fid${i}`, sql.BigInt, fid);
    return `@fid${i}`;
  });

  const movData = await movRequest.query(`
    SELECT
      cf.IdFactura,
      fac.NumeroFactura,
      fac.NumeroFacturaRecibida,
      cc.SapCodigo AS CentroCostoCodigo,
      cm.Codigo AS CuentaMayorCodigo,
      cf.IdProductor,
      prod.CodigoProveedor, prod.Nombre AS ProductorNombre,
      EspecieNombre = (
        SELECT TOP 1 esp.Glosa
        FROM [cfl].[DetalleFlete] df
        INNER JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
        WHERE df.IdCabeceraFlete = cf.IdCabeceraFlete
        ORDER BY COALESCE(df.Cantidad, 0) DESC, COALESCE(df.Peso, 0) DESC
      ),
      cf.MontoAplicado
    FROM [cfl].[CabeceraFlete] cf
    INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = cf.IdFactura
    LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
    LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = cf.IdCuentaMayor
    LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
    WHERE cf.IdFactura IN (${facParams.join(',')})
      AND UPPER(cf.Estado) IN ('FACTURADO', 'PREFACTURADO', 'COMPLETADO')
    ORDER BY fac.NumeroFactura, cc.SapCodigo, cm.Codigo, prod.Nombre;
  `);

  // Group into documents
  const glosa = planilla.glosa_cabecera;
  const indicadorImpuesto = planilla.indicador_impuesto || 'C0';
  const temporada = planilla.temporada || null;
  const codigoCA = planilla.codigo_cargo_abono || null;

  const docGroups = new Map();
  for (const row of movData.recordset) {
    const docKey = `${row.id_factura}|${row.centro_costo_codigo || 'SIN_CC'}|${row.cuenta_mayor_codigo || 'SIN_CM'}`;
    if (!docGroups.has(docKey)) {
      docGroups.set(docKey, {
        centro_costo_codigo: row.centro_costo_codigo,
        cuenta_mayor_codigo: row.cuenta_mayor_codigo,
        numero_factura_recibida: row.numero_factura_recibida || null,
        numero_pre_factura: row.numero_factura || null,
        lineas: new Map(),
      });
    }
    const doc = docGroups.get(docKey);
    const lineKey = `${row.id_productor}|${row.especie_nombre || ''}`;
    if (!doc.lineas.has(lineKey)) {
      doc.lineas.set(lineKey, {
        id_productor: row.id_productor,
        codigo_proveedor: row.codigo_proveedor,
        especie: row.especie_nombre || null,
        monto: 0,
      });
    }
    doc.lineas.get(lineKey).monto += Number(row.monto_aplicado || 0);
  }

  let totalLineas = 0;
  let montoTotal = 0;
  let docNum = 0;

  for (const [, group] of docGroups) {
    docNum++;
    const creditLines = Array.from(group.lineas.values());
    const montoDebito = creditLines.reduce((s, l) => s + l.monto, 0);
    montoTotal += montoDebito;

    const docReferencia = group.numero_factura_recibida || null;
    const docTextoCredito = group.numero_pre_factura
      ? `PREFACTURA ${group.numero_pre_factura}`
      : glosa;

    const insertDoc = await new sql.Request(transaction)
      .input('idPlanilla', sql.BigInt, idPlanilla)
      .input('numDoc', sql.Int, docNum)
      .input('ccCodigo', sql.NVarChar(20), group.centro_costo_codigo)
      .input('cmCodigo', sql.NVarChar(30), group.cuenta_mayor_codigo)
      .input('docRef', sql.NVarChar(60), docReferencia)
      .input('docPreFac', sql.NVarChar(40), group.numero_pre_factura)
      .input('montoDebito', sql.Decimal(18, 2), montoDebito)
      .input('totalLineas', sql.Int, 1 + creditLines.length)
      .query(`
        INSERT INTO [cfl].[PlanillaSapDocumento]
          (IdPlanillaSap, NumeroDocumento,
           CentroCostoCodigo, CuentaMayorCodigo,
           Referencia, NumeroPreFactura,
           MontoDebito, TotalLineas)
        OUTPUT INSERTED.IdPlanillaSapDocumento
        VALUES
          (@idPlanilla, @numDoc,
           @ccCodigo, @cmCodigo,
           @docRef, @docPreFac,
           @montoDebito, @totalLineas);
      `);
    const idDoc = Number(insertDoc.recordset[0].id_planilla_sap_documento);

    let lineNum = 0;

    // Debit line
    lineNum++;
    totalLineas++;
    await new sql.Request(transaction)
      .input('idDoc', sql.BigInt, idDoc)
      .input('numLinea', sql.Int, lineNum)
      .input('esDocNuevo', sql.Bit, 1)
      .input('clave', sql.NVarChar(10), '50')
      .input('cuentaMayor', sql.NVarChar(30), group.cuenta_mayor_codigo)
      .input('importe', sql.Decimal(18, 2), -montoDebito)
      .input('centroCosto', sql.NVarChar(20), group.centro_costo_codigo)
      .input('nroAsignacion', sql.NVarChar(100), docReferencia)
      .input('textoLinea', sql.NVarChar(100), glosa)
      .input('indicadorImp', sql.NVarChar(10), indicadorImpuesto)
      .input('temporada', sql.NVarChar(20), temporada)
      .input('tipoCA', sql.NVarChar(20), codigoCA)
      .query(`
        INSERT INTO [cfl].[PlanillaSapLinea]
          (IdPlanillaSapDocumento, NumeroLinea, EsDocNuevo, ClaveContabilizacion,
           CuentaMayor, Importe, CentroCosto, NroAsignacion, TextoLinea,
           IndicadorImpuesto, Temporada, TipoCargoAbono)
        VALUES
          (@idDoc, @numLinea, @esDocNuevo, @clave,
           @cuentaMayor, @importe, @centroCosto, @nroAsignacion, @textoLinea,
           @indicadorImp, @temporada, @tipoCA);
      `);

    // Credit lines
    for (const linea of creditLines) {
      lineNum++;
      totalLineas++;
      const oc = ocMap[linea.codigo_proveedor] || {};
      await new sql.Request(transaction)
        .input('idDoc', sql.BigInt, idDoc)
        .input('numLinea', sql.Int, lineNum)
        .input('esDocNuevo', sql.Bit, 0)
        .input('clave', sql.NVarChar(10), '29')
        .input('codProveedor', sql.NVarChar(20), linea.codigo_proveedor)
        .input('indicadorCME', sql.NVarChar(5), 'A')
        .input('importe', sql.Decimal(18, 2), linea.monto)
        .input('ordenCompra', sql.NVarChar(30), oc.orden_compra || null)
        .input('posicionOC', sql.NVarChar(10), oc.posicion_oc || '10')
        .input('nroAsignacion', sql.NVarChar(100), linea.especie)
        .input('textoLinea', sql.NVarChar(100), docTextoCredito)
        .input('indicadorImp', sql.NVarChar(10), indicadorImpuesto)
        .input('temporada', sql.NVarChar(20), temporada)
        .input('tipoCA', sql.NVarChar(20), codigoCA)
        .query(`
          INSERT INTO [cfl].[PlanillaSapLinea]
            (IdPlanillaSapDocumento, NumeroLinea, EsDocNuevo, ClaveContabilizacion,
             CodigoProveedor, IndicadorCME, Importe, OrdenCompra, PosicionOC,
             NroAsignacion, TextoLinea, IndicadorImpuesto, Temporada, TipoCargoAbono)
          VALUES
            (@idDoc, @numLinea, @esDocNuevo, @clave,
             @codProveedor, @indicadorCME, @importe, @ordenCompra, @posicionOC,
             @nroAsignacion, @textoLinea, @indicadorImp, @temporada, @tipoCA);
        `);
    }
  }

  // Update totals
  await new sql.Request(transaction)
    .input('id', sql.BigInt, idPlanilla)
    .input('totalLineas', sql.Int, totalLineas)
    .input('totalDocs', sql.Int, docNum)
    .input('montoTotal', sql.Decimal(18, 2), montoTotal)
    .input('updatedAt', sql.DateTime2(0), new Date())
    .query(`
      UPDATE [cfl].[PlanillaSap]
      SET TotalLineas = @totalLineas, TotalDocumentos = @totalDocs,
          MontoTotal = @montoTotal, FechaActualizacion = @updatedAt
      WHERE IdPlanillaSap = @id;
    `);
}

// ---------------------------------------------------------------------------
// GET /planillas-sap — list
// ---------------------------------------------------------------------------
router.get('/', async (req, res, next) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT p.IdPlanillaSap, p.FechaDocumento, p.FechaContabilizacion,
             p.GlosaCabecera, p.TotalLineas, p.TotalDocumentos, p.MontoTotal,
             p.Estado, p.FechaCreacion, p.Temporada,
             facturas_count = (
               SELECT COUNT(*) FROM [cfl].[PlanillaSapFactura] psf
               WHERE psf.IdPlanillaSap = p.IdPlanillaSap
             ),
             periodo_label = (
               SELECT TOP 1
                 CONCAT(
                   CHOOSE(MONTH(fac.FechaEmision),
                     'Enero','Febrero','Marzo','Abril','Mayo','Junio',
                     'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'),
                   ' ', YEAR(fac.FechaEmision))
               FROM [cfl].[PlanillaSapFactura] psf
               INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = psf.IdFactura
               WHERE psf.IdPlanillaSap = p.IdPlanillaSap
               ORDER BY fac.FechaEmision
             ),
             empresas_nombres = (
               SELECT STRING_AGG(sub.empresa_nombre, ', ')
               FROM (
                 SELECT DISTINCT
                   empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa))
                 FROM [cfl].[PlanillaSapFactura] psf
                 INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = psf.IdFactura
                 LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
                 WHERE psf.IdPlanillaSap = p.IdPlanillaSap
               ) sub
             )
      FROM [cfl].[PlanillaSap] p
      ORDER BY p.FechaCreacion DESC;
    `);
    res.json({ data: result.recordset });
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// GET /planillas-sap/movimientos — movements for planilla generation
// ---------------------------------------------------------------------------
router.get('/movimientos', async (req, res, next) => {
  const raw = req.query.facturas_ids;
  if (!raw) { res.status(400).json({ error: 'facturas_ids requerido' }); return; }

  const ids = String(raw).split(',').map(Number).filter(n => Number.isFinite(n) && n > 0);
  if (ids.length === 0) { res.status(400).json({ error: 'facturas_ids inválido' }); return; }

  try {
    const pool = await getPool();
    const request = pool.request();

    // Build safe IN clause with parameters
    const paramClauses = ids.map((id, i) => {
      request.input(`fid${i}`, sql.BigInt, id);
      return `@fid${i}`;
    });

    const result = await request.query(`
      SELECT
        cf.IdCabeceraFlete,
        cf.IdFactura,
        fac.NumeroFactura,
        cf.FechaSalida,
        cf.GuiaRemision AS NumeroGuia,
        cc.SapCodigo AS CentroCostoCodigo,
        cm.Codigo AS CuentaMayorCodigo,
        cm.Glosa AS CuentaMayorGlosa,
        cf.IdProductor,
        prod.Nombre AS ProductorNombre,
        prod.CodigoProveedor,
        cf.MontoAplicado,
        especie_nombre = (
          SELECT TOP 1 esp.Glosa
          FROM [cfl].[DetalleFlete] df
          INNER JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
          WHERE df.IdCabeceraFlete = cf.IdCabeceraFlete
        ),
        tf.Nombre AS TipoFleteNombre
      FROM [cfl].[CabeceraFlete] cf
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = cf.IdFactura
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = cf.IdCuentaMayor
      LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      WHERE cf.IdFactura IN (${paramClauses.join(',')})
        AND UPPER(cf.Estado) IN ('FACTURADO', 'PREFACTURADO', 'COMPLETADO')
      ORDER BY fac.NumeroFactura, cc.SapCodigo, cm.Codigo, prod.Nombre;
    `);

    res.json({ data: result.recordset });
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// GET /planillas-sap/:id — detail
// ---------------------------------------------------------------------------
router.get('/:id', async (req, res, next) => {
  const id = parsePositiveInt(req.params.id, 0);
  if (!id) { res.status(400).json({ error: 'id inválido' }); return; }
  try {
    const pool = await getPool();
    const planilla = await fetchPlanilla(pool, id);
    if (!planilla) { res.status(404).json({ error: 'Planilla no encontrada' }); return; }
    res.json({ data: planilla });
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// POST /planillas-sap/generar — generate from selected movements
// ---------------------------------------------------------------------------
router.post('/generar', validate({ body: generarBody }), async (req, res, next) => {
  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  const {
    facturas_ids, movimientos_ids,
    fecha_documento, fecha_contabilizacion,
    glosa_cabecera,
    temporada, codigo_cargo_abono, glosa_cargo_abono,
    indicador_impuesto, productores_oc,
  } = req.body;

  let transaction;
  try {
    const pool = await getPool();

    // Verify all facturas exist and are in valid state
    const facRequest = pool.request();
    const facParams = facturas_ids.map((id, i) => {
      facRequest.input(`fid${i}`, sql.BigInt, id);
      return `@fid${i}`;
    });
    const facCheck = await facRequest.query(`
      SELECT IdFactura, Estado, NumeroFactura, NumeroFacturaRecibida
      FROM [cfl].[CabeceraFactura]
      WHERE IdFactura IN (${facParams.join(',')});
    `);

    if (facCheck.recordset.length !== facturas_ids.length) {
      const found = new Set(facCheck.recordset.map(r => r.id_factura));
      const missing = facturas_ids.filter(id => !found.has(id));
      res.status(404).json({ error: `Facturas no encontradas: ${missing.join(', ')}` });
      return;
    }

    // Build OC lookup
    const ocMap = {};
    if (productores_oc) {
      for (const p of productores_oc) {
        ocMap[p.id_productor] = { orden_compra: p.orden_compra, posicion_oc: p.posicion_oc || '10' };
      }
    }

    // Fetch selected movements grouped by (factura, centro_costo, cuenta_mayor, productor)
    const movRequest = pool.request();
    const movParams = movimientos_ids.map((id, i) => {
      movRequest.input(`mid${i}`, sql.BigInt, id);
      return `@mid${i}`;
    });
    const facParamsMov = facturas_ids.map((id, i) => {
      movRequest.input(`sfid${i}`, sql.BigInt, id);
      return `@sfid${i}`;
    });

    const movData = await movRequest.query(`
      SELECT
        cf.IdFactura,
        fac.NumeroFactura,
        fac.NumeroFacturaRecibida,
        cc.SapCodigo AS CentroCostoCodigo,
        cm.Codigo AS CuentaMayorCodigo,
        cf.IdProductor,
        prod.CodigoProveedor, prod.Nombre AS ProductorNombre,
        EspecieNombre = (
          SELECT TOP 1 esp.Glosa
          FROM [cfl].[DetalleFlete] df
          INNER JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
          WHERE df.IdCabeceraFlete = cf.IdCabeceraFlete
          ORDER BY COALESCE(df.Cantidad, 0) DESC, COALESCE(df.Peso, 0) DESC
        ),
        cf.MontoAplicado
      FROM [cfl].[CabeceraFlete] cf
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = cf.IdFactura
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = cf.IdCuentaMayor
      LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
      WHERE cf.IdCabeceraFlete IN (${movParams.join(',')})
        AND cf.IdFactura IN (${facParamsMov.join(',')})
        AND UPPER(cf.Estado) IN ('FACTURADO', 'PREFACTURADO', 'COMPLETADO')
      ORDER BY fac.NumeroFactura, cc.SapCodigo, cm.Codigo, prod.Nombre;
    `);

    if (!movData.recordset.length) {
      res.status(422).json({ error: 'No hay movimientos válidos para generar la planilla' });
      return;
    }

    // Validate all producers have CodigoProveedor
    const sinCodigo = movData.recordset.filter(r => !r.codigo_proveedor);
    if (sinCodigo.length > 0) {
      const nombres = [...new Set(sinCodigo.map(r => r.productor_nombre || `Productor #${r.id_productor}`))];
      res.status(422).json({
        error: `Productores sin código SAP: ${nombres.join(', ')}. Asigne un CodigoProveedor antes de generar.`,
      });
      return;
    }

    // Group into SAP documents: each unique Factura + CC + CuentaMayor = 1 document
    // Within each document, group credit lines by Productor + Especie
    const docGroups = new Map();
    for (const row of movData.recordset) {
      const docKey = `${row.id_factura}|${row.centro_costo_codigo || 'SIN_CC'}|${row.cuenta_mayor_codigo || 'SIN_CM'}`;
      if (!docGroups.has(docKey)) {
        docGroups.set(docKey, {
          centro_costo_codigo: row.centro_costo_codigo,
          cuenta_mayor_codigo: row.cuenta_mayor_codigo,
          numero_factura_recibida: row.numero_factura_recibida || null,
          numero_pre_factura: row.numero_factura || null,
          lineas: new Map(), // keyed by productor|especie
        });
      }
      const doc = docGroups.get(docKey);
      const lineKey = `${row.id_productor}|${row.especie_nombre || ''}`;
      if (!doc.lineas.has(lineKey)) {
        doc.lineas.set(lineKey, {
          id_productor: row.id_productor,
          codigo_proveedor: row.codigo_proveedor,
          productor_nombre: row.productor_nombre,
          especie: row.especie_nombre || null,
          monto: 0,
        });
      }
      doc.lineas.get(lineKey).monto += Number(row.monto_aplicado || 0);
    }

    // Build warnings
    const warnings = [];
    const productoresConOc = new Set(Object.keys(ocMap).map(Number));
    const todosProductores = new Set(movData.recordset.map(r => r.id_productor));
    for (const pid of todosProductores) {
      if (!productoresConOc.has(pid)) {
        const nombre = movData.recordset.find(r => r.id_productor === pid)?.productor_nombre;
        warnings.push(`Productor "${nombre || pid}" sin Orden de Compra asignada`);
      }
    }

    // Start transaction
    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    // Insert PlanillaSap header
    const idUsuario = Number(req.jwtPayload?.id_usuario) || null;
    const insertHdr = await new sql.Request(transaction)
      .input('fechaDoc', sql.Date, new Date(fecha_documento))
      .input('fechaContab', sql.Date, new Date(fecha_contabilizacion))
      .input('glosa', sql.NVarChar(100), glosa_cabecera)
      .input('temporada', sql.NVarChar(20), temporada || null)
      .input('codigoCA', sql.NVarChar(20), codigo_cargo_abono || null)
      .input('glosaCA', sql.NVarChar(100), glosa_cargo_abono || null)
      .input('indicadorImp', sql.NVarChar(10), indicador_impuesto || 'C0')
      .input('estado', sql.NVarChar(20), 'generada')
      .input('idUsuario', sql.BigInt, idUsuario)
      .input('createdAt', sql.DateTime2(0), now)
      .query(`
        INSERT INTO [cfl].[PlanillaSap]
          (FechaDocumento, FechaContabilizacion, GlosaCabecera,
           Temporada, CodigoCargoAbono, GlosaCargoAbono, IndicadorImpuesto,
           Estado, IdUsuarioCreador, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdPlanillaSap
        VALUES
          (@fechaDoc, @fechaContab, @glosa,
           @temporada, @codigoCA, @glosaCA, @indicadorImp,
           @estado, @idUsuario, @createdAt, @createdAt);
      `);
    const idPlanilla = Number(insertHdr.recordset[0].id_planilla_sap);

    // Insert PlanillaSapFactura records (N:N bridge)
    for (const facId of facturas_ids) {
      await new sql.Request(transaction)
        .input('idPlanilla', sql.BigInt, idPlanilla)
        .input('idFactura', sql.BigInt, facId)
        .query(`
          INSERT INTO [cfl].[PlanillaSapFactura] (IdPlanillaSap, IdFactura)
          VALUES (@idPlanilla, @idFactura);
        `);
    }

    let totalLineas = 0;
    let montoTotal = 0;
    let docNum = 0;

    for (const [, group] of docGroups) {
      docNum++;
      const creditLines = Array.from(group.lineas.values());
      const montoDebito = creditLines.reduce((s, l) => s + l.monto, 0);
      montoTotal += montoDebito;

      // Per-document referencia and texto derived from the factura
      const docReferencia = group.numero_factura_recibida || null;
      const docTextoCredito = group.numero_pre_factura
        ? `PREFACTURA ${group.numero_pre_factura}`
        : glosa_cabecera;

      // Insert document
      const insertDoc = await new sql.Request(transaction)
        .input('idPlanilla', sql.BigInt, idPlanilla)
        .input('numDoc', sql.Int, docNum)
        .input('ccCodigo', sql.NVarChar(20), group.centro_costo_codigo)
        .input('cmCodigo', sql.NVarChar(30), group.cuenta_mayor_codigo)
        .input('docRef', sql.NVarChar(60), docReferencia)
        .input('docPreFac', sql.NVarChar(40), group.numero_pre_factura)
        .input('montoDebito', sql.Decimal(18, 2), montoDebito)
        .input('totalLineas', sql.Int, 1 + creditLines.length)
        .query(`
          INSERT INTO [cfl].[PlanillaSapDocumento]
            (IdPlanillaSap, NumeroDocumento,
             CentroCostoCodigo, CuentaMayorCodigo,
             Referencia, NumeroPreFactura,
             MontoDebito, TotalLineas)
          OUTPUT INSERTED.IdPlanillaSapDocumento
          VALUES
            (@idPlanilla, @numDoc,
             @ccCodigo, @cmCodigo,
             @docRef, @docPreFac,
             @montoDebito, @totalLineas);
        `);
      const idDoc = Number(insertDoc.recordset[0].id_planilla_sap_documento);

      let lineNum = 0;

      // Debit line (header, clave 50)
      lineNum++;
      totalLineas++;
      await new sql.Request(transaction)
        .input('idDoc', sql.BigInt, idDoc)
        .input('numLinea', sql.Int, lineNum)
        .input('esDocNuevo', sql.Bit, 1)
        .input('clave', sql.NVarChar(10), '50')
        .input('cuentaMayor', sql.NVarChar(30), group.cuenta_mayor_codigo)
        .input('importe', sql.Decimal(18, 2), -montoDebito)
        .input('centroCosto', sql.NVarChar(20), group.centro_costo_codigo)
        .input('nroAsignacion', sql.NVarChar(100), docReferencia)
        .input('textoLinea', sql.NVarChar(100), glosa_cabecera)
        .input('indicadorImp', sql.NVarChar(10), indicador_impuesto || 'C0')
        .input('temporada', sql.NVarChar(20), temporada || null)
        .input('tipoCA', sql.NVarChar(20), codigo_cargo_abono || null)
        .query(`
          INSERT INTO [cfl].[PlanillaSapLinea]
            (IdPlanillaSapDocumento, NumeroLinea, EsDocNuevo, ClaveContabilizacion,
             CuentaMayor, Importe, CentroCosto, NroAsignacion, TextoLinea,
             IndicadorImpuesto, Temporada, TipoCargoAbono)
          VALUES
            (@idDoc, @numLinea, @esDocNuevo, @clave,
             @cuentaMayor, @importe, @centroCosto, @nroAsignacion, @textoLinea,
             @indicadorImp, @temporada, @tipoCA);
        `);

      // Credit lines (one per productor+especie, clave 29)
      for (const linea of creditLines) {
        lineNum++;
        totalLineas++;
        const oc = ocMap[linea.id_productor] || {};
        await new sql.Request(transaction)
          .input('idDoc', sql.BigInt, idDoc)
          .input('numLinea', sql.Int, lineNum)
          .input('esDocNuevo', sql.Bit, 0)
          .input('clave', sql.NVarChar(10), '29')
          .input('codProveedor', sql.NVarChar(20), linea.codigo_proveedor)
          .input('indicadorCME', sql.NVarChar(5), 'A')
          .input('importe', sql.Decimal(18, 2), linea.monto)
          .input('ordenCompra', sql.NVarChar(30), oc.orden_compra || null)
          .input('posicionOC', sql.NVarChar(10), oc.posicion_oc || '10')
          .input('nroAsignacion', sql.NVarChar(100), linea.especie)
          .input('textoLinea', sql.NVarChar(100), docTextoCredito)
          .input('indicadorImp', sql.NVarChar(10), indicador_impuesto || 'C0')
          .input('temporada', sql.NVarChar(20), temporada || null)
          .input('tipoCA', sql.NVarChar(20), codigo_cargo_abono || null)
          .query(`
            INSERT INTO [cfl].[PlanillaSapLinea]
              (IdPlanillaSapDocumento, NumeroLinea, EsDocNuevo, ClaveContabilizacion,
               CodigoProveedor, IndicadorCME, Importe, OrdenCompra, PosicionOC,
               NroAsignacion, TextoLinea, IndicadorImpuesto, Temporada, TipoCargoAbono)
            VALUES
              (@idDoc, @numLinea, @esDocNuevo, @clave,
               @codProveedor, @indicadorCME, @importe, @ordenCompra, @posicionOC,
               @nroAsignacion, @textoLinea, @indicadorImp, @temporada, @tipoCA);
          `);
      }
    }

    // Update totals
    await new sql.Request(transaction)
      .input('id', sql.BigInt, idPlanilla)
      .input('totalLineas', sql.Int, totalLineas)
      .input('totalDocs', sql.Int, docNum)
      .input('montoTotal', sql.Decimal(18, 2), montoTotal)
      .input('updatedAt', sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[PlanillaSap]
        SET TotalLineas = @totalLineas, TotalDocumentos = @totalDocs,
            MontoTotal = @montoTotal, FechaActualizacion = @updatedAt
        WHERE IdPlanillaSap = @id;
      `);

    await transaction.commit();

    const response = {
      data: { id_planilla_sap: idPlanilla, total_lineas: totalLineas, total_documentos: docNum },
    };
    if (warnings.length > 0) response.warnings = warnings;

    res.status(201).json(response);
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /planillas-sap/:id/facturas-elegibles — available prefacturas for same period
// ---------------------------------------------------------------------------
router.get('/:id/facturas-elegibles', async (req, res, next) => {
  const id = parsePositiveInt(req.params.id, 0);
  if (!id) { res.status(400).json({ error: 'id inválido' }); return; }

  try {
    const pool = await getPool();

    // Get planilla's current period (from its linked facturas)
    const periodoResult = await pool.request()
      .input('id', sql.BigInt, id)
      .query(`
        SELECT DISTINCT YEAR(fac.FechaEmision) AS anio, MONTH(fac.FechaEmision) AS mes
        FROM [cfl].[PlanillaSapFactura] psf
        INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = psf.IdFactura
        WHERE psf.IdPlanillaSap = @id;
      `);

    if (!periodoResult.recordset.length) {
      res.json({ data: [] });
      return;
    }

    // Build period filter (may have multiple months if mixed, but typically one)
    const periodos = periodoResult.recordset;
    const periodConditions = periodos.map((p, i) => `(YEAR(fac.FechaEmision) = @anio${i} AND MONTH(fac.FechaEmision) = @mes${i})`);

    const eligRequest = pool.request().input('id', sql.BigInt, id);
    periodos.forEach((p, i) => {
      eligRequest.input(`anio${i}`, sql.Int, p.anio);
      eligRequest.input(`mes${i}`, sql.Int, p.mes);
    });

    const eligResult = await eligRequest.query(`
      SELECT
        fac.IdFactura,
        fac.NumeroFactura,
        fac.FechaEmision,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa)),
        fac.MontoTotal
      FROM [cfl].[CabeceraFactura] fac
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      WHERE LOWER(fac.estado) = 'recibida'
        AND (${periodConditions.join(' OR ')})
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[PlanillaSapFactura] psf
          WHERE psf.IdFactura = fac.IdFactura
        )
      ORDER BY fac.FechaEmision DESC;
    `);

    res.json({ data: eligResult.recordset });
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// POST /planillas-sap/:id/facturas — add pre facturas to generada planilla
// ---------------------------------------------------------------------------
router.post('/:id/facturas', validate({ params: idParam, body: agregarFacturasBody }), async (req, res, next) => {
  const id = Number(req.params.id);
  const { facturas_ids } = req.body;

  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();

    // Verify planilla exists and is generada
    const planCheck = await pool.request().input('id', sql.BigInt, id).query(`
      SELECT Estado FROM [cfl].[PlanillaSap] WHERE IdPlanillaSap = @id;
    `);
    if (!planCheck.recordset[0]) { res.status(404).json({ error: 'Planilla no encontrada' }); return; }
    if (planCheck.recordset[0].estado.toLowerCase() !== 'generada') {
      res.status(409).json({ error: 'Solo se pueden editar planillas en estado generada' }); return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();

    // Insert bridge records
    for (const facId of facturas_ids) {
      await new sql.Request(transaction)
        .input('idPlanilla', sql.BigInt, id)
        .input('idFactura', sql.BigInt, facId)
        .query(`
          IF NOT EXISTS (
            SELECT 1 FROM [cfl].[PlanillaSapFactura]
            WHERE IdPlanillaSap = @idPlanilla AND IdFactura = @idFactura
          )
          INSERT INTO [cfl].[PlanillaSapFactura] (IdPlanillaSap, IdFactura)
          VALUES (@idPlanilla, @idFactura);
        `);
    }

    // Regenerate documents and lines
    await regenerateDocuments(transaction, id);
    await transaction.commit();

    res.json({ message: `${facturas_ids.length} pre factura(s) agregada(s)` });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// DELETE /planillas-sap/:id/facturas/:id_factura — remove pre factura from planilla
// ---------------------------------------------------------------------------
router.delete('/:id/facturas/:id_factura', async (req, res, next) => {
  const id = parsePositiveInt(req.params.id, 0);
  const idFactura = parsePositiveInt(req.params.id_factura, 0);
  if (!id || !idFactura) { res.status(400).json({ error: 'Parámetros inválidos' }); return; }

  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  let transaction;
  try {
    const pool = await getPool();

    // Verify planilla is generada
    const planCheck = await pool.request().input('id', sql.BigInt, id).query(`
      SELECT Estado FROM [cfl].[PlanillaSap] WHERE IdPlanillaSap = @id;
    `);
    if (!planCheck.recordset[0]) { res.status(404).json({ error: 'Planilla no encontrada' }); return; }
    if (planCheck.recordset[0].estado.toLowerCase() !== 'generada') {
      res.status(409).json({ error: 'Solo se pueden editar planillas en estado generada' }); return;
    }

    // Check that at least one factura will remain
    const countResult = await pool.request().input('id', sql.BigInt, id).query(`
      SELECT COUNT(*) AS total FROM [cfl].[PlanillaSapFactura] WHERE IdPlanillaSap = @id;
    `);
    if (countResult.recordset[0].total <= 1) {
      res.status(422).json({ error: 'La planilla debe tener al menos una pre factura. Use anular para eliminarla por completo.' });
      return;
    }

    transaction = new sql.Transaction(pool);
    await transaction.begin();

    // Remove bridge record
    await new sql.Request(transaction)
      .input('idPlanilla', sql.BigInt, id)
      .input('idFactura', sql.BigInt, idFactura)
      .query(`
        DELETE FROM [cfl].[PlanillaSapFactura]
        WHERE IdPlanillaSap = @idPlanilla AND IdFactura = @idFactura;
      `);

    // Regenerate documents and lines
    await regenerateDocuments(transaction, id);
    await transaction.commit();

    res.json({ message: 'Pre factura quitada de la planilla' });
  } catch (err) {
    if (transaction) { try { await transaction.rollback(); } catch (_) {} }
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /planillas-sap/:id/export — download Excel
// ---------------------------------------------------------------------------
router.get('/:id/export', async (req, res, next) => {
  const id = parsePositiveInt(req.params.id, 0);
  if (!id) { res.status(400).json({ error: 'id inválido' }); return; }

  try {
    const pool = await getPool();
    const planilla = await fetchPlanilla(pool, id);
    if (!planilla) { res.status(404).json({ error: 'Planilla no encontrada' }); return; }

    const wb = generateSapExcel(planilla);
    const filename = `planilla-sap-${(planilla.periodo_label || id).replace(/\s+/g, '-')}.xlsx`;

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// PATCH /planillas-sap/:id/estado — enviada or anulada
// ---------------------------------------------------------------------------
router.patch('/:id/estado', validate({ params: idParam, body: cambiarEstadoBody }), async (req, res, next) => {
  const id = req.params.id;
  const { estado } = req.body;

  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  try {
    const pool = await getPool();

    // Validate current state
    const current = await pool.request().input('id', sql.BigInt, id).query(`
      SELECT Estado FROM [cfl].[PlanillaSap] WHERE IdPlanillaSap = @id;
    `);
    if (!current.recordset[0]) { res.status(404).json({ error: 'Planilla no encontrada' }); return; }
    const estadoActual = current.recordset[0].estado.toLowerCase();

    if (estadoActual !== 'generada') {
      res.status(409).json({ error: `No se puede cambiar estado desde '${estadoActual}'. Solo planillas en estado generada pueden ser modificadas.` });
      return;
    }

    const now = new Date();

    if (estado === 'anulada') {
      // Anular: delete bridge records so pre-facturas become available again
      let transaction = new sql.Transaction(pool);
      await transaction.begin();
      try {
        // Delete lines, documents, bridge records
        await new sql.Request(transaction).input('id', sql.BigInt, id).query(`
          DELETE l FROM [cfl].[PlanillaSapLinea] l
          INNER JOIN [cfl].[PlanillaSapDocumento] d ON d.IdPlanillaSapDocumento = l.IdPlanillaSapDocumento
          WHERE d.IdPlanillaSap = @id;
        `);
        await new sql.Request(transaction).input('id', sql.BigInt, id).query(`
          DELETE FROM [cfl].[PlanillaSapDocumento] WHERE IdPlanillaSap = @id;
        `);
        await new sql.Request(transaction).input('id', sql.BigInt, id).query(`
          DELETE FROM [cfl].[PlanillaSapFactura] WHERE IdPlanillaSap = @id;
        `);
        await new sql.Request(transaction)
          .input('id', sql.BigInt, id)
          .input('estado', sql.NVarChar(20), 'anulada')
          .input('updatedAt', sql.DateTime2(0), now)
          .query(`
            UPDATE [cfl].[PlanillaSap]
            SET Estado = @estado, TotalLineas = 0, TotalDocumentos = 0, MontoTotal = 0,
                FechaActualizacion = @updatedAt
            WHERE IdPlanillaSap = @id;
          `);
        await transaction.commit();
      } catch (txErr) {
        try { await transaction.rollback(); } catch (_) {}
        throw txErr;
      }
    } else {
      // enviada: just update estado
      await pool.request()
        .input('id', sql.BigInt, id)
        .input('estado', sql.NVarChar(20), estado)
        .input('updatedAt', sql.DateTime2(0), now)
        .query(`
          UPDATE [cfl].[PlanillaSap]
          SET Estado = @estado, FechaActualizacion = @updatedAt
          WHERE IdPlanillaSap = @id;
        `);
    }

    res.json({ message: `Planilla marcada como ${estado}` });
  } catch (err) { next(err); }
});

module.exports = { planillasSapRouter: router };
