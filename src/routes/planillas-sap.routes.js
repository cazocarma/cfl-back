'use strict';

const express = require('express');
const { getPool, sql } = require('../db');
const { resolveAuthzContext, hasAnyPermission } = require('../authz');
const { parsePositiveInt } = require('../utils/parse');
const { validate } = require('../middleware/validate.middleware');
const { generarBody, cambiarEstadoBody, idParam } = require('../schemas/planillas-sap.schemas');
const { generateSapExcel } = require('../services/planilla-sap-export');

const router = express.Router();

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
      SELECT p.*,
        fac.NumeroFactura,
        empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa))
      FROM [cfl].[PlanillaSap] p
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = p.IdFactura
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      WHERE p.IdPlanillaSap = @id;
    `);
  if (!hdr.recordset[0]) return null;
  const planilla = hdr.recordset[0];

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

// ---------------------------------------------------------------------------
// GET /planillas-sap — list
// ---------------------------------------------------------------------------
router.get('/', async (req, res, next) => {
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT p.IdPlanillaSap, p.IdFactura, p.FechaDocumento, p.FechaContabilizacion,
             p.GlosaCabecera, p.TotalLineas, p.TotalDocumentos, p.MontoTotal,
             p.Estado, p.FechaCreacion, p.Temporada,
             fac.NumeroFactura,
             empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), CONCAT('Empresa #', fac.IdEmpresa))
      FROM [cfl].[PlanillaSap] p
      INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = p.IdFactura
      LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = fac.IdEmpresa
      ORDER BY p.FechaCreacion DESC;
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
// POST /planillas-sap/generar — generate from a pre-factura
// ---------------------------------------------------------------------------
router.post('/generar', validate({ body: generarBody }), async (req, res, next) => {
  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  const {
    id_factura, fecha_documento, fecha_contabilizacion,
    glosa_cabecera, temporada, codigo_cargo_abono, glosa_cargo_abono,
    indicador_impuesto, productores_oc,
  } = req.body;

  let transaction;
  try {
    const pool = await getPool();

    // Verify factura exists and is recibida or borrador
    const facCheck = await pool.request()
      .input('idFactura', sql.BigInt, id_factura)
      .query(`SELECT IdFactura, estado FROM [cfl].[CabeceraFactura] WHERE IdFactura = @idFactura;`);
    if (!facCheck.recordset[0]) { res.status(404).json({ error: 'Pre factura no encontrada' }); return; }

    // Build OC lookup from user input
    const ocMap = {};
    if (productores_oc) {
      for (const p of productores_oc) {
        ocMap[p.id_productor] = { orden_compra: p.orden_compra, posicion_oc: p.posicion_oc || '10' };
      }
    }

    // Fetch movements grouped by (folio, centro_costo, cuenta_mayor) then by producer
    const movData = await pool.request()
      .input('idFactura', sql.BigInt, id_factura)
      .query(`
        SELECT
          cf.IdFolio,
          fol.FolioNumero,
          cf.IdCentroCosto, cc.SapCodigo AS CentroCostoCodigo,
          cf.IdCuentaMayor, cm.Codigo AS CuentaMayorCodigo,
          cf.IdProductor,
          prod.CodigoProveedor, prod.Nombre AS ProductorNombre,
          SUM(cf.MontoAplicado) AS MontoProductor,
          especie_primaria = (
            SELECT TOP 1 esp.Glosa
            FROM [cfl].[DetalleFlete] df
            INNER JOIN [cfl].[Especie] esp ON esp.IdEspecie = df.IdEspecie
            WHERE df.IdCabeceraFlete IN (
              SELECT MIN(cf2.IdCabeceraFlete)
              FROM [cfl].[CabeceraFlete] cf2
              WHERE cf2.IdFolio = cf.IdFolio AND cf2.IdProductor = cf.IdProductor
                AND cf2.IdCentroCosto = cf.IdCentroCosto
            )
          )
        FROM [cfl].[FacturaFolio] ff
        INNER JOIN [cfl].[CabeceraFlete] cf ON cf.IdFolio = ff.IdFolio
        INNER JOIN [cfl].[Folio] fol ON fol.IdFolio = cf.IdFolio
        LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
        LEFT JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = cf.IdCuentaMayor
        LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor
        WHERE ff.IdFactura = @idFactura
          AND UPPER(cf.Estado) IN ('FACTURADO', 'ASIGNADO_FOLIO')
        GROUP BY cf.IdFolio, fol.FolioNumero, cf.IdCentroCosto, cc.SapCodigo,
                 cf.IdCuentaMayor, cm.Codigo, cf.IdProductor,
                 prod.CodigoProveedor, prod.Nombre
        ORDER BY cf.IdFolio, cc.SapCodigo, prod.Nombre;
      `);

    if (!movData.recordset.length) {
      res.status(422).json({ error: 'No hay movimientos para generar la planilla' });
      return;
    }

    // Group into SAP documents: each unique (IdFolio, CentroCostoCodigo) = 1 document
    const docGroups = new Map();
    for (const row of movData.recordset) {
      const key = `${row.id_folio}_${row.centro_costo_codigo || 'SIN_CC'}`;
      if (!docGroups.has(key)) {
        docGroups.set(key, {
          id_folio: row.id_folio,
          folio_numero: row.folio_numero,
          id_centro_costo: row.id_centro_costo,
          centro_costo_codigo: row.centro_costo_codigo,
          id_cuenta_mayor: row.id_cuenta_mayor,
          cuenta_mayor_codigo: row.cuenta_mayor_codigo,
          productores: [],
        });
      }
      docGroups.get(key).productores.push(row);
    }

    // Start transaction
    transaction = new sql.Transaction(pool);
    await transaction.begin();
    const now = new Date();

    // Insert PlanillaSap header
    const insertHdr = await new sql.Request(transaction)
      .input('idFactura', sql.BigInt, id_factura)
      .input('fechaDoc', sql.Date, new Date(fecha_documento))
      .input('fechaContab', sql.Date, new Date(fecha_contabilizacion))
      .input('glosa', sql.NVarChar(100), glosa_cabecera)
      .input('temporada', sql.NVarChar(20), temporada || null)
      .input('codigoCA', sql.NVarChar(20), codigo_cargo_abono || null)
      .input('glosaCA', sql.NVarChar(100), glosa_cargo_abono || null)
      .input('indicadorImp', sql.NVarChar(10), indicador_impuesto || 'C0')
      .input('estado', sql.NVarChar(20), 'generada')
      .input('createdAt', sql.DateTime2(0), now)
      .query(`
        INSERT INTO [cfl].[PlanillaSap]
          (IdFactura, FechaDocumento, FechaContabilizacion, GlosaCabecera,
           Temporada, CodigoCargoAbono, GlosaCargoAbono, IndicadorImpuesto,
           Estado, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdPlanillaSap
        VALUES
          (@idFactura, @fechaDoc, @fechaContab, @glosa,
           @temporada, @codigoCA, @glosaCA, @indicadorImp,
           @estado, @createdAt, @createdAt);
      `);
    const idPlanilla = Number(insertHdr.recordset[0].id_planilla_sap);

    let totalLineas = 0;
    let montoTotal = 0;
    let docNum = 0;

    for (const [, group] of docGroups) {
      docNum++;
      const montoDebito = group.productores.reduce((s, p) => s + Number(p.monto_productor || 0), 0);
      montoTotal += montoDebito;

      // Insert document
      const insertDoc = await new sql.Request(transaction)
        .input('idPlanilla', sql.BigInt, idPlanilla)
        .input('numDoc', sql.Int, docNum)
        .input('idFolio', sql.BigInt, group.id_folio)
        .input('folioNum', sql.NVarChar(30), group.folio_numero)
        .input('idCC', sql.BigInt, group.id_centro_costo)
        .input('ccCodigo', sql.NVarChar(20), group.centro_costo_codigo)
        .input('idCM', sql.BigInt, group.id_cuenta_mayor)
        .input('cmCodigo', sql.NVarChar(30), group.cuenta_mayor_codigo)
        .input('montoDebito', sql.Decimal(18, 2), montoDebito)
        .input('totalLineas', sql.Int, 1 + group.productores.length)
        .query(`
          INSERT INTO [cfl].[PlanillaSapDocumento]
            (IdPlanillaSap, NumeroDocumento, IdFolio, FolioNumero,
             IdCentroCosto, CentroCostoCodigo, IdCuentaMayor, CuentaMayorCodigo,
             MontoDebito, TotalLineas)
          OUTPUT INSERTED.IdPlanillaSapDocumento
          VALUES
            (@idPlanilla, @numDoc, @idFolio, @folioNum,
             @idCC, @ccCodigo, @idCM, @cmCodigo, @montoDebito, @totalLineas);
        `);
      const idDoc = Number(insertDoc.recordset[0].id_planilla_sap_documento);

      let lineNum = 0;
      const textoLinea = `FLETES FOLIO ${group.folio_numero || ''}`.trim();

      // Debit line (header)
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
        .input('textoLinea', sql.NVarChar(100), textoLinea)
        .input('indicadorImp', sql.NVarChar(10), indicador_impuesto || 'C0')
        .input('temporada', sql.NVarChar(20), temporada || null)
        .input('tipoCA', sql.NVarChar(20), codigo_cargo_abono || null)
        .query(`
          INSERT INTO [cfl].[PlanillaSapLinea]
            (IdPlanillaSapDocumento, NumeroLinea, EsDocNuevo, ClaveContabilizacion,
             CuentaMayor, Importe, CentroCosto, TextoLinea,
             IndicadorImpuesto, Temporada, TipoCargoAbono)
          VALUES
            (@idDoc, @numLinea, @esDocNuevo, @clave,
             @cuentaMayor, @importe, @centroCosto, @textoLinea,
             @indicadorImp, @temporada, @tipoCA);
        `);

      // Credit lines (one per producer)
      for (const prod of group.productores) {
        lineNum++;
        totalLineas++;
        const oc = ocMap[prod.id_productor] || {};
        await new sql.Request(transaction)
          .input('idDoc', sql.BigInt, idDoc)
          .input('numLinea', sql.Int, lineNum)
          .input('esDocNuevo', sql.Bit, 0)
          .input('clave', sql.NVarChar(10), '29')
          .input('codProveedor', sql.NVarChar(20), prod.codigo_proveedor)
          .input('indicadorCME', sql.NVarChar(5), 'A')
          .input('importe', sql.Decimal(18, 2), Number(prod.monto_productor || 0))
          .input('ordenCompra', sql.NVarChar(30), oc.orden_compra || null)
          .input('posicionOC', sql.NVarChar(10), oc.posicion_oc || '10')
          .input('nroAsignacion', sql.NVarChar(100), prod.especie_primaria || null)
          .input('textoLinea', sql.NVarChar(100), textoLinea)
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
    res.status(201).json({ data: { id_planilla_sap: idPlanilla, total_lineas: totalLineas, total_documentos: docNum } });
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
    const filename = `planilla-sap-${planilla.numero_factura || id}.xlsx`;

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// PATCH /planillas-sap/:id/estado
// ---------------------------------------------------------------------------
router.patch('/:id/estado', validate({ params: idParam, body: cambiarEstadoBody }), async (req, res, next) => {
  const id = req.params.id;
  const { estado } = req.body;

  const { allowed } = await checkPlanillasPerm(req);
  if (!allowed) { res.status(403).json({ error: 'Sin permiso' }); return; }

  try {
    const pool = await getPool();
    const now = new Date();
    await pool.request()
      .input('id', sql.BigInt, id)
      .input('estado', sql.NVarChar(20), estado)
      .input('updatedAt', sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[PlanillaSap]
        SET Estado = @estado, FechaActualizacion = @updatedAt
        WHERE IdPlanillaSap = @id;
      `);
    res.json({ message: `Planilla marcada como ${estado}` });
  } catch (err) { next(err); }
});

module.exports = { planillasSapRouter: router };
