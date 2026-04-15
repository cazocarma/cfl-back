const { getPool, sql } = require("../../db");
const { config } = require("../../config");
const { logger } = require("../../logger");
const { queryOdata } = require("./sap-query");

const ODATA_DESTINATION = "PRD_GW";
const ODATA_ENTITY_SET = "YWT_B_GET_BPTB00_CDS/YWT_B_GET_BPTB00";

// ---------------------------------------------------------------------------
// OData record → Productor row mapping
// ---------------------------------------------------------------------------

function trim(value, maxLen) {
  const s = String(value ?? "").trim();
  return maxLen ? s.slice(0, maxLen) : s;
}

function parseSapDate(value) {
  if (!value) return null;
  const s = String(value).trim();

  // OData: "/Date(1234567890000)/" format
  const odataMatch = s.match(/\/Date\((\d+)\)\//);
  if (odataMatch) {
    return new Date(Number(odataMatch[1]));
  }

  // ISO or YYYYMMDD
  if (/^\d{8}$/.test(s)) {
    const y = Number(s.slice(0, 4));
    const m = Number(s.slice(4, 6));
    const d = Number(s.slice(6, 8));
    const date = new Date(Date.UTC(y, m - 1, d));
    return Number.isNaN(date.getTime()) ? null : date;
  }

  const date = new Date(s);
  return Number.isNaN(date.getTime()) ? null : date;
}

function field(record, name) {
  return record[name] ?? record[name.toLowerCase()] ?? record[name.toUpperCase()] ?? "";
}

function mapOdataToProductor(record) {
  const name1 = trim(field(record, "NAME1"), 80);
  const name2 = trim(field(record, "NAME2"), 80);
  const nombre = [name1, name2].filter(Boolean).join(" ").slice(0, 150);

  return {
    codigoProveedor: trim(field(record, "PARTNER"), 20),
    rut: trim(field(record, "BU_SORT1"), 20) || null,
    nombre: nombre || trim(field(record, "PARTNER"), 150),
    pais: trim(field(record, "REGION"), 2) ? "CL" : null,
    region: trim(field(record, "REGION"), 10) || null,
    comuna: trim(field(record, "BEZEI"), 100) || null,
    distrito: trim(field(record, "CITY2"), 100) || null,
    calle: trim(field(record, "STREET"), 150) || null,
    fechaActualizacionSap: parseSapDate(field(record, "AEDAT")),
  };
}

// ---------------------------------------------------------------------------
// Sync: fetch from SAP OData → MERGE into cfl.Productor
// ---------------------------------------------------------------------------

async function syncProductores() {
  const destination = config.sapEtl.defaultDestination === "PRD" ? ODATA_DESTINATION : ODATA_DESTINATION;

  logger.info("Iniciando sincronizacion de productores desde SAP OData");

  const records = await queryOdata(destination, ODATA_ENTITY_SET, { top: 10000 });

  if (!records || records.length === 0) {
    logger.info("SAP OData no devolvio registros de productores");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  logger.info({ count: records.length }, "Registros OData recibidos para productores");

  const productores = records
    .map(mapOdataToProductor)
    .filter((p) => p.codigoProveedor && p.nombre);

  // Deduplicate by codigoProveedor (keep last)
  const byKey = new Map();
  for (const p of productores) {
    byKey.set(p.codigoProveedor, p);
  }
  const unique = [...byKey.values()];

  const pool = await getPool();
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  const stgName = `##stg_productores_${process.pid}_${Date.now()}`;

  try {
    // Create global temp table (## so bulk load's separate connection can see it)
    await new sql.Request(transaction).query(`
      CREATE TABLE ${stgName} (
        CodigoProveedor NVARCHAR(20) NOT NULL,
        Rut             NVARCHAR(20) NULL,
        Nombre          NVARCHAR(150) NOT NULL,
        Pais            CHAR(2) NULL,
        Region          NVARCHAR(10) NULL,
        Comuna          NVARCHAR(100) NULL,
        Distrito        NVARCHAR(100) NULL,
        Calle           NVARCHAR(150) NULL,
        FechaActualizacionSap DATETIME2(3) NULL
      );
    `);

    // Bulk insert to staging
    const stageTable = new sql.Table(stgName);
    stageTable.create = false;
    stageTable.columns.add("CodigoProveedor", sql.NVarChar(20), { nullable: false });
    stageTable.columns.add("Rut", sql.NVarChar(20), { nullable: true });
    stageTable.columns.add("Nombre", sql.NVarChar(150), { nullable: false });
    stageTable.columns.add("Pais", sql.Char(2), { nullable: true });
    stageTable.columns.add("Region", sql.NVarChar(10), { nullable: true });
    stageTable.columns.add("Comuna", sql.NVarChar(100), { nullable: true });
    stageTable.columns.add("Distrito", sql.NVarChar(100), { nullable: true });
    stageTable.columns.add("Calle", sql.NVarChar(150), { nullable: true });
    stageTable.columns.add("FechaActualizacionSap", sql.DateTime2(3), { nullable: true });

    for (const p of unique) {
      stageTable.rows.add(
        p.codigoProveedor,
        p.rut,
        p.nombre,
        p.pais,
        p.region,
        p.comuna,
        p.distrito,
        p.calle,
        p.fechaActualizacionSap
      );
    }

    const bulkRequest = new sql.Request(transaction);
    await bulkRequest.bulk(stageTable);

    // MERGE with OUTPUT to count inserts/updates
    const result = await new sql.Request(transaction).query(`
      CREATE TABLE #merge_actions (MergeAction NVARCHAR(10));

      MERGE [cfl].[Productor] AS tgt
      USING ${stgName} AS src
        ON tgt.CodigoProveedor = src.CodigoProveedor
      WHEN MATCHED AND (
           ISNULL(tgt.Rut, '')     <> ISNULL(src.Rut, '')
        OR tgt.Nombre              <> src.Nombre
        OR ISNULL(tgt.Pais, '')    <> ISNULL(src.Pais, '')
        OR ISNULL(tgt.Region, '')  <> ISNULL(src.Region, '')
        OR ISNULL(tgt.Comuna, '')  <> ISNULL(src.Comuna, '')
        OR ISNULL(tgt.Distrito,'') <> ISNULL(src.Distrito, '')
        OR ISNULL(tgt.Calle, '')   <> ISNULL(src.Calle, '')
      ) THEN UPDATE SET
        tgt.Rut = src.Rut,
        tgt.Nombre = src.Nombre,
        tgt.Pais = src.Pais,
        tgt.Region = src.Region,
        tgt.Comuna = src.Comuna,
        tgt.Distrito = src.Distrito,
        tgt.Calle = src.Calle,
        tgt.FechaActualizacionSap = src.FechaActualizacionSap,
        tgt.FechaActualizacion = SYSUTCDATETIME()
      WHEN NOT MATCHED BY TARGET THEN INSERT (
        CodigoProveedor, Rut, Nombre, Pais, Region, Comuna, Distrito, Calle,
        Activo, FechaActualizacionSap, FechaCreacion, FechaActualizacion
      ) VALUES (
        src.CodigoProveedor, src.Rut, src.Nombre, src.Pais, src.Region,
        src.Comuna, src.Distrito, src.Calle,
        1, src.FechaActualizacionSap, SYSUTCDATETIME(), SYSUTCDATETIME()
      )
      OUTPUT $action INTO #merge_actions;

      SELECT
        inserted  = ISNULL(SUM(CASE WHEN MergeAction = 'INSERT' THEN 1 ELSE 0 END), 0),
        updated   = ISNULL(SUM(CASE WHEN MergeAction = 'UPDATE' THEN 1 ELSE 0 END), 0)
      FROM #merge_actions;

      DROP TABLE #merge_actions;
    `);

    await new sql.Request(transaction).query(`DROP TABLE ${stgName};`);
    await transaction.commit();

    const inserted = result.recordset[0]?.inserted || 0;
    const updated = result.recordset[0]?.updated || 0;
    const unchanged = unique.length - inserted - updated;

    logger.info({ inserted, updated, unchanged, total: unique.length }, "Sincronizacion de productores completada");

    return { inserted, updated, unchanged, total: unique.length };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    try { await pool.request().query(`IF OBJECT_ID('tempdb..${stgName}') IS NOT NULL DROP TABLE ${stgName};`); } catch { /* no-op */ }
    const inner = error?.originalError || error?.precedingErrors?.[0] || error?.errors?.[0];
    logger.error({
      message: error?.message,
      code: error?.code,
      innerMessage: inner?.message,
      innerNumber: inner?.number,
      innerState: inner?.state,
      innerClass: inner?.class,
      innerLineNumber: inner?.lineNumber,
      innerKeys: inner ? Object.keys(inner) : null,
      errorKeys: Object.keys(error || {}),
      allErrors: JSON.stringify(
        (error?.precedingErrors || error?.errors || []).map((e) => ({
          message: e?.message, number: e?.number, state: e?.state, lineNumber: e?.lineNumber,
        }))
      ),
    }, "Fallo sync-productores (detalle SQL)");
    throw error;
  }
}

module.exports = { syncProductores };
