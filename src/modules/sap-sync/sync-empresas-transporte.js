const { getPool, sql } = require("../../db");
const { logger } = require("../../logger");
const { queryRfc } = require("./sap-query");

const RFC_DESTINATION = "PRD";
const RFC_TABLE = "YWT_CDTB24";
const RFC_FIELDS = [
  "YWT_CDFIELD24",
  "TRANSPORTCO_ABREV",
  "TRANSPORTCO_DENOMINA",
];
// YWT_CDTB24 es multi-idioma. Nos quedamos solo con la fila en español
// para evitar duplicados por idioma y descartamos la fila sin código.
const RFC_WHERE = "LANGU = 'S'";

function trim(value, maxLen) {
  const s = String(value ?? "").trim();
  return maxLen ? s.slice(0, maxLen) : s;
}

function mapRecord(record) {
  const sapCodigo = trim(record.YWT_CDFIELD24, 10);
  if (!sapCodigo) return null;

  const denomina = trim(record.TRANSPORTCO_DENOMINA, 100);
  const abrev = trim(record.TRANSPORTCO_ABREV, 100);

  return {
    sapCodigo,
    razonSocial: denomina || abrev || sapCodigo,
    nombreRepresentante: abrev || null,
  };
}

async function syncEmpresasTransporte() {
  logger.info("Iniciando sincronizacion de empresas de transporte desde SAP RFC YWT_CDTB24");

  const records = await queryRfc(RFC_DESTINATION, RFC_TABLE, RFC_FIELDS, RFC_WHERE, 0);

  if (!records || records.length === 0) {
    logger.info("SAP RFC no devolvio empresas de transporte");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  const mapped = records.map(mapRecord).filter(Boolean);

  // Dedup por SapCodigo (con LANGU='S' filtrado ya no deberían haber
  // duplicados; conservamos la garantía por defensa).
  const byKey = new Map();
  for (const row of mapped) {
    if (!byKey.has(row.sapCodigo)) byKey.set(row.sapCodigo, row);
  }
  const unique = [...byKey.values()];

  if (unique.length === 0) {
    logger.info("SAP RFC devolvio filas pero ninguna válida tras mapeo");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  const pool = await getPool();
  const transaction = new sql.Transaction(pool);
  const stgName = `##stg_empresas_transporte_sync_${process.pid}_${Date.now()}`;

  await transaction.begin();

  try {
    await new sql.Request(transaction).query(`
      CREATE TABLE ${stgName} (
        SapCodigo           NVARCHAR(10)  NOT NULL,
        RazonSocial         NVARCHAR(100) NOT NULL,
        NombreRepresentante NVARCHAR(100) NULL
      );
    `);

    const stage = new sql.Table(stgName);
    stage.create = false;
    stage.columns.add("SapCodigo", sql.NVarChar(10), { nullable: false });
    stage.columns.add("RazonSocial", sql.NVarChar(100), { nullable: false });
    stage.columns.add("NombreRepresentante", sql.NVarChar(100), { nullable: true });

    for (const r of unique) {
      stage.rows.add(r.sapCodigo, r.razonSocial, r.nombreRepresentante);
    }

    await new sql.Request(transaction).bulk(stage);

    const now = new Date();
    const result = await new sql.Request(transaction)
      .input("now", sql.DateTime2(0), now)
      .query(`
        CREATE TABLE #updated_empresas (IdEmpresa BIGINT NOT NULL);
        CREATE TABLE #inserted_empresas (IdEmpresa BIGINT NOT NULL);

        UPDATE tgt
           SET RazonSocial = s.RazonSocial,
               NombreRepresentante = COALESCE(s.NombreRepresentante, tgt.NombreRepresentante),
               Activo = 1,
               FechaActualizacion = @now
        OUTPUT INSERTED.IdEmpresa INTO #updated_empresas
        FROM cfl.EmpresaTransporte tgt
        INNER JOIN ${stgName} s
          ON UPPER(LTRIM(RTRIM(tgt.SapCodigo))) = UPPER(LTRIM(RTRIM(s.SapCodigo)))
        WHERE ISNULL(tgt.RazonSocial, '') <> ISNULL(s.RazonSocial, '')
           OR ISNULL(tgt.NombreRepresentante, '') <> ISNULL(s.NombreRepresentante, ISNULL(tgt.NombreRepresentante, ''))
           OR ISNULL(tgt.Activo, 0) <> 1;

        INSERT INTO cfl.EmpresaTransporte
          (SapCodigo, Rut, RazonSocial, NombreRepresentante, Correo, Telefono, Activo, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdEmpresa INTO #inserted_empresas
        SELECT s.SapCodigo, NULL, s.RazonSocial, s.NombreRepresentante, NULL, NULL, 1, @now, @now
        FROM ${stgName} s
        WHERE NOT EXISTS (
          SELECT 1 FROM cfl.EmpresaTransporte t
          WHERE UPPER(LTRIM(RTRIM(t.SapCodigo))) = UPPER(LTRIM(RTRIM(s.SapCodigo)))
        );

        SELECT
          inserted = (SELECT COUNT_BIG(1) FROM #inserted_empresas),
          updated  = (SELECT COUNT_BIG(1) FROM #updated_empresas);

        DROP TABLE #inserted_empresas;
        DROP TABLE #updated_empresas;
      `);

    await new sql.Request(transaction).query(`DROP TABLE ${stgName};`);
    await transaction.commit();

    const inserted = Number(result.recordset[0]?.inserted || 0);
    const updated = Number(result.recordset[0]?.updated || 0);
    const unchanged = unique.length - inserted - updated;

    logger.info({ inserted, updated, unchanged, total: unique.length }, "Sincronizacion de empresas de transporte completada");

    return { inserted, updated, unchanged, total: unique.length };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    try { await pool.request().query(`IF OBJECT_ID('tempdb..${stgName}') IS NOT NULL DROP TABLE ${stgName};`); } catch { /* no-op */ }
    logger.error({ message: error?.message, stack: error?.stack }, "Fallo sync-empresas-transporte");
    throw error;
  }
}

module.exports = { syncEmpresasTransporte };
