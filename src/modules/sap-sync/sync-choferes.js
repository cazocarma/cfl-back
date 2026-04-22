const { getPool, sql } = require("../../db");
const { logger } = require("../../logger");
const { queryRfc } = require("./sap-query");

// ────────────────────────────────────────────────────────────────────
// Sync de choferes consolidado desde tres fuentes, con dedup por
// SapIdFiscalNorm (forma normalizada del RUT sin puntuacion) y
// prioridad explicita:
//
//   RFC YWTRM_TB_CONDUCT (30)   ← fuente canonica SAP maestro
//   VW_LikpActual        (20)   ← extraido de fletes SAP
//   VW_RomanaCabeceraActual (10) ← parseado del texto Conductor romana
//
// La prioridad solo afecta que literal/nombre/telefono se conserva al
// agregar al staging: para un mismo chofer logico (mismo Norm), la
// fuente de mayor prioridad define los valores.
//
// El MERGE matchea por Norm y PRESERVA el literal SapIdFiscal que ya
// exista en cfl.Chofer. Esto evita colisiones UNIQUE cuando historico
// tenga variantes del mismo RUT con distinto formato.
// ────────────────────────────────────────────────────────────────────

const RFC_DESTINATION = "PRD";
const RFC_TABLE = "YWTRM_TB_CONDUCT";
const RFC_FIELDS = ["CONDUCTOR", "NAME_CONDUCTOR", "FONO_CONDUCTOR"];

const PRIORITY = {
  RFC: 30,
  LIKP: 20,
  ROMANA: 10,
};

// Regex para extraer un RUT chileno del texto libre del conductor de
// romana (ej: "12.345.678-9 | JUAN PEREZ").
const ROMANA_RUT_PATTERN = /\b(\d{1,2}(?:[.\s]?\d{3}){2}-?[\dkK]|\d{7,8}-?[\dkK])\b/;

function trim(value, maxLen) {
  const s = String(value ?? "").trim();
  return maxLen ? s.slice(0, maxLen) : s;
}

// Normalizacion alineada con la columna computed SapIdFiscalNorm en
// cfl.Chofer (UPPER + REPLACE de '.', '-', ' ', CHAR(9), CHAR(10)).
function normalizeRut(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[.\- \t\n]/g, "");
}

function preferCandidate(next, current) {
  if (!current) return true;
  if (next.priority !== current.priority) return next.priority > current.priority;
  // Tiebreak: nombre mas largo (mas informacion) gana.
  return (next.sapNombre?.length || 0) > (current.sapNombre?.length || 0);
}

function addCandidate(byNorm, candidate) {
  const norm = normalizeRut(candidate.sapIdFiscal);
  if (!norm) return;
  const current = byNorm.get(norm);
  if (preferCandidate(candidate, current)) {
    byNorm.set(norm, { ...candidate, norm });
  }
}

function parseChoferFromRomanaTexto(conductorRaw) {
  const s = trim(conductorRaw);
  if (!s) return null;
  const m = s.match(ROMANA_RUT_PATTERN);
  if (!m) return null;

  const sapIdFiscal = trim(m[1], 24);
  if (!sapIdFiscal) return null;

  const sapNombre = trim(
    s.replace(m[1], " ")
     .replace(/[|,;/()]+/g, " ")
     .replace(/\s+/g, " "),
    80,
  ) || s;

  return { sapIdFiscal, sapNombre, telefono: null };
}

// ────────────────────────────────────────────────────────────────────
// Carga de candidatos por fuente
// ────────────────────────────────────────────────────────────────────

async function loadFromRfc() {
  const records = await queryRfc(RFC_DESTINATION, RFC_TABLE, RFC_FIELDS, "", 0);
  return (records || [])
    .map((r) => {
      const sapIdFiscal = trim(r.CONDUCTOR, 24);
      if (!sapIdFiscal) return null;
      const sapNombre = trim(r.NAME_CONDUCTOR, 80);
      if (!sapNombre) return null;
      return {
        sapIdFiscal,
        sapNombre,
        telefono: trim(r.FONO_CONDUCTOR, 30) || null,
        priority: PRIORITY.RFC,
      };
    })
    .filter(Boolean);
}

async function loadFromLikp(pool) {
  const result = await pool.request().query(`
    SELECT
      SapIdFiscal = NULLIF(LTRIM(RTRIM(lk.SapIdFiscalChofer)), ''),
      SapNombre   = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), '')
    FROM [cfl].[SapEntrega] e
    INNER JOIN [cfl].[VW_LikpActual] lk
      ON lk.SapNumeroEntrega = e.SapNumeroEntrega
     AND lk.SistemaFuente = e.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(lk.SapIdFiscalChofer)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), '') IS NOT NULL;
  `);
  return (result.recordset || [])
    .map((row) => ({
      sapIdFiscal: trim(row.SapIdFiscal, 24),
      sapNombre: trim(row.SapNombre, 80),
      telefono: null,
      priority: PRIORITY.LIKP,
    }))
    .filter((c) => c.sapIdFiscal && c.sapNombre);
}

async function loadFromRomana(pool) {
  const result = await pool.request().query(`
    SELECT Conductor = NULLIF(LTRIM(RTRIM(rc.Conductor)), '')
    FROM [cfl].[RomanaEntrega] re
    INNER JOIN [cfl].[VW_RomanaCabeceraActual] rc
      ON rc.NumeroPartida = re.NumeroPartida
     AND rc.GuiaDespacho = re.GuiaDespacho
     AND rc.SistemaFuente = re.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(rc.Conductor)), '') IS NOT NULL;
  `);
  return (result.recordset || [])
    .map((row) => {
      const parsed = parseChoferFromRomanaTexto(row.Conductor);
      if (!parsed) return null;
      return { ...parsed, priority: PRIORITY.ROMANA };
    })
    .filter(Boolean);
}

// ────────────────────────────────────────────────────────────────────
// Orquestacion
// ────────────────────────────────────────────────────────────────────

async function syncChoferes() {
  logger.info("Iniciando sincronizacion de choferes desde RFC + LIKP + Romana");

  const pool = await getPool();

  const [rfcRows, likpRows, romanaRows] = await Promise.all([
    loadFromRfc(),
    loadFromLikp(pool),
    loadFromRomana(pool),
  ]);

  const byNorm = new Map();
  for (const c of rfcRows) addCandidate(byNorm, c);
  for (const c of likpRows) addCandidate(byNorm, c);
  for (const c of romanaRows) addCandidate(byNorm, c);

  logger.info(
    {
      rfc: rfcRows.length,
      likp: likpRows.length,
      romana: romanaRows.length,
      unicos: byNorm.size,
    },
    "Candidatos agregados por fuente",
  );

  if (byNorm.size === 0) {
    logger.info("No se encontraron choferes candidatos en ninguna fuente");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  const unique = [...byNorm.values()];

  const transaction = new sql.Transaction(pool);
  const stgName = `##stg_choferes_sync_${process.pid}_${Date.now()}`;

  await transaction.begin();

  try {
    await new sql.Request(transaction).query(`
      CREATE TABLE ${stgName} (
        SapIdFiscal NVARCHAR(24) NOT NULL,
        SapNombre   NVARCHAR(80) NOT NULL,
        Telefono    NVARCHAR(30) NULL,
        Norm        NVARCHAR(24) NOT NULL
      );
    `);

    const stage = new sql.Table(stgName);
    stage.create = false;
    stage.columns.add("SapIdFiscal", sql.NVarChar(24), { nullable: false });
    stage.columns.add("SapNombre", sql.NVarChar(80), { nullable: false });
    stage.columns.add("Telefono", sql.NVarChar(30), { nullable: true });
    stage.columns.add("Norm", sql.NVarChar(24), { nullable: false });

    for (const r of unique) {
      stage.rows.add(r.sapIdFiscal, r.sapNombre, r.telefono, r.norm);
    }

    await new sql.Request(transaction).bulk(stage);

    // Match por Norm. Preserva literal existente en cfl.Chofer: si
    // hubiera filas duplicadas historicas con mismo Norm y distinto
    // literal, actualizamos solo la primera (por IdChofer asc). El
    // saneamiento de duplicados queda fuera del sync.
    const result = await new sql.Request(transaction).query(`
      CREATE TABLE #updated_choferes (IdChofer BIGINT NOT NULL);
      CREATE TABLE #inserted_choferes (IdChofer BIGINT NOT NULL);

      ;WITH tgt_ranked AS (
        SELECT IdChofer, SapIdFiscalNorm,
               rn = ROW_NUMBER() OVER (PARTITION BY SapIdFiscalNorm ORDER BY IdChofer ASC)
        FROM cfl.Chofer
      )
      UPDATE tgt
         SET SapNombre = s.SapNombre,
             Telefono  = ISNULL(NULLIF(s.Telefono, ''), tgt.Telefono),
             Activo    = 1
      OUTPUT INSERTED.IdChofer INTO #updated_choferes
      FROM cfl.Chofer tgt
      INNER JOIN tgt_ranked r ON r.IdChofer = tgt.IdChofer AND r.rn = 1
      INNER JOIN ${stgName} s ON s.Norm = tgt.SapIdFiscalNorm
      WHERE ISNULL(tgt.SapNombre, '') <> ISNULL(s.SapNombre, '')
         OR ISNULL(tgt.Telefono, '') <> ISNULL(NULLIF(s.Telefono, ''), ISNULL(tgt.Telefono, ''))
         OR ISNULL(tgt.Activo, 0) <> 1;

      INSERT INTO cfl.Chofer (SapIdFiscal, SapNombre, Telefono, Activo)
      OUTPUT INSERTED.IdChofer INTO #inserted_choferes
      SELECT s.SapIdFiscal, s.SapNombre, NULLIF(s.Telefono, ''), 1
      FROM ${stgName} s
      WHERE NOT EXISTS (
        SELECT 1 FROM cfl.Chofer t
        WHERE t.SapIdFiscalNorm = s.Norm
      );

      SELECT
        inserted = (SELECT COUNT_BIG(1) FROM #inserted_choferes),
        updated  = (SELECT COUNT_BIG(1) FROM #updated_choferes);

      DROP TABLE #inserted_choferes;
      DROP TABLE #updated_choferes;
    `);

    await new sql.Request(transaction).query(`DROP TABLE ${stgName};`);
    await transaction.commit();

    const inserted = Number(result.recordset[0]?.inserted || 0);
    const updated = Number(result.recordset[0]?.updated || 0);
    const unchanged = unique.length - inserted - updated;

    logger.info(
      { inserted, updated, unchanged, total: unique.length },
      "Sincronizacion de choferes completada",
    );
    return { inserted, updated, unchanged, total: unique.length };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    try { await pool.request().query(`IF OBJECT_ID('tempdb..${stgName}') IS NOT NULL DROP TABLE ${stgName};`); } catch { /* no-op */ }
    logger.error(
      { message: error?.message, stack: error?.stack },
      "Fallo sync-choferes",
    );
    throw error;
  }
}

module.exports = { syncChoferes };
