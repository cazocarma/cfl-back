const { getPool, sql } = require("../../db");
const { logger } = require("../../logger");

const CHOFER_SOURCE_PRIORITY = {
  SAP: 30,
  ROMANA: 20,
};

const CAMION_SOURCE_PRIORITY = {
  SAP: 30,
  ROMANA: 20,
};

const ROMANA_RUT_PATTERN = /\b(\d{1,2}(?:[.\s]?\d{3}){2}-?[\dkK]|\d{7,8}-?[\dkK])\b/;

function trimToNull(value, maxLen) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) return null;
  return maxLen ? trimmed.slice(0, maxLen) : trimmed;
}

function toUpperTrimmed(value, maxLen, fallback = null) {
  const trimmed = trimToNull(value, maxLen);
  return trimmed ? trimmed.toUpperCase() : fallback;
}

function normalizeAlphaNumeric(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function normalizeChoferKey(value) {
  return normalizeAlphaNumeric(value);
}

function normalizePatenteKey(value) {
  return normalizeAlphaNumeric(value);
}

function normalizeCarroKey(value) {
  return normalizeAlphaNumeric(value) || "SINCARRO";
}

function toTimestampMs(value) {
  if (!value) return 0;
  const date = value instanceof Date ? value : new Date(value);
  const ms = date.getTime();
  return Number.isFinite(ms) ? ms : 0;
}

function preferCandidate(next, current) {
  if (!current) return true;
  if ((next.priority || 0) !== (current.priority || 0)) {
    return (next.priority || 0) > (current.priority || 0);
  }
  if ((next.timestampMs || 0) !== (current.timestampMs || 0)) {
    return (next.timestampMs || 0) > (current.timestampMs || 0);
  }
  return (next.rankText || "").length > (current.rankText || "").length;
}

function extractChoferFromRomana(conductorValue) {
  const conductor = trimToNull(conductorValue, 80);
  if (!conductor) return null;

  const rutMatch = conductor.match(ROMANA_RUT_PATTERN);
  if (!rutMatch) return null;

  const sapIdFiscal = toUpperTrimmed(rutMatch[1], 20);
  if (!sapIdFiscal) return null;

  const sapNombre = trimToNull(
    conductor
      .replace(rutMatch[1], " ")
      .replace(/[|,;/()]+/g, " ")
      .replace(/\s+/g, " "),
    80,
  ) || conductor;

  return {
    sapIdFiscal,
    sapNombre,
    rankText: sapNombre,
  };
}

async function loadChoferSources(pool) {
  const result = await pool.request().query(`
    SELECT
      Origen = CAST('SAP' AS NVARCHAR(10)),
      FechaRef = COALESCE(lk.FechaExtraccion, e.FechaActualizacion, e.FechaCreacion),
      SapIdFiscal = NULLIF(LTRIM(RTRIM(lk.SapIdFiscalChofer)), ''),
      SapNombre = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), ''),
      ConductorRomana = CAST(NULL AS NVARCHAR(80))
    FROM [cfl].[SapEntrega] e
    INNER JOIN [cfl].[VW_LikpActual] lk
      ON lk.SapNumeroEntrega = e.SapNumeroEntrega
     AND lk.SistemaFuente = e.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(lk.SapIdFiscalChofer)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), '') IS NOT NULL

    UNION ALL

    SELECT
      Origen = CAST('ROMANA' AS NVARCHAR(10)),
      FechaRef = COALESCE(rc.FechaExtraccion, re.FechaActualizacion, re.FechaCreacion),
      SapIdFiscal = CAST(NULL AS NVARCHAR(20)),
      SapNombre = CAST(NULL AS NVARCHAR(80)),
      ConductorRomana = NULLIF(LTRIM(RTRIM(rc.Conductor)), '')
    FROM [cfl].[RomanaEntrega] re
    INNER JOIN [cfl].[VW_RomanaCabeceraActual] rc
      ON rc.NumeroPartida = re.NumeroPartida
     AND rc.GuiaDespacho = re.GuiaDespacho
     AND rc.SistemaFuente = re.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(rc.Conductor)), '') IS NOT NULL;
  `);

  const byKey = new Map();

  for (const row of result.recordset || []) {
    let candidate = null;

    if (row.Origen === "SAP") {
      const sapIdFiscal = toUpperTrimmed(row.SapIdFiscal, 20);
      const sapNombre = trimToNull(row.SapNombre, 80);
      if (sapIdFiscal && sapNombre) {
        candidate = {
          sapIdFiscal,
          sapNombre,
          rankText: sapNombre,
        };
      }
    } else if (row.Origen === "ROMANA") {
      candidate = extractChoferFromRomana(row.ConductorRomana);
    }

    if (!candidate?.sapIdFiscal || !candidate?.sapNombre) continue;

    const normalizedKey = normalizeChoferKey(candidate.sapIdFiscal);
    if (!normalizedKey) continue;

    const rankedCandidate = {
      ...candidate,
      priority: CHOFER_SOURCE_PRIORITY[row.Origen] || 0,
      timestampMs: toTimestampMs(row.FechaRef),
    };

    const current = byKey.get(normalizedKey);
    if (preferCandidate(rankedCandidate, current)) {
      byKey.set(normalizedKey, rankedCandidate);
    }
  }

  return [...byKey.values()].map(({ sapIdFiscal, sapNombre }) => ({
    sapIdFiscal,
    sapNombre,
  }));
}

async function resolveDefaultTipoCamionIds(pool) {
  const result = await pool.request().query(`
    SELECT
      IdTipoCamion,
      Nombre = UPPER(LTRIM(RTRIM(Nombre)))
    FROM [cfl].[TipoCamion]
    WHERE UPPER(LTRIM(RTRIM(Nombre))) IN ('PLANO SOLO', 'PLANO CON CARRO');
  `);

  const defaults = {
    planoSolo: null,
    planoConCarro: null,
  };

  for (const row of result.recordset || []) {
    if (row.Nombre === "PLANO SOLO") {
      defaults.planoSolo = Number(row.IdTipoCamion);
    }
    if (row.Nombre === "PLANO CON CARRO") {
      defaults.planoConCarro = Number(row.IdTipoCamion);
    }
  }

  if (!defaults.planoSolo || !defaults.planoConCarro) {
    throw new Error("No existen los tipos de camion base (PLANO SOLO / PLANO CON CARRO).");
  }

  return defaults;
}

async function loadCamionSources(pool, defaultTipoIds) {
  const result = await pool.request().query(`
    SELECT
      Origen = CAST('SAP' AS NVARCHAR(10)),
      FechaRef = COALESCE(lk.FechaExtraccion, e.FechaActualizacion, e.FechaCreacion),
      SapPatente = NULLIF(LTRIM(RTRIM(lk.SapPatente)), ''),
      SapCarro = NULLIF(LTRIM(RTRIM(lk.SapCarro)), '')
    FROM [cfl].[SapEntrega] e
    INNER JOIN [cfl].[VW_LikpActual] lk
      ON lk.SapNumeroEntrega = e.SapNumeroEntrega
     AND lk.SistemaFuente = e.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(lk.SapPatente)), '') IS NOT NULL

    UNION ALL

    SELECT
      Origen = CAST('ROMANA' AS NVARCHAR(10)),
      FechaRef = COALESCE(rc.FechaExtraccion, re.FechaActualizacion, re.FechaCreacion),
      SapPatente = NULLIF(LTRIM(RTRIM(rc.Patente)), ''),
      SapCarro = NULLIF(LTRIM(RTRIM(rc.Carro)), '')
    FROM [cfl].[RomanaEntrega] re
    INNER JOIN [cfl].[VW_RomanaCabeceraActual] rc
      ON rc.NumeroPartida = re.NumeroPartida
     AND rc.GuiaDespacho = re.GuiaDespacho
     AND rc.SistemaFuente = re.SistemaFuente
    WHERE NULLIF(LTRIM(RTRIM(rc.Patente)), '') IS NOT NULL;
  `);

  const byKey = new Map();

  for (const row of result.recordset || []) {
    const sapPatente = toUpperTrimmed(row.SapPatente, 20);
    if (!sapPatente) continue;

    const sapCarro = toUpperTrimmed(row.SapCarro, 20, "SIN-CARRO");
    const normalizedPatente = normalizePatenteKey(sapPatente);
    const normalizedCarro = normalizeCarroKey(sapCarro);

    if (!normalizedPatente) continue;

    const candidate = {
      idTipoCamion: normalizedCarro === "SINCARRO"
        ? defaultTipoIds.planoSolo
        : defaultTipoIds.planoConCarro,
      sapPatente,
      sapCarro,
      rankText: `${sapPatente}|${sapCarro}`,
      priority: CAMION_SOURCE_PRIORITY[row.Origen] || 0,
      timestampMs: toTimestampMs(row.FechaRef),
    };

    const key = `${normalizedPatente}|${normalizedCarro}`;
    const current = byKey.get(key);
    if (preferCandidate(candidate, current)) {
      byKey.set(key, candidate);
    }
  }

  return [...byKey.values()].map(({ idTipoCamion, sapPatente, sapCarro }) => ({
    idTipoCamion,
    sapPatente,
    sapCarro,
  }));
}

async function syncChoferes() {
  logger.info("Iniciando sincronizacion de choferes desde entregas cargadas");

  const pool = await getPool();
  const sourceRows = await loadChoferSources(pool);

  if (sourceRows.length === 0) {
    logger.info("No se encontraron choferes candidatos para sincronizar");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  const transaction = new sql.Transaction(pool);
  const stgName = `##stg_choferes_sync_${process.pid}_${Date.now()}`;

  await transaction.begin();

  try {
    await new sql.Request(transaction).query(`
      CREATE TABLE ${stgName} (
        SapIdFiscal NVARCHAR(20) NOT NULL,
        SapNombre   NVARCHAR(80) NOT NULL
      );
    `);

    const stageTable = new sql.Table(stgName);
    stageTable.create = false;
    stageTable.columns.add("SapIdFiscal", sql.NVarChar(20), { nullable: false });
    stageTable.columns.add("SapNombre", sql.NVarChar(80), { nullable: false });

    for (const row of sourceRows) {
      stageTable.rows.add(row.sapIdFiscal, row.sapNombre);
    }

    await new sql.Request(transaction).bulk(stageTable);

    const result = await new sql.Request(transaction).query(`
      CREATE TABLE #updated_choferes (IdChofer BIGINT NOT NULL);
      CREATE TABLE #inserted_choferes (IdChofer BIGINT NOT NULL);

      ;WITH src AS (
        SELECT
          SapIdFiscal,
          SapNombre,
          NormalizedId = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
        FROM ${stgName}
      ),
      tgt_ranked AS (
        SELECT
          IdChofer,
          SapIdFiscal,
          SapNombre,
          Activo,
          NormalizedId = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
          rn = ROW_NUMBER() OVER (
            PARTITION BY UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdChofer ASC
          )
        FROM [cfl].[Chofer]
      ),
      matched AS (
        SELECT
          t.IdChofer,
          s.SapNombre
        FROM src s
        INNER JOIN tgt_ranked t
          ON t.rn = 1
         AND t.NormalizedId = s.NormalizedId
        WHERE ISNULL(t.SapNombre, '') <> ISNULL(s.SapNombre, '')
           OR ISNULL(t.Activo, 0) <> 1
      )
      UPDATE tgt
         SET SapNombre = m.SapNombre,
             Activo = 1
      OUTPUT INSERTED.IdChofer INTO #updated_choferes
      FROM [cfl].[Chofer] tgt
      INNER JOIN matched m
        ON m.IdChofer = tgt.IdChofer;

      ;WITH src AS (
        SELECT
          SapIdFiscal,
          SapNombre,
          NormalizedId = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
        FROM ${stgName}
      ),
      tgt_ranked AS (
        SELECT
          IdChofer,
          NormalizedId = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
          rn = ROW_NUMBER() OVER (
            PARTITION BY UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapIdFiscal)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdChofer ASC
          )
        FROM [cfl].[Chofer]
      )
      INSERT INTO [cfl].[Chofer] (
        SapIdFiscal,
        SapNombre,
        Telefono,
        Activo
      )
      OUTPUT INSERTED.IdChofer INTO #inserted_choferes
      SELECT
        s.SapIdFiscal,
        s.SapNombre,
        NULL,
        1
      FROM src s
      WHERE NOT EXISTS (
        SELECT 1
        FROM tgt_ranked t
        WHERE t.rn = 1
          AND t.NormalizedId = s.NormalizedId
      );

      SELECT
        inserted = (SELECT COUNT_BIG(1) FROM #inserted_choferes),
        updated = (SELECT COUNT_BIG(1) FROM #updated_choferes);

      DROP TABLE #updated_choferes;
      DROP TABLE #inserted_choferes;
    `);

    await new sql.Request(transaction).query(`DROP TABLE ${stgName};`);
    await transaction.commit();

    const inserted = Number(result.recordset[0]?.inserted || 0);
    const updated = Number(result.recordset[0]?.updated || 0);
    const unchanged = sourceRows.length - inserted - updated;

    logger.info({ inserted, updated, unchanged, total: sourceRows.length }, "Sincronizacion de choferes completada");

    return { inserted, updated, unchanged, total: sourceRows.length };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    try { await pool.request().query(`IF OBJECT_ID('tempdb..${stgName}') IS NOT NULL DROP TABLE ${stgName};`); } catch { /* no-op */ }
    logger.error({ message: error?.message, stack: error?.stack }, "Fallo sync-choferes");
    throw error;
  }
}

async function syncCamiones() {
  logger.info("Iniciando sincronizacion de camiones desde entregas cargadas");

  const pool = await getPool();
  const defaultTipoIds = await resolveDefaultTipoCamionIds(pool);
  const sourceRows = await loadCamionSources(pool, defaultTipoIds);

  if (sourceRows.length === 0) {
    logger.info("No se encontraron camiones candidatos para sincronizar");
    return { inserted: 0, updated: 0, unchanged: 0, total: 0 };
  }

  const transaction = new sql.Transaction(pool);
  const stgName = `##stg_camiones_sync_${process.pid}_${Date.now()}`;

  await transaction.begin();

  try {
    await new sql.Request(transaction).query(`
      CREATE TABLE ${stgName} (
        IdTipoCamion BIGINT NOT NULL,
        SapPatente   NVARCHAR(20) NOT NULL,
        SapCarro     NVARCHAR(20) NOT NULL
      );
    `);

    const stageTable = new sql.Table(stgName);
    stageTable.create = false;
    stageTable.columns.add("IdTipoCamion", sql.BigInt, { nullable: false });
    stageTable.columns.add("SapPatente", sql.NVarChar(20), { nullable: false });
    stageTable.columns.add("SapCarro", sql.NVarChar(20), { nullable: false });

    for (const row of sourceRows) {
      stageTable.rows.add(row.idTipoCamion, row.sapPatente, row.sapCarro);
    }

    await new sql.Request(transaction).bulk(stageTable);

    const now = new Date();
    const result = await new sql.Request(transaction)
      .input("now", sql.DateTime2(0), now)
      .query(`
        CREATE TABLE #updated_camiones (IdCamion BIGINT NOT NULL);
        CREATE TABLE #inserted_camiones (IdCamion BIGINT NOT NULL);

        ;WITH src AS (
          SELECT
            IdTipoCamion,
            SapPatente,
            SapCarro,
            NormalizedPatente = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
            NormalizedCarro = CASE
              WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                THEN 'SINCARRO'
              ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            END
          FROM ${stgName}
        ),
        tgt_ranked AS (
          SELECT
            IdCamion,
            IdTipoCamion,
            SapPatente,
            SapCarro,
            Activo,
            NormalizedPatente = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
            NormalizedCarro = CASE
              WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                THEN 'SINCARRO'
              ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            END,
            rn = ROW_NUMBER() OVER (
              PARTITION BY
                UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
                CASE
                  WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                    THEN 'SINCARRO'
                  ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
                END
              ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdCamion ASC
            )
          FROM [cfl].[Camion]
        ),
        matched AS (
          SELECT
            t.IdCamion,
            s.IdTipoCamion
          FROM src s
          INNER JOIN tgt_ranked t
            ON t.rn = 1
           AND t.NormalizedPatente = s.NormalizedPatente
           AND t.NormalizedCarro = s.NormalizedCarro
          WHERE ISNULL(t.IdTipoCamion, 0) <> ISNULL(s.IdTipoCamion, 0)
             OR ISNULL(t.Activo, 0) <> 1
        )
        UPDATE tgt
           SET IdTipoCamion = m.IdTipoCamion,
               Activo = 1,
               FechaActualizacion = @now
        OUTPUT INSERTED.IdCamion INTO #updated_camiones
        FROM [cfl].[Camion] tgt
        INNER JOIN matched m
          ON m.IdCamion = tgt.IdCamion;

        ;WITH src AS (
          SELECT
            IdTipoCamion,
            SapPatente,
            SapCarro,
            NormalizedPatente = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
            NormalizedCarro = CASE
              WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                THEN 'SINCARRO'
              ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            END
          FROM ${stgName}
        ),
        tgt_ranked AS (
          SELECT
            IdCamion,
            NormalizedPatente = UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
            NormalizedCarro = CASE
              WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                THEN 'SINCARRO'
              ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
            END,
            rn = ROW_NUMBER() OVER (
              PARTITION BY
                UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')),
                CASE
                  WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
                    THEN 'SINCARRO'
                  ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
                END
              ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdCamion ASC
            )
          FROM [cfl].[Camion]
        )
        INSERT INTO [cfl].[Camion] (
          IdTipoCamion,
          SapPatente,
          SapCarro,
          Activo,
          FechaCreacion,
          FechaActualizacion
        )
        OUTPUT INSERTED.IdCamion INTO #inserted_camiones
        SELECT
          s.IdTipoCamion,
          s.SapPatente,
          s.SapCarro,
          1,
          @now,
          @now
        FROM src s
        WHERE NOT EXISTS (
          SELECT 1
          FROM tgt_ranked t
          WHERE t.rn = 1
            AND t.NormalizedPatente = s.NormalizedPatente
            AND t.NormalizedCarro = s.NormalizedCarro
        );

        SELECT
          inserted = (SELECT COUNT_BIG(1) FROM #inserted_camiones),
          updated = (SELECT COUNT_BIG(1) FROM #updated_camiones);

        DROP TABLE #updated_camiones;
        DROP TABLE #inserted_camiones;
      `);

    await new sql.Request(transaction).query(`DROP TABLE ${stgName};`);
    await transaction.commit();

    const inserted = Number(result.recordset[0]?.inserted || 0);
    const updated = Number(result.recordset[0]?.updated || 0);
    const unchanged = sourceRows.length - inserted - updated;

    logger.info({ inserted, updated, unchanged, total: sourceRows.length }, "Sincronizacion de camiones completada");

    return { inserted, updated, unchanged, total: sourceRows.length };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    try { await pool.request().query(`IF OBJECT_ID('tempdb..${stgName}') IS NOT NULL DROP TABLE ${stgName};`); } catch { /* no-op */ }
    logger.error({ message: error?.message, stack: error?.stack }, "Fallo sync-camiones");
    throw error;
  }
}

module.exports = {
  syncChoferes,
  syncCamiones,
};
