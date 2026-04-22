const { sql } = require("../../db");
const { buildDomainError } = require("../../helpers");
const { normalizePatente, normalizeCarro, trimOrNull, isUniqueViolation } = require("./normalize");

const NORM_PATENTE_SQL = "UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapPatente)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))";
const NORM_CARRO_SQL = `
  CASE
    WHEN NULLIF(UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), '')), '') IS NULL
      THEN 'SINCARRO'
    ELSE UPPER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(SapCarro)), '.', ''), '-', ''), ' ', ''), CHAR(9), ''), CHAR(10), ''))
  END
`;

async function findCamion(transaction, { sap_patente, sap_carro }) {
  const normP = normalizePatente(sap_patente);
  const normC = normalizeCarro(sap_carro);
  if (!normP) return null;
  const result = await new sql.Request(transaction)
    .input("normP", sql.NVarChar(20), normP)
    .input("normC", sql.NVarChar(20), normC)
    .query(`
      SELECT TOP 1 IdCamion, IdTipoCamion, SapPatente, SapCarro, Activo
      FROM [cfl].[Camion]
      WHERE ${NORM_PATENTE_SQL} = @normP
        AND ${NORM_CARRO_SQL} = @normC
      ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdCamion ASC;
    `);
  return result.recordset[0] || null;
}

async function insertCamion(transaction, data, now) {
  const sapPatente = trimOrNull(data.sap_patente, 20);
  const sapCarro = trimOrNull(data.sap_carro, 20) || "SIN-CARRO";
  const req = new sql.Request(transaction)
    .input("idTipoCamion", sql.BigInt, Number(data.id_tipo_camion))
    .input("sapPatente", sql.NVarChar(20), sapPatente)
    .input("sapCarro", sql.NVarChar(20), sapCarro)
    .input("activo", sql.Bit, data.activo === false ? 0 : 1)
    .input("now", sql.DateTime2(0), now);
  const result = await req.query(`
    INSERT INTO [cfl].[Camion] (IdTipoCamion, SapPatente, SapCarro, Activo, FechaCreacion, FechaActualizacion)
    OUTPUT INSERTED.IdCamion
    VALUES (@idTipoCamion, @sapPatente, @sapCarro, @activo, @now, @now);
  `);
  return Number(result.recordset[0].IdCamion);
}

async function updateCamionFields(transaction, idCamion, fields, now, opts = {}) {
  const { allowKeyFields = false } = opts;
  const sets = [];
  const req = new sql.Request(transaction)
    .input("idCamion", sql.BigInt, idCamion)
    .input("now", sql.DateTime2(0), now);
  // sap_patente + sap_carro son las claves de matching con VW_LikpActual /
  // VW_RomanaCabeceraActual. Solo se tocan en merge defensivo inicial.
  if (allowKeyFields && Object.prototype.hasOwnProperty.call(fields, "sap_patente")) {
    req.input("sapPatente", sql.NVarChar(20), trimOrNull(fields.sap_patente, 20));
    sets.push("SapPatente = @sapPatente");
  }
  if (allowKeyFields && Object.prototype.hasOwnProperty.call(fields, "sap_carro")) {
    req.input("sapCarro", sql.NVarChar(20), trimOrNull(fields.sap_carro, 20) || "SIN-CARRO");
    sets.push("SapCarro = @sapCarro");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "id_tipo_camion") && fields.id_tipo_camion) {
    req.input("idTipoCamion", sql.BigInt, Number(fields.id_tipo_camion));
    sets.push("IdTipoCamion = @idTipoCamion");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "activo")) {
    req.input("activo", sql.Bit, fields.activo === false ? 0 : 1);
    sets.push("Activo = @activo");
  }
  if (sets.length === 0) return;
  sets.push("FechaActualizacion = @now");
  await req.query(`
    UPDATE [cfl].[Camion]
    SET ${sets.join(", ")}
    WHERE IdCamion = @idCamion;
  `);
}

async function getCamionTipo(transaction, idCamion) {
  const result = await new sql.Request(transaction)
    .input("idCamion", sql.BigInt, idCamion)
    .query(`SELECT TOP 1 IdTipoCamion FROM [cfl].[Camion] WHERE IdCamion = @idCamion;`);
  return result.recordset[0] ? Number(result.recordset[0].IdTipoCamion) : null;
}

// Aplica el cambio de tipo de camion (update_tipo_camion). Retorna
// `true` si realmente cambio (el IdTipoCamion nuevo difiere del actual).
async function applyTipoCamionChange(transaction, change, now) {
  if (!change || !change.id_camion || !change.to_id_tipo_camion) return false;
  const idCamion = Number(change.id_camion);
  const toTipo = Number(change.to_id_tipo_camion);
  const currentTipo = await getCamionTipo(transaction, idCamion);
  if (currentTipo === toTipo) return false;
  await updateCamionFields(transaction, idCamion, { id_tipo_camion: toTipo }, now);
  return true;
}

async function resolveCamion(transaction, intent, now) {
  let tipoCamionChanged = false;
  let resolvedIdCamion = null;

  if (!intent || intent.mode === "empty") {
    // Aun asi podria venir update_tipo_camion aislado (camion preseleccionado + cambio de tipo).
    if (intent?.update_tipo_camion) {
      tipoCamionChanged = await applyTipoCamionChange(transaction, intent.update_tipo_camion, now);
      resolvedIdCamion = Number(intent.update_tipo_camion.id_camion);
    }
    return { idCamion: resolvedIdCamion, tipoCamionChanged };
  }

  if (intent.mode === "matched") {
    if (intent.update_tipo_camion) {
      tipoCamionChanged = await applyTipoCamionChange(transaction, intent.update_tipo_camion, now);
      resolvedIdCamion = Number(intent.update_tipo_camion.id_camion);
    }
    return { idCamion: resolvedIdCamion, tipoCamionChanged };
  }

  if (intent.mode === "update") {
    const update = intent.update;
    if (!update?.id_camion) {
      throw buildDomainError("transport.camion.update requiere id_camion", 400);
    }
    const idCamion = Number(update.id_camion);
    await updateCamionFields(transaction, idCamion, update.fields || {}, now);
    // si el update incluyo id_tipo_camion lo marcamos como cambio para forzar recalc
    if (Object.prototype.hasOwnProperty.call(update.fields || {}, "id_tipo_camion")) {
      tipoCamionChanged = true;
    }
    // tambien puede venir update_tipo_camion explicito
    if (intent.update_tipo_camion) {
      const changed = await applyTipoCamionChange(transaction, intent.update_tipo_camion, now);
      tipoCamionChanged = tipoCamionChanged || changed;
    }
    return { idCamion, tipoCamionChanged };
  }

  if (intent.mode === "pending_create") {
    const draft = intent.pending_create;
    if (!draft || !trimOrNull(draft.sap_patente) || !draft.id_tipo_camion) {
      throw buildDomainError("transport.camion.pending_create requiere sap_patente e id_tipo_camion", 400);
    }
    const existing = await findCamion(transaction, draft);
    if (existing) {
      // Ya existe un camion con esa patente+carro. Reutilizar.
      // Si el tipo difiere y el caller quiere actualizarlo, tendria que haber
      // mandado update_tipo_camion; aqui NO cambiamos el tipo (evita side effect
      // no confirmado).
      return { idCamion: Number(existing.IdCamion), tipoCamionChanged: false };
    }
    try {
      const idCamion = await insertCamion(transaction, draft, now);
      return { idCamion, tipoCamionChanged: false };
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      const retry = await findCamion(transaction, draft);
      if (retry) {
        return { idCamion: Number(retry.IdCamion), tipoCamionChanged: false };
      }
      throw error;
    }
  }

  return { idCamion: resolvedIdCamion, tipoCamionChanged };
}

module.exports = { resolveCamion, getCamionTipo };
