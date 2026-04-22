const { sql } = require("../../db");
const { buildDomainError } = require("../../helpers");
const { normalizeRut, trimOrNull, isUniqueViolation } = require("./normalize");

// Match por SapIdFiscalNorm (columna computed persistida en cfl.Chofer).
async function findChoferByNorm(transaction, sapIdFiscal) {
  const norm = normalizeRut(sapIdFiscal);
  if (!norm) return null;
  const result = await new sql.Request(transaction)
    .input("norm", sql.NVarChar(24), norm)
    .query(`
      SELECT TOP 1 IdChofer, SapIdFiscal, SapNombre, Telefono, Activo
      FROM [cfl].[Chofer]
      WHERE SapIdFiscalNorm = @norm
      ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdChofer ASC;
    `);
  return result.recordset[0] || null;
}

async function insertChofer(transaction, data) {
  const req = new sql.Request(transaction);
  req.input("sapIdFiscal", sql.NVarChar(24), trimOrNull(data.sap_id_fiscal, 24));
  req.input("sapNombre", sql.NVarChar(80), trimOrNull(data.sap_nombre, 80));
  req.input("telefono", sql.NVarChar(30), trimOrNull(data.telefono, 30));
  req.input("activo", sql.Bit, data.activo === false ? 0 : 1);
  const result = await req.query(`
    INSERT INTO [cfl].[Chofer] (SapIdFiscal, SapNombre, Telefono, Activo)
    OUTPUT INSERTED.IdChofer
    VALUES (@sapIdFiscal, @sapNombre, @telefono, @activo);
  `);
  return Number(result.recordset[0].IdChofer);
}

async function updateChoferFields(transaction, idChofer, fields, opts = {}) {
  const { allowKeyFields = false } = opts;
  const sets = [];
  const req = new sql.Request(transaction).input("idChofer", sql.BigInt, idChofer);
  // sap_id_fiscal es el enlace con YWTRM_TB_CONDUCT y el campo usado para matchear
  // conductores en VW_LikpActual/Romana. Solo se toca en merge defensivo inicial.
  if (allowKeyFields && Object.prototype.hasOwnProperty.call(fields, "sap_id_fiscal")) {
    req.input("sapIdFiscal", sql.NVarChar(24), trimOrNull(fields.sap_id_fiscal, 24));
    sets.push("SapIdFiscal = @sapIdFiscal");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "sap_nombre")) {
    req.input("sapNombre", sql.NVarChar(80), trimOrNull(fields.sap_nombre, 80));
    sets.push("SapNombre = @sapNombre");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "telefono")) {
    req.input("telefono", sql.NVarChar(30), trimOrNull(fields.telefono, 30));
    sets.push("Telefono = @telefono");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "activo")) {
    req.input("activo", sql.Bit, fields.activo === false ? 0 : 1);
    sets.push("Activo = @activo");
  }
  if (sets.length === 0) return;
  await req.query(`
    UPDATE [cfl].[Chofer]
    SET ${sets.join(", ")}
    WHERE IdChofer = @idChofer;
  `);
}

async function mergeEmptyChofer(transaction, existing, draft) {
  const fillable = {};
  if (!trimOrNull(existing.Telefono) && trimOrNull(draft.telefono)) fillable.telefono = draft.telefono;
  // SapNombre puede mejorar si el draft tiene mas info (nombre mas largo)
  const draftNombre = trimOrNull(draft.sap_nombre) || "";
  const existingNombre = trimOrNull(existing.SapNombre) || "";
  if (draftNombre.length > existingNombre.length) fillable.sap_nombre = draft.sap_nombre;
  if (Object.keys(fillable).length === 0) return;
  await updateChoferFields(transaction, Number(existing.IdChofer), fillable, { allowKeyFields: true });
}

async function resolveChofer(transaction, intent) {
  if (!intent || intent.mode === "empty") return null;

  if (intent.mode === "matched") return null;

  if (intent.mode === "update") {
    const update = intent.update;
    if (!update?.id_chofer) {
      throw buildDomainError("transport.chofer.update requiere id_chofer", 400);
    }
    await updateChoferFields(transaction, Number(update.id_chofer), update.fields || {});
    return Number(update.id_chofer);
  }

  if (intent.mode === "pending_create") {
    const draft = intent.pending_create;
    if (!draft || !trimOrNull(draft.sap_id_fiscal) || !trimOrNull(draft.sap_nombre)) {
      throw buildDomainError("transport.chofer.pending_create requiere sap_id_fiscal y sap_nombre", 400);
    }
    const existing = await findChoferByNorm(transaction, draft.sap_id_fiscal);
    if (existing) {
      await mergeEmptyChofer(transaction, existing, draft);
      return Number(existing.IdChofer);
    }
    try {
      return await insertChofer(transaction, draft);
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      const retry = await findChoferByNorm(transaction, draft.sap_id_fiscal);
      if (retry) {
        await mergeEmptyChofer(transaction, retry, draft);
        return Number(retry.IdChofer);
      }
      throw error;
    }
  }

  return null;
}

module.exports = { resolveChofer };
