const { sql } = require("../../db");
const { buildDomainError } = require("../../helpers");
const { normalizeRut, trimOrNull, isUniqueViolation } = require("./normalize");

// Buscar empresa por SapCodigo (preferido) o por Rut normalizado.
async function findEmpresa(transaction, { sap_codigo, rut }) {
  if (sap_codigo) {
    const byCode = await new sql.Request(transaction)
      .input("code", sql.NVarChar(10), trimOrNull(sap_codigo, 10))
      .query(`
        SELECT TOP 1 IdEmpresa, SapCodigo, Rut, RazonSocial, NombreRepresentante, Correo, Telefono, Activo
        FROM [cfl].[EmpresaTransporte]
        WHERE SapCodigo = @code;
      `);
    if (byCode.recordset.length > 0) return byCode.recordset[0];
  }
  if (rut) {
    const norm = normalizeRut(rut);
    if (!norm) return null;
    const byRut = await new sql.Request(transaction)
      .input("rutNorm", sql.NVarChar(20), norm)
      .query(`
        SELECT TOP 1 IdEmpresa, SapCodigo, Rut, RazonSocial, NombreRepresentante, Correo, Telefono, Activo
        FROM [cfl].[EmpresaTransporte]
        WHERE UPPER(REPLACE(REPLACE(REPLACE(ISNULL(Rut, ''), '.', ''), '-', ''), ' ', '')) = @rutNorm;
      `);
    if (byRut.recordset.length > 0) return byRut.recordset[0];
  }
  return null;
}

async function insertEmpresa(transaction, data, now) {
  const req = new sql.Request(transaction);
  req.input("sapCodigo", sql.NVarChar(10), trimOrNull(data.sap_codigo, 10));
  req.input("rut", sql.NVarChar(20), trimOrNull(data.rut, 20));
  req.input("razonSocial", sql.NVarChar(100), trimOrNull(data.razon_social, 100));
  req.input("nombreRep", sql.NVarChar(100), trimOrNull(data.nombre_representante, 100));
  req.input("correo", sql.NVarChar(100), trimOrNull(data.correo, 100));
  req.input("telefono", sql.NVarChar(20), trimOrNull(data.telefono, 20));
  req.input("activo", sql.Bit, data.activo === false ? 0 : 1);
  req.input("now", sql.DateTime2(0), now);
  const result = await req.query(`
    INSERT INTO [cfl].[EmpresaTransporte] (
      SapCodigo, Rut, RazonSocial, NombreRepresentante, Correo, Telefono, Activo,
      FechaCreacion, FechaActualizacion
    )
    OUTPUT INSERTED.IdEmpresa
    VALUES (
      @sapCodigo, @rut, @razonSocial, @nombreRep, @correo, @telefono, @activo,
      @now, @now
    );
  `);
  return Number(result.recordset[0].IdEmpresa);
}

async function updateEmpresaFields(transaction, idEmpresa, fields, now, opts = {}) {
  const { allowKeyFields = false } = opts;
  const sets = [];
  const req = new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresa)
    .input("now", sql.DateTime2(0), now);
  // sap_codigo es el enlace con SAP YWT_CDTB24; solo se permite tocar cuando
  // allowKeyFields=true (merge defensivo inicial desde pending_create). En un
  // UPDATE disparado por el user se ignora para evitar romper el matching con
  // snapshots futuros.
  if (allowKeyFields && Object.prototype.hasOwnProperty.call(fields, "sap_codigo")) {
    req.input("sapCodigo", sql.NVarChar(10), trimOrNull(fields.sap_codigo, 10));
    sets.push("SapCodigo = @sapCodigo");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "rut")) {
    req.input("rut", sql.NVarChar(20), trimOrNull(fields.rut, 20));
    sets.push("Rut = @rut");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "razon_social")) {
    req.input("razonSocial", sql.NVarChar(100), trimOrNull(fields.razon_social, 100));
    sets.push("RazonSocial = @razonSocial");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "nombre_representante")) {
    req.input("nombreRep", sql.NVarChar(100), trimOrNull(fields.nombre_representante, 100));
    sets.push("NombreRepresentante = @nombreRep");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "correo")) {
    req.input("correo", sql.NVarChar(100), trimOrNull(fields.correo, 100));
    sets.push("Correo = @correo");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "telefono")) {
    req.input("telefono", sql.NVarChar(20), trimOrNull(fields.telefono, 20));
    sets.push("Telefono = @telefono");
  }
  if (Object.prototype.hasOwnProperty.call(fields, "activo")) {
    req.input("activo", sql.Bit, fields.activo === false ? 0 : 1);
    sets.push("Activo = @activo");
  }
  if (sets.length === 0) return; // nada que actualizar
  sets.push("FechaActualizacion = @now");
  await req.query(`
    UPDATE [cfl].[EmpresaTransporte]
    SET ${sets.join(", ")}
    WHERE IdEmpresa = @idEmpresa;
  `);
}

// Rellena campos del registro existente que esten vacios con datos del draft.
// Se aplica despues de un find exitoso en mode=pending_create (merge defensivo).
async function mergeEmptyEmpresa(transaction, existing, draft, now) {
  const fillable = {};
  if (!trimOrNull(existing.SapCodigo) && trimOrNull(draft.sap_codigo)) fillable.sap_codigo = draft.sap_codigo;
  if (!trimOrNull(existing.Rut) && trimOrNull(draft.rut)) fillable.rut = draft.rut;
  if (!trimOrNull(existing.RazonSocial) && trimOrNull(draft.razon_social)) fillable.razon_social = draft.razon_social;
  if (!trimOrNull(existing.NombreRepresentante) && trimOrNull(draft.nombre_representante)) fillable.nombre_representante = draft.nombre_representante;
  if (!trimOrNull(existing.Correo) && trimOrNull(draft.correo)) fillable.correo = draft.correo;
  if (!trimOrNull(existing.Telefono) && trimOrNull(draft.telefono)) fillable.telefono = draft.telefono;
  if (Object.keys(fillable).length === 0) return;
  await updateEmpresaFields(transaction, Number(existing.IdEmpresa), fillable, now, { allowKeyFields: true });
}

async function resolveEmpresa(transaction, intent, now) {
  if (!intent || intent.mode === "empty") return null;

  if (intent.mode === "matched") {
    return null; // el caller usa el id original de cabeceraIn.id_empresa_transporte
  }

  if (intent.mode === "update") {
    const update = intent.update;
    if (!update?.id_empresa_transporte) {
      throw buildDomainError("transport.empresa.update requiere id_empresa_transporte", 400);
    }
    await updateEmpresaFields(transaction, Number(update.id_empresa_transporte), update.fields || {}, now);
    return Number(update.id_empresa_transporte);
  }

  if (intent.mode === "pending_create") {
    const draft = intent.pending_create;
    if (!draft || !trimOrNull(draft.rut)) {
      throw buildDomainError("transport.empresa.pending_create requiere rut", 400);
    }
    // Defensa contra race: intentar find primero.
    const existing = await findEmpresa(transaction, draft);
    if (existing) {
      await mergeEmptyEmpresa(transaction, existing, draft, now);
      return Number(existing.IdEmpresa);
    }
    try {
      return await insertEmpresa(transaction, draft, now);
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      // Otra sesion gano el INSERT entre nuestro find y nuestro insert.
      // Retry find y merge.
      const retry = await findEmpresa(transaction, draft);
      if (retry) {
        await mergeEmptyEmpresa(transaction, retry, draft, now);
        return Number(retry.IdEmpresa);
      }
      throw error;
    }
  }

  return null;
}

module.exports = { resolveEmpresa };
