/**
 * Helpers de acceso a BD compartidos entre routes.
 * Para parsing y lógica de ciclo de vida ver utils/parse.js y utils/lifecycle.js.
 */

const { sql } = require("./db");
const { parseOptionalBigInt } = require("./utils/parse");
const { LIFECYCLE_STATUS } = require("./utils/lifecycle");

/**
 * Resuelve el id_movil para una cabecera.
 * Prioriza id_movil explícito; si no, busca por (empresa, chofer, camión) y crea si no existe.
 */
async function resolveMovilId(transaction, cabeceraIn, now, fallbackMovilId = null) {
  const explicitMovilId = parseOptionalBigInt(cabeceraIn.id_movil);
  if (explicitMovilId) return explicitMovilId;

  const idEmpresaTransporte = parseOptionalBigInt(cabeceraIn.id_empresa_transporte);
  const idChofer = parseOptionalBigInt(cabeceraIn.id_chofer);
  const idCamion = parseOptionalBigInt(cabeceraIn.id_camion);

  if (!idEmpresaTransporte || !idChofer || !idCamion) return fallbackMovilId;

  const lookup = await new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresaTransporte)
    .input("idChofer", sql.BigInt, idChofer)
    .input("idCamion", sql.BigInt, idCamion)
    .query(`
      SELECT TOP 1 IdMovil
      FROM [cfl].[Movil]
      WHERE IdEmpresaTransporte = @idEmpresa
        AND IdChofer = @idChofer
        AND IdCamion = @idCamion
      ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdMovil ASC;
    `);

  const existingMovilId = lookup.recordset[0]?.IdMovil || null;
  if (existingMovilId) return Number(existingMovilId);

  const created = await new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresaTransporte)
    .input("idChofer", sql.BigInt, idChofer)
    .input("idCamion", sql.BigInt, idCamion)
    .input("activo", sql.Bit, true)
    .input("createdAt", sql.DateTime2(0), now)
    .input("updatedAt", sql.DateTime2(0), now)
    .query(`
      INSERT INTO [cfl].[Movil] (
        [IdChofer],
        [IdEmpresaTransporte],
        [IdCamion],
        [Activo],
        [FechaCreacion],
        [FechaActualizacion]
      )
      OUTPUT INSERTED.IdMovil
      VALUES (
        @idChofer,
        @idEmpresa,
        @idCamion,
        @activo,
        @createdAt,
        @updatedAt
      );
    `);

  return Number(created.recordset[0].IdMovil);
}

function buildDomainError(message, statusCode = 422) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

/**
 * Resuelve la imputacion contable para una cabecera.
 * Estrategia incremental:
 * - Si viene IdImputacionFlete, valida consistencia y prioriza esa regla.
 * - Si viene trio (tipo+centro+cuenta), intenta mapear a una imputacion activa.
 * - Si falta centro/cuenta, intenta autocompletar cuando exista una unica opcion activa.
 * - Si no hay match, mantiene snapshot (centro/cuenta) y retorna IdImputacionFlete = null.
 */
async function resolveImputacionFlete(transaction, {
  idTipoFlete,
  idCentroCosto = null,
  idCuentaMayor = null,
  idImputacionFlete = null,
}) {
  const tipo = parseOptionalBigInt(idTipoFlete);
  const centro = parseOptionalBigInt(idCentroCosto);
  const cuenta = parseOptionalBigInt(idCuentaMayor);
  const imputacion = parseOptionalBigInt(idImputacionFlete);

  if (!tipo) {
    return {
      idTipoFlete: null,
      idCentroCosto: centro,
      idCuentaMayor: cuenta,
      idImputacionFlete: imputacion,
    };
  }

  if (imputacion) {
    const byId = await new sql.Request(transaction)
      .input("idImputacionFlete", sql.BigInt, imputacion)
      .query(`
        SELECT TOP 1
          IdImputacionFlete,
          IdTipoFlete,
          IdCentroCosto,
          IdCuentaMayor
        FROM [cfl].[ImputacionFlete]
        WHERE IdImputacionFlete = @idImputacionFlete;
      `);

    const row = byId.recordset[0] || null;
    if (!row) {
      throw buildDomainError("La imputacion seleccionada no existe", 422);
    }

    if (tipo && Number(row.IdTipoFlete) !== tipo) {
      throw buildDomainError("La imputacion no corresponde al tipo de flete seleccionado", 422);
    }
    if (centro && Number(row.IdCentroCosto) !== centro) {
      throw buildDomainError("La imputacion no corresponde al centro de costo seleccionado", 422);
    }
    if (cuenta && Number(row.IdCuentaMayor) !== cuenta) {
      throw buildDomainError("La imputacion no corresponde a la cuenta mayor seleccionada", 422);
    }

    return {
      idTipoFlete: Number(row.IdTipoFlete),
      idCentroCosto: Number(row.IdCentroCosto),
      idCuentaMayor: Number(row.IdCuentaMayor),
      idImputacionFlete: Number(row.IdImputacionFlete),
    };
  }

  if (centro && cuenta) {
    const byCombo = await new sql.Request(transaction)
      .input("idTipoFlete", sql.BigInt, tipo)
      .input("idCentroCosto", sql.BigInt, centro)
      .input("idCuentaMayor", sql.BigInt, cuenta)
      .query(`
        SELECT TOP 1
          IdImputacionFlete,
          IdTipoFlete,
          IdCentroCosto,
          IdCuentaMayor
        FROM [cfl].[ImputacionFlete]
        WHERE IdTipoFlete = @idTipoFlete
          AND IdCentroCosto = @idCentroCosto
          AND IdCuentaMayor = @idCuentaMayor
        ORDER BY CASE WHEN Activo = 1 THEN 0 ELSE 1 END, IdImputacionFlete ASC;
      `);

    const row = byCombo.recordset[0] || null;
    if (row) {
      return {
        idTipoFlete: Number(row.IdTipoFlete),
        idCentroCosto: Number(row.IdCentroCosto),
        idCuentaMayor: Number(row.IdCuentaMayor),
        idImputacionFlete: Number(row.IdImputacionFlete),
      };
    }

    return {
      idTipoFlete: tipo,
      idCentroCosto: centro,
      idCuentaMayor: cuenta,
      idImputacionFlete: null,
    };
  }

  const byType = await new sql.Request(transaction)
    .input("idTipoFlete", sql.BigInt, tipo)
    .query(`
      SELECT
        IdImputacionFlete,
        IdTipoFlete,
        IdCentroCosto,
        IdCuentaMayor
      FROM [cfl].[ImputacionFlete]
      WHERE IdTipoFlete = @idTipoFlete
        AND Activo = 1
      ORDER BY IdImputacionFlete ASC;
    `);

  const rows = byType.recordset || [];
  if (rows.length === 1) {
    const row = rows[0];
    return {
      idTipoFlete: Number(row.IdTipoFlete),
      idCentroCosto: Number(row.IdCentroCosto),
      idCuentaMayor: Number(row.IdCuentaMayor),
      idImputacionFlete: Number(row.IdImputacionFlete),
    };
  }

  if (centro) {
    const byTypeCentro = rows.filter((row) => Number(row.IdCentroCosto) === centro);
    if (byTypeCentro.length === 1) {
      const row = byTypeCentro[0];
      return {
        idTipoFlete: Number(row.IdTipoFlete),
        idCentroCosto: Number(row.IdCentroCosto),
        idCuentaMayor: Number(row.IdCuentaMayor),
        idImputacionFlete: Number(row.IdImputacionFlete),
      };
    }
  }

  if (cuenta) {
    const byTypeCuenta = rows.filter((row) => Number(row.IdCuentaMayor) === cuenta);
    if (byTypeCuenta.length === 1) {
      const row = byTypeCuenta[0];
      return {
        idTipoFlete: Number(row.IdTipoFlete),
        idCentroCosto: Number(row.IdCentroCosto),
        idCuentaMayor: Number(row.IdCuentaMayor),
        idImputacionFlete: Number(row.IdImputacionFlete),
      };
    }
  }

  return {
    idTipoFlete: tipo,
    idCentroCosto: centro,
    idCuentaMayor: cuenta,
    idImputacionFlete: null,
  };
}

module.exports = {
  buildDomainError,
  resolveMovilId,
  resolveImputacionFlete,
  // Re-exports for convenience (use direct imports from utils/ for new code)
  LIFECYCLE_STATUS,
};
