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

    // tipo + centro + cuenta están todos definidos pero no existe una
    // imputación activa que los combine. No se permite persistir el flete
    // con `idImputacionFlete: null` porque rompe la planilla SAP.
    throw buildDomainError(
      "No existe una imputación configurada para la combinación tipo flete + centro costo + cuenta mayor. Revisa el mantenedor de Imputaciones.",
      422,
    );
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

  // No se pudo resolver una imputación única para los inputs dados
  // (tipo flete solo, o tipo + centro/cuenta parcial con múltiples matches).
  // En vez de retornar silenciosamente un flete "sin imputación", fallamos
  // aquí para que el caller pida los IDs exactos (o los mantenedores se
  // completen).
  throw buildDomainError(
    "No se pudo resolver una imputación única para el flete. Ingresa centro de costo y cuenta mayor específicos, o completa el mantenedor de Imputaciones.",
    422,
  );
}

/**
 * Retorna el IdTemporada de la única temporada activa. Falla con 422 si no existe
 * ninguna. La unicidad está garantizada por índice filtrado en BD.
 */
async function getIdTemporadaActiva(transaction) {
  const result = await new sql.Request(transaction).query(`
    SELECT TOP 1 IdTemporada
    FROM [cfl].[Temporada]
    WHERE Activa = 1;
  `);
  const idTemporada = result.recordset[0]?.IdTemporada;
  if (!idTemporada) {
    throw buildDomainError(
      "No hay temporada activa. Active una temporada antes de crear fletes.",
      422,
    );
  }
  return Number(idTemporada);
}

module.exports = {
  buildDomainError,
  resolveMovilId,
  resolveImputacionFlete,
  getIdTemporadaActiva,
  // Re-exports for convenience (use direct imports from utils/ for new code)
  LIFECYCLE_STATUS,
};
