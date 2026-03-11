/**
 * Utilidades de parsing y ciclo de vida compartidas entre routes.
 * Centraliza lógica que antes estaba duplicada en dashboard y fletes.
 */

const { sql } = require("./db");

// ---------------------------------------------------------------------------
// Parsing básico
// ---------------------------------------------------------------------------

/** Recorta un string; devuelve null si vacío o no es string. */
function toNullableTrimmedString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** Parsea un entero positivo; devuelve null si inválido o no positivo. */
function parseOptionalBigInt(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

/** Igual que parseOptionalBigInt pero semánticamente requerido en el contexto de uso. */
function parseRequiredBigInt(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

/** Parsea entero positivo con fallback; usado en paginación. */
function parsePositiveInt(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/** Limita value al rango [min, max]. */
function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

// ---------------------------------------------------------------------------
// Normalización de dominio
// ---------------------------------------------------------------------------

/** Normaliza tipo de movimiento a PUSH o PULL; devuelve null si inválido. */
function normalizeTipoMovimiento(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "PUSH" || normalized === "DESPACHO") return "PUSH";
  if (normalized === "PULL" || normalized === "RETORNO") return "PULL";
  return null;
}

/** Estados canónicos del ciclo de vida de un flete. */
const LIFECYCLE_STATUS = {
  DETECTADO: "DETECTADO",
  ACTUALIZADO: "ACTUALIZADO",
  ANULADO: "ANULADO",
  EN_REVISION: "EN_REVISION",
  COMPLETADO: "COMPLETADO",
  ASIGNADO_FOLIO: "ASIGNADO_FOLIO",
  FACTURADO: "FACTURADO",
};

/**
 * Normaliza un valor de estado a su clave canónica.
 * Maneja alias heredados (COMPLETO, VALIDADO, CERRADO).
 * Devuelve null si el valor no es reconocido.
 */
function normalizeLifecycleStatus(rawStatus) {
  const normalized = String(rawStatus || "").trim().toUpperCase();
  if (!normalized) return null;

  if (normalized === "COMPLETO") return LIFECYCLE_STATUS.COMPLETADO;
  if (normalized === "VALIDADO") return LIFECYCLE_STATUS.ASIGNADO_FOLIO;
  if (normalized === "CERRADO") return LIFECYCLE_STATUS.FACTURADO;

  return Object.values(LIFECYCLE_STATUS).includes(normalized) ? normalized : null;
}

/**
 * Deriva el estado del ciclo de vida según las condiciones actuales del flete.
 * Los estados ANULADO y FACTURADO son forzados desde el request; el resto se calcula.
 */
function deriveLifecycleStatus({
  requestedStatus,
  idFolio,
  idTipoFlete,
  idCentroCosto,
  idDetalleViaje,
  idMovil,
  idTarifa,
  hasDetalles,
}) {
  if (requestedStatus === LIFECYCLE_STATUS.ANULADO) return LIFECYCLE_STATUS.ANULADO;
  if (requestedStatus === LIFECYCLE_STATUS.FACTURADO) return LIFECYCLE_STATUS.FACTURADO;
  if (idFolio && Number(idFolio) > 0) return LIFECYCLE_STATUS.ASIGNADO_FOLIO;

  const isComplete =
    Boolean(idTipoFlete) &&
    Boolean(idCentroCosto) &&
    Boolean(idDetalleViaje) &&
    Boolean(idMovil) &&
    Boolean(idTarifa) &&
    Boolean(hasDetalles);

  return isComplete ? LIFECYCLE_STATUS.COMPLETADO : LIFECYCLE_STATUS.EN_REVISION;
}

// ---------------------------------------------------------------------------
// Helpers de BD: movil y folio
// ---------------------------------------------------------------------------

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

/**
 * Devuelve el id_folio si es válido y no es el folio reservado (folio_numero = 0).
 * Usado para determinar si el flete debe quedar en estado ASIGNADO_FOLIO.
 */
async function resolveFolioForLifecycle(transaction, idFolio) {
  const parsed = Number(idFolio);
  if (!Number.isInteger(parsed) || parsed <= 0) return null;

  const result = await new sql.Request(transaction)
    .input("idFolio", sql.BigInt, parsed)
    .query(`
      SELECT TOP 1 FolioNumero
      FROM [cfl].[Folio]
      WHERE IdFolio = @idFolio;
    `);

  const row = result.recordset[0] || null;
  if (!row) return parsed;

  const numero = String(row.FolioNumero || "").trim();
  return numero === "0" ? null : parsed;
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
  // Parsing
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  parsePositiveInt,
  clamp,
  // Dominio
  normalizeTipoMovimiento,
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
  // BD
  resolveMovilId,
  resolveFolioForLifecycle,
  resolveImputacionFlete,
};
