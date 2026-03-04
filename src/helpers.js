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
      SELECT TOP 1 id_movil
      FROM [cfl].[CFL_movil]
      WHERE id_empresa_transporte = @idEmpresa
        AND id_chofer = @idChofer
        AND id_camion = @idCamion
      ORDER BY CASE WHEN activo = 1 THEN 0 ELSE 1 END, id_movil ASC;
    `);

  const existingMovilId = lookup.recordset[0]?.id_movil || null;
  if (existingMovilId) return Number(existingMovilId);

  const created = await new sql.Request(transaction)
    .input("idEmpresa", sql.BigInt, idEmpresaTransporte)
    .input("idChofer", sql.BigInt, idChofer)
    .input("idCamion", sql.BigInt, idCamion)
    .input("activo", sql.Bit, true)
    .input("createdAt", sql.DateTime2(0), now)
    .input("updatedAt", sql.DateTime2(0), now)
    .query(`
      INSERT INTO [cfl].[CFL_movil] (
        [id_chofer],
        [id_empresa_transporte],
        [id_camion],
        [activo],
        [created_at],
        [updated_at]
      )
      OUTPUT INSERTED.id_movil
      VALUES (
        @idChofer,
        @idEmpresa,
        @idCamion,
        @activo,
        @createdAt,
        @updatedAt
      );
    `);

  return Number(created.recordset[0].id_movil);
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
      SELECT TOP 1 folio_numero
      FROM [cfl].[CFL_folio]
      WHERE id_folio = @idFolio;
    `);

  const row = result.recordset[0] || null;
  if (!row) return parsed;

  const numero = String(row.folio_numero || "").trim();
  return numero === "0" ? null : parsed;
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
};
