/**
 * Utilidades de parsing de parámetros de entrada.
 * Sin dependencias de BD ni de dominio.
 */

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

/** Normaliza tipo de movimiento a PUSH o PULL; devuelve null si inválido. */
function normalizeTipoMovimiento(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized === "PUSH" || normalized === "DESPACHO") return "PUSH";
  if (normalized === "PULL" || normalized === "RETORNO") return "PULL";
  return null;
}

module.exports = {
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  parsePositiveInt,
  clamp,
  normalizeTipoMovimiento,
};
