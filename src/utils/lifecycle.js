/**
 * Constantes y lógica del ciclo de vida de un flete.
 * Sin dependencias de BD.
 */

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

module.exports = {
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
};
