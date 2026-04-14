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
  PREFACTURADO: "PREFACTURADO",
  FACTURADO: "FACTURADO",
};

/**
 * Grafo de transiciones permitidas (clave = estado actual, valor = set de
 * estados destino válidos). Un INSERT de flete nuevo arranca desde "null"
 * (sin estado previo) — por eso existe la entrada `null`.
 *
 * Reglas:
 *   - Un flete nuevo sólo puede nacer en DETECTADO, ACTUALIZADO, EN_REVISION
 *     o COMPLETADO (nunca directamente en PREFACTURADO/FACTURADO).
 *   - PREFACTURADO es un estado interno que SOLO lo produce el endpoint
 *     `/facturas/generar` pasando `internalTransition: true`.
 *   - FACTURADO y ANULADO son terminales.
 */
const ALLOWED_TRANSITIONS = {
  null: new Set([
    LIFECYCLE_STATUS.DETECTADO,
    LIFECYCLE_STATUS.ACTUALIZADO,
    LIFECYCLE_STATUS.EN_REVISION,
    LIFECYCLE_STATUS.COMPLETADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.DETECTADO]: new Set([
    LIFECYCLE_STATUS.ACTUALIZADO,
    LIFECYCLE_STATUS.EN_REVISION,
    LIFECYCLE_STATUS.COMPLETADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.ACTUALIZADO]: new Set([
    LIFECYCLE_STATUS.EN_REVISION,
    LIFECYCLE_STATUS.COMPLETADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.EN_REVISION]: new Set([
    LIFECYCLE_STATUS.COMPLETADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.COMPLETADO]: new Set([
    LIFECYCLE_STATUS.EN_REVISION,
    LIFECYCLE_STATUS.PREFACTURADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.PREFACTURADO]: new Set([
    LIFECYCLE_STATUS.COMPLETADO,
    LIFECYCLE_STATUS.FACTURADO,
    LIFECYCLE_STATUS.ANULADO,
  ]),
  [LIFECYCLE_STATUS.FACTURADO]: new Set([]),
  [LIFECYCLE_STATUS.ANULADO]: new Set([]),
};

/** Estados que sólo pueden alcanzarse via flujos internos específicos. */
const INTERNAL_ONLY_STATES = new Set([
  LIFECYCLE_STATUS.PREFACTURADO,
  LIFECYCLE_STATUS.FACTURADO,
]);

/**
 * Normaliza un valor de estado a su clave canónica.
 * Maneja alias heredados (COMPLETO, VALIDADO, CERRADO).
 * Devuelve null si el valor no es reconocido.
 */
function normalizeLifecycleStatus(rawStatus) {
  const normalized = String(rawStatus || "").trim().toUpperCase();
  if (!normalized) return null;

  if (normalized === "COMPLETO") return LIFECYCLE_STATUS.COMPLETADO;
  if (normalized === "VALIDADO") return LIFECYCLE_STATUS.PREFACTURADO;
  if (normalized === "CERRADO") return LIFECYCLE_STATUS.FACTURADO;

  return Object.values(LIFECYCLE_STATUS).includes(normalized) ? normalized : null;
}

/**
 * ¿Puede un flete en estado `fromStatus` pasar a `toStatus`?
 * `fromStatus = null` representa un flete que aún no existe (INSERT).
 */
function canTransitionTo(fromStatus, toStatus) {
  if (!toStatus) return false;
  const from = fromStatus === null || fromStatus === undefined ? null : fromStatus;
  const allowed = ALLOWED_TRANSITIONS[from];
  if (!allowed) return false;
  return allowed.has(toStatus);
}

/**
 * Deriva el estado del ciclo de vida según las condiciones actuales del flete.
 *
 * @param {object} args
 * @param {string|null} args.currentStatus       Estado actual en BD (null si es INSERT).
 * @param {string|null} args.requestedStatus     Estado solicitado por el caller.
 * @param {boolean}     [args.internalTransition=false]
 *   Si `true`, se permiten transiciones a estados INTERNAL_ONLY_STATES
 *   (PREFACTURADO, FACTURADO). Sólo lo activan los flujos de /facturas y
 *   /planillas internamente.
 * @param {number|null} args.idTipoFlete
 * @param {number|null} args.idCentroCosto
 * @param {number|null} args.idDetalleViaje
 * @param {number|null} args.idMovil
 * @param {number|null} args.idTarifa
 * @param {boolean}     args.hasDetalles
 */
function deriveLifecycleStatus({
  currentStatus = null,
  requestedStatus,
  internalTransition = false,
  idTipoFlete,
  idCentroCosto,
  idDetalleViaje,
  idMovil,
  idTarifa,
  hasDetalles,
}) {
  // Honra el estado solicitado SÓLO si (a) es una transición válida y
  // (b) el caller tiene permiso de disparar ese estado (INTERNAL_ONLY_STATES
  // requieren `internalTransition: true`).
  if (requestedStatus) {
    const isInternalOnly = INTERNAL_ONLY_STATES.has(requestedStatus);
    if (isInternalOnly && !internalTransition) {
      // El request no puede disparar PREFACTURADO/FACTURADO directamente;
      // caemos al cálculo por completitud abajo.
    } else if (canTransitionTo(currentStatus, requestedStatus)) {
      return requestedStatus;
    } else {
      // Transición solicitada es inválida desde el estado actual. Caemos al
      // cálculo por completitud; el caller puede tratar este caso como error
      // si es relevante.
    }
  }

  const isComplete =
    Boolean(idTipoFlete) &&
    Boolean(idCentroCosto) &&
    Boolean(idDetalleViaje) &&
    Boolean(idMovil) &&
    Boolean(idTarifa) &&
    Boolean(hasDetalles);

  const computed = isComplete ? LIFECYCLE_STATUS.COMPLETADO : LIFECYCLE_STATUS.EN_REVISION;
  // Si el cálculo automático no es una transición válida desde el estado
  // actual (ej. ya es PREFACTURADO y se intenta downgrade a EN_REVISION),
  // mantenemos el estado actual.
  if (!canTransitionTo(currentStatus, computed)) {
    return currentStatus || computed;
  }
  return computed;
}

module.exports = {
  LIFECYCLE_STATUS,
  INTERNAL_ONLY_STATES,
  ALLOWED_TRANSITIONS,
  normalizeLifecycleStatus,
  canTransitionTo,
  deriveLifecycleStatus,
};
