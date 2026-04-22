// ────────────────────────────────────────────────────────────────────
// Pipeline de resolucion de entidades de transporte + recalc de tarifa.
//
// Integracion: llamado ANTES del INSERT/UPDATE de CabeceraFlete en los
// endpoints existentes. Muta los ids de transporte y la tarifa del
// payload para que el flujo posterior (resolveMovilId + INSERT) use
// los ids definitivos.
//
// Todo ejecuta dentro de la transaccion abierta por el caller.
// ────────────────────────────────────────────────────────────────────

const { resolveEmpresa } = require("./resolve-empresa");
const { resolveChofer } = require("./resolve-chofer");
const { resolveCamion, getCamionTipo } = require("./resolve-camion");
const { recalcTarifa } = require("./recalc-tarifa");

// Aplica la seccion `transport` del body:
//  1. Crea/actualiza empresa, chofer y camion segun intents.
//  2. Si camion cambio de tipo o recalc_tarifa=true, recalcula tarifa
//     con idTipoCamion final y ruta provista (route_context).
//  3. Muta `cabeceraIn` con los ids resueltos y con la tarifa recalculada
//     (id_tarifa, monto_aplicado) si corresponde.
//
// Retorna metadata usada por el caller (ej. warnings).
async function applyTransportIntent(transaction, { cabeceraIn, transportIntent, now }) {
  const warnings = [];

  if (!transportIntent) {
    return { warnings, tipoCamionChanged: false, idTarifaRecalculada: null };
  }

  // 1. Empresa
  const resolvedEmpresaId = await resolveEmpresa(transaction, transportIntent.empresa, now);
  if (resolvedEmpresaId) {
    cabeceraIn.id_empresa_transporte = resolvedEmpresaId;
  }

  // 2. Chofer
  const resolvedChoferId = await resolveChofer(transaction, transportIntent.chofer);
  if (resolvedChoferId) {
    cabeceraIn.id_chofer = resolvedChoferId;
  }

  // 3. Camion (tambien aplica update_tipo_camion si viene confirmado)
  const camionResult = await resolveCamion(transaction, transportIntent.camion, now);
  if (camionResult.idCamion) {
    cabeceraIn.id_camion = camionResult.idCamion;
  }
  const tipoCamionChanged = camionResult.tipoCamionChanged;

  // 4. Recalc tarifa si corresponde
  let idTarifaRecalculada = null;
  const shouldRecalc = transportIntent.recalc_tarifa === true || tipoCamionChanged;

  if (shouldRecalc) {
    const ctx = transportIntent.route_context || {};
    const fechaSalida = ctx.fecha_salida || cabeceraIn.fecha_salida || null;
    const idRuta = ctx.id_ruta || null;
    const finalIdCamion = cabeceraIn.id_camion ? Number(cabeceraIn.id_camion) : null;
    const idTipoCamion = finalIdCamion ? await getCamionTipo(transaction, finalIdCamion) : null;

    const tarifa = await recalcTarifa(transaction, { idRuta, idTipoCamion, fechaSalida });
    if (tarifa.idTarifa) {
      cabeceraIn.id_tarifa = tarifa.idTarifa;
      // MontoAplicado = MontoFijo + MontoExtra (el front preserva si edito manualmente).
      const extra = Number(cabeceraIn.monto_extra || 0) || 0;
      cabeceraIn.monto_aplicado = Number(tarifa.montoFijo || 0) + extra;
      idTarifaRecalculada = tarifa.idTarifa;
    } else {
      // Sin match: dejar id_tarifa y monto_aplicado en null.
      cabeceraIn.id_tarifa = null;
      cabeceraIn.monto_aplicado = null;
      warnings.push("No se encontró tarifa para la combinación ruta + tipo camión + fecha. Se guardó sin tarifa (monto_aplicado = null).");
    }
  }

  return {
    warnings,
    tipoCamionChanged,
    idTarifaRecalculada,
  };
}

module.exports = {
  applyTransportIntent,
};
