'use strict'

const { sql } = require('../db')

// ---------------------------------------------------------------------------
// Constantes
// ---------------------------------------------------------------------------

/** Tasa de IVA vigente. */
const IVA_RATE = 0.19

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Convierte un valor a Number; devuelve `fallback` si el resultado no es finito.
 * @param {*} v       - Valor a convertir.
 * @param {number} [fallback=0] - Valor por defecto.
 * @returns {number}
 */
function toN(v, fallback = 0) {
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

/**
 * Calcula montos neto, IVA y total a partir de un arreglo de movimientos.
 * Cada movimiento debe tener la propiedad `monto_aplicado` (o `MontoAplicado`).
 * @param {Array<{monto_aplicado?: number|string}>} movimientos
 * @returns {{ montoNeto: number, montoIva: number, montoTotal: number }}
 */
function calcMontos(movimientos) {
  const montoNeto = movimientos.reduce((s, m) => s + toN(m.monto_aplicado), 0)
  const montoIva = Math.round(montoNeto * IVA_RATE * 100) / 100
  const montoTotal = Math.round((montoNeto + montoIva) * 100) / 100
  return { montoNeto, montoIva, montoTotal }
}

/**
 * Construye una lista parametrizada de IN para una consulta SQL.
 * Agrega los inputs al `request` y devuelve el fragmento SQL (e.g. `@p0, @p1, @p2`).
 * @param {import('mssql').Request} request - Request de mssql al que se agregan los inputs.
 * @param {Array<number>} ids              - IDs a parametrizar.
 * @param {string} prefix                  - Prefijo para los nombres de parámetro.
 * @returns {string} Fragmento SQL con los placeholders.
 */
function buildInClause(request, ids, prefix) {
  ids.forEach((id, i) => request.input(`${prefix}${i}`, sql.BigInt, id))
  return ids.map((_, i) => `@${prefix}${i}`).join(',')
}

// ---------------------------------------------------------------------------
// Fragmentos SQL reutilizables
// ---------------------------------------------------------------------------

/**
 * Retorna la subconsulta SQL que filtra folios ya incluidos en facturas no anuladas.
 * Uso tipico: `WHERE ${alias}.IdFolio NOT IN (${buildFolioExclusionFilter(alias)})`
 * @param {string} [alias='f'] - Alias de la tabla de folios en la consulta exterior.
 * @returns {string} Subconsulta SQL (sin WHERE externo, solo la expresion NOT IN).
 */
function buildFolioExclusionFilter(alias = 'f') {
  return `${alias}.IdFolio NOT IN (
  SELECT ff.IdFolio
  FROM [cfl].[FacturaFolio] ff
  INNER JOIN [cfl].[CabeceraFactura] fac ON fac.IdFactura = ff.IdFactura
  WHERE LOWER(fac.estado) != 'anulada'
)`
}

/**
 * Columnas SELECT del query de movimientos (superset usado en fetchFactura).
 * @private
 */
const MOVIMIENTOS_COLUMNS = `
      cf.IdCabeceraFlete,
      cf.IdFolio,
      fol.FolioNumero,
      cf.SapNumeroEntrega,
      cf.NumeroEntrega,
      cf.GuiaRemision,
      cf.TipoMovimiento,
      cf.estado,
      cf.FechaSalida,
      cf.MontoAplicado,
      tipo_flete_nombre  = tf.nombre,
      tipo_flete_codigo  = tf.SapCodigo,
      centro_costo       = cc.nombre,
      centro_costo_codigo = cc.SapCodigo,
      ruta = COALESCE(r.NombreRuta,
        CASE WHEN no.nombre IS NOT NULL OR nd.nombre IS NOT NULL
          THEN CONCAT(COALESCE(no.nombre,'Origen'), ' -> ', COALESCE(nd.nombre,'Destino'))
          ELSE NULL END
      ),
      empresa_nombre = COALESCE(NULLIF(LTRIM(RTRIM(emp.RazonSocial)), ''), NULL),
      empresa_rut    = emp.Rut,
      chofer_nombre  = ch.SapNombre,
      chofer_rut     = ch.SapIdFiscal,
      camion_patente = cam.SapPatente,
      camion_carro   = cam.SapCarro,
      tipo_camion    = tc.Nombre,
      detalle_viaje  = dv.Descripcion,
      productor_nombre = prod.Nombre,
      productor_codigo = prod.CodigoProveedor`

/**
 * JOINs estandar para el query de movimientos.
 * @private
 */
const MOVIMIENTOS_JOINS = `
    FROM [cfl].[CabeceraFlete] cf
    LEFT JOIN [cfl].[Folio] fol ON fol.IdFolio = cf.IdFolio
    LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
    LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
    LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
    LEFT JOIN [cfl].[EmpresaTransporte] emp ON emp.IdEmpresa = mv.IdEmpresaTransporte
    LEFT JOIN [cfl].[Chofer] ch ON ch.IdChofer = mv.IdChofer
    LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
    LEFT JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = cam.IdTipoCamion
    LEFT JOIN [cfl].[Tarifa] tar ON tar.IdTarifa = cf.IdTarifa
    LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tar.IdRuta
    LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
    LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
    LEFT JOIN [cfl].[DetalleViaje] dv ON dv.IdDetalleViaje = cf.IdDetalleViaje
    LEFT JOIN [cfl].[Productor] prod ON prod.IdProductor = cf.IdProductor`

/**
 * Construye el SELECT completo de movimientos con todos los JOINs estandar.
 * El llamador provee la clausula WHERE y opcionalmente el ORDER BY.
 * @param {string} whereClause - Clausula WHERE completa (incluir la palabra WHERE).
 * @param {string} [orderBy='cf.FechaSalida, cf.IdCabeceraFlete'] - Columnas de ordenamiento.
 * @returns {string} Query SQL listo para ejecutar.
 */
function buildMovimientosQuery(whereClause, orderBy = 'cf.FechaSalida, cf.IdCabeceraFlete') {
  return `SELECT${MOVIMIENTOS_COLUMNS}${MOVIMIENTOS_JOINS}
    ${whereClause}
    ORDER BY ${orderBy};`
}

// ---------------------------------------------------------------------------
// Operaciones parametrizadas
// ---------------------------------------------------------------------------

/**
 * Actualiza el estado de los fletes asociados a un conjunto de folios.
 * Patron duplicado en generar, agregar folios, quitar folios, eliminar y anular.
 * @param {import('mssql').Transaction|import('mssql').Request} requestOrTransaction
 *   - Si es Transaction se crea un Request interno; si ya es Request se usa directamente.
 * @param {Array<number>} folioIds   - IDs de folios cuyos fletes se actualizan.
 * @param {string} fromEstado        - Estado actual esperado (UPPER-cased, e.g. 'ASIGNADO_FOLIO').
 * @param {string} toEstado          - Nuevo estado (e.g. 'FACTURADO').
 * @param {Date} now                 - Timestamp para FechaActualizacion.
 * @returns {Promise<import('mssql').IResult<any>>} Resultado del UPDATE.
 */
async function updateFletesEstado(requestOrTransaction, folioIds, fromEstado, toEstado, now) {
  if (!folioIds.length) return null

  // Validar estados para prevenir inyección SQL (solo valores conocidos)
  const VALID_STATES = ['ASIGNADO_FOLIO', 'FACTURADO', 'COMPLETADO', 'DETECTADO']
  if (!VALID_STATES.includes(fromEstado) || !VALID_STATES.includes(toEstado)) {
    throw new Error(`Estado inválido: from=${fromEstado}, to=${toEstado}`)
  }

  // Si recibimos un Transaction, creamos un Request a partir de él
  const req = (requestOrTransaction instanceof sql.Transaction)
    ? new sql.Request(requestOrTransaction)
    : requestOrTransaction

  req.input('updatedAt', sql.DateTime2(0), now)
  const inFragment = buildInClause(req, folioIds, 'uf')

  return req.query(`
    UPDATE [cfl].[CabeceraFlete]
    SET estado = '${toEstado}', FechaActualizacion = @updatedAt
    WHERE IdFolio IN (${inFragment})
      AND UPPER(estado) = '${fromEstado}';
  `)
}

module.exports = {
  IVA_RATE,
  toN,
  calcMontos,
  buildInClause,
  buildFolioExclusionFilter,
  buildMovimientosQuery,
  updateFletesEstado,
}
