const { sql } = require("../../db");

// Busca la tarifa de mayor prioridad que matchea (ruta, tipo_camion, fecha).
// Devuelve `{ idTarifa, montoFijo, moneda }` o `{ idTarifa: null, ... }` si no hay match.
//
// Alineado con la logica de flete-route-resolver.service.ts del frontend:
// prioridad DESC y desempate por IdTarifa DESC (mas reciente). Solo considera
// tarifas activas dentro de la vigencia dada fechaSalida.
async function recalcTarifa(transaction, { idRuta, idTipoCamion, fechaSalida }) {
  if (!idRuta || !idTipoCamion || !fechaSalida) {
    return { idTarifa: null, montoFijo: null, moneda: null };
  }
  const result = await new sql.Request(transaction)
    .input("idRuta", sql.BigInt, Number(idRuta))
    .input("idTipoCamion", sql.BigInt, Number(idTipoCamion))
    .input("fecha", sql.Date, fechaSalida)
    .query(`
      SELECT TOP 1 IdTarifa, MontoFijo, Moneda
      FROM [cfl].[Tarifa]
      WHERE IdRuta = @idRuta
        AND IdTipoCamion = @idTipoCamion
        AND Activo = 1
        AND (VigenciaDesde IS NULL OR VigenciaDesde <= @fecha)
        AND (VigenciaHasta IS NULL OR VigenciaHasta >= @fecha)
      ORDER BY Prioridad DESC, IdTarifa DESC;
    `);
  const row = result.recordset[0];
  if (!row) return { idTarifa: null, montoFijo: null, moneda: null };
  return {
    idTarifa: Number(row.IdTarifa),
    montoFijo: row.MontoFijo === null || row.MontoFijo === undefined ? null : Number(row.MontoFijo),
    moneda: row.Moneda ? String(row.Moneda) : null,
  };
}

module.exports = { recalcTarifa };
