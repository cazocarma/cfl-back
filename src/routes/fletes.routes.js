const express = require("express");
const { getPool, sql } = require("../db");

const router = express.Router();

function toNullableTrimmedString(value) {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseOptionalBigInt(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function parseRequiredBigInt(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

async function fetchCabecera(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    SELECT TOP 1 *
    FROM [cfl].[CFL_cabecera_flete]
    WHERE id_cabecera_flete = @idCabecera;
  `);

  return result.recordset[0] || null;
}

async function fetchDetalles(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    SELECT
      id_detalle_flete,
      id_cabecera_flete,
      id_especie,
      material,
      descripcion,
      cantidad,
      unidad,
      peso,
      created_at
    FROM [cfl].[CFL_detalle_flete]
    WHERE id_cabecera_flete = @idCabecera
    ORDER BY id_detalle_flete ASC;
  `);

  return result.recordset;
}

router.get("/:id_cabecera_flete", async (req, res, next) => {
  const idCabecera = Number(req.params.id_cabecera_flete);
  if (!Number.isInteger(idCabecera) || idCabecera <= 0) {
    res.status(400).json({ error: "id_cabecera_flete invalido" });
    return;
  }

  try {
    const pool = await getPool();
    const cabecera = await fetchCabecera(pool, idCabecera);
    if (!cabecera) {
      res.status(404).json({ error: "Cabecera no encontrada" });
      return;
    }

    const detalles = await fetchDetalles(pool, idCabecera);
    res.json({ data: { cabecera, detalles } });
  } catch (error) {
    next(error);
  }
});

router.post("/manual", async (req, res, next) => {
  const body = req.body || {};
  const cabeceraIn = body.cabecera || {};
  const detallesIn = Array.isArray(body.detalles) ? body.detalles : [];

  const idTipoFlete = parseRequiredBigInt(cabeceraIn.id_tipo_flete);
  const idCentroCostoFinal = parseRequiredBigInt(cabeceraIn.id_centro_costo_final);
  const tipoMovimiento = toNullableTrimmedString(cabeceraIn.tipo_movimiento) || "PUSH";
  const estado = toNullableTrimmedString(cabeceraIn.estado) || "Completo";
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const cuentaMayorFinal = toNullableTrimmedString(cabeceraIn.cuenta_mayor_final);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoFinal) {
    res.status(400).json({ error: "Falta id_centro_costo_final" });
    return;
  }
  if (!["PUSH", "PULL"].includes(tipoMovimiento)) {
    res.status(400).json({ error: "tipo_movimiento invalido (PUSH/PULL)" });
    return;
  }
  if (!fechaSalida) {
    res.status(400).json({ error: "Falta fecha_salida (YYYY-MM-DD)" });
    return;
  }
  if (!horaSalida) {
    res.status(400).json({ error: "Falta hora_salida (HH:MM[:SS])" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();

    const insertCabeceraReq = new sql.Request(transaction);
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_detalle_viaje));
    insertCabeceraReq.input("idFolio", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_folio));
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), toNullableTrimmedString(cabeceraIn.sap_numero_entrega_sugerido));
    insertCabeceraReq.input("sapCodigoTipoFleteSug", sql.Char(4), toNullableTrimmedString(cabeceraIn.sap_codigo_tipo_flete_sugerido));
    insertCabeceraReq.input("sapCentroCostoSug", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_centro_costo_sugerido));
    insertCabeceraReq.input("sapCuentaMayorSug", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_cuenta_mayor_sugerida));
    insertCabeceraReq.input("cuentaMayorFinal", sql.Char(10), toNullableTrimmedString(cabeceraIn.cuenta_mayor_final));
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), tipoMovimiento);
    insertCabeceraReq.input("estado", sql.VarChar(20), estado);
    insertCabeceraReq.input("fechaSalida", sql.Date, fechaSalida);
    insertCabeceraReq.input("horaSalida", sql.VarChar(8), horaSalida);
    insertCabeceraReq.input("montoAplicado", sql.Decimal(18, 2), montoAplicado);
    insertCabeceraReq.input("idMovil", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_movil));
    insertCabeceraReq.input("idTarifa", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_tarifa));
    insertCabeceraReq.input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones));
    insertCabeceraReq.input("idUsuarioCreador", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_usuario_creador));
    insertCabeceraReq.input("idTipoFlete", sql.BigInt, idTipoFlete);
    insertCabeceraReq.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("idCentroCostoFinal", sql.BigInt, idCentroCostoFinal);

    const cabeceraResult = await insertCabeceraReq.query(`
      INSERT INTO [cfl].[CFL_cabecera_flete] (
        [id_detalle_viaje],
        [id_folio],
        [sap_numero_entrega_sugerido],
        [sap_codigo_tipo_flete_sugerido],
        [sap_centro_costo_sugerido],
        [sap_cuenta_mayor_sugerida],
        [cuenta_mayor_final],
        [tipo_movimiento],
        [estado],
        [fecha_salida],
        [hora_salida],
        [monto_aplicado],
        [id_movil],
        [id_tarifa],
        [observaciones],
        [id_usuario_creador],
        [id_tipo_flete],
        [created_at],
        [updated_at],
        [id_centro_costo_final]
      )
      OUTPUT INSERTED.id_cabecera_flete
      VALUES (
        @idDetalleViaje,
        @idFolio,
        @sapNumeroEntrega,
        @sapCodigoTipoFleteSug,
        @sapCentroCostoSug,
        @sapCuentaMayorSug,
        @cuentaMayorFinal,
        @tipoMovimiento,
        @estado,
        @fechaSalida,
        CAST(@horaSalida AS TIME),
        @montoAplicado,
        @idMovil,
        @idTarifa,
        @observaciones,
        @idUsuarioCreador,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCentroCostoFinal
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].id_cabecera_flete;

    for (const detalle of detallesIn) {
      const material = toNullableTrimmedString(detalle.material);
      const descripcion = toNullableTrimmedString(detalle.descripcion);
      const unidad = toNullableTrimmedString(detalle.unidad);
      const cantidad = detalle.cantidad === null || detalle.cantidad === undefined || detalle.cantidad === "" ? null : Number(detalle.cantidad);
      const peso = detalle.peso === null || detalle.peso === undefined || detalle.peso === "" ? null : Number(detalle.peso);
      const idEspecie = parseOptionalBigInt(detalle.id_especie);

      await new sql.Request(transaction)
        .input("idCabeceraFlete", sql.BigInt, idCabeceraFlete)
        .input("idEspecie", sql.BigInt, idEspecie)
        .input("material", sql.VarChar(50), material)
        .input("descripcion", sql.VarChar(100), descripcion)
        .input("cantidad", sql.Decimal(12, 2), Number.isFinite(cantidad) ? cantidad : null)
        .input("unidad", sql.Char(3), unidad ? unidad.slice(0, 3) : null)
        .input("peso", sql.Decimal(15, 3), Number.isFinite(peso) ? peso : null)
        .input("createdAt", sql.DateTime2(0), now)
        .query(`
          INSERT INTO [cfl].[CFL_detalle_flete] (
            [id_cabecera_flete],
            [id_especie],
            [material],
            [descripcion],
            [cantidad],
            [unidad],
            [peso],
            [created_at]
          )
          VALUES (
            @idCabeceraFlete,
            @idEspecie,
            @material,
            @descripcion,
            @cantidad,
            @unidad,
            @peso,
            @createdAt
          );
        `);
    }

    await transaction.commit();

    res.status(201).json({
      message: "Flete manual creado",
      data: {
        id_cabecera_flete: idCabeceraFlete,
      },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }
    next(error);
  }
});

router.put("/:id_cabecera_flete", async (req, res, next) => {
  const idCabecera = Number(req.params.id_cabecera_flete);
  if (!Number.isInteger(idCabecera) || idCabecera <= 0) {
    res.status(400).json({ error: "id_cabecera_flete invalido" });
    return;
  }

  const body = req.body || {};
  const cabeceraIn = body.cabecera || {};
  const detallesIn = Array.isArray(body.detalles) ? body.detalles : [];

  const idTipoFlete = parseRequiredBigInt(cabeceraIn.id_tipo_flete);
  const idCentroCostoFinal = parseRequiredBigInt(cabeceraIn.id_centro_costo_final);
  const tipoMovimiento = toNullableTrimmedString(cabeceraIn.tipo_movimiento) || "PUSH";
  const estado = toNullableTrimmedString(cabeceraIn.estado) || "Completo";
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoFinal) {
    res.status(400).json({ error: "Falta id_centro_costo_final" });
    return;
  }
  if (!["PUSH", "PULL"].includes(tipoMovimiento)) {
    res.status(400).json({ error: "tipo_movimiento invalido (PUSH/PULL)" });
    return;
  }
  if (!fechaSalida) {
    res.status(400).json({ error: "Falta fecha_salida (YYYY-MM-DD)" });
    return;
  }
  if (!horaSalida) {
    res.status(400).json({ error: "Falta hora_salida (HH:MM[:SS])" });
    return;
  }

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();
    const existing = await fetchCabecera(pool, idCabecera);
    if (!existing) {
      await transaction.rollback();
      res.status(404).json({ error: "Cabecera no encontrada" });
      return;
    }

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .input("tipoMovimiento", sql.VarChar(4), tipoMovimiento)
      .input("estado", sql.VarChar(20), estado)
      .input("fechaSalida", sql.Date, fechaSalida)
      .input("horaSalida", sql.VarChar(8), horaSalida)
      .input("montoAplicado", sql.Decimal(18, 2), montoAplicado)
      .input("cuentaMayorFinal", sql.Char(10), cuentaMayorFinal ? cuentaMayorFinal.slice(0, 10) : null)
      .input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones))
      .input("idTipoFlete", sql.BigInt, idTipoFlete)
      .input("idCentroCostoFinal", sql.BigInt, idCentroCostoFinal)
      .input("updatedAt", sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET
          tipo_movimiento = @tipoMovimiento,
          estado = @estado,
          fecha_salida = @fechaSalida,
          hora_salida = CAST(@horaSalida AS TIME),
          monto_aplicado = @montoAplicado,
          cuenta_mayor_final = @cuentaMayorFinal,
          observaciones = @observaciones,
          id_tipo_flete = @idTipoFlete,
          id_centro_costo_final = @idCentroCostoFinal,
          updated_at = @updatedAt
        WHERE id_cabecera_flete = @idCabecera;
      `);

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .query(`
        DELETE FROM [cfl].[CFL_detalle_flete]
        WHERE id_cabecera_flete = @idCabecera;
      `);

    for (const detalle of detallesIn) {
      const material = toNullableTrimmedString(detalle.material);
      const descripcion = toNullableTrimmedString(detalle.descripcion);
      const unidad = toNullableTrimmedString(detalle.unidad);
      const cantidad = detalle.cantidad === null || detalle.cantidad === undefined || detalle.cantidad === "" ? null : Number(detalle.cantidad);
      const peso = detalle.peso === null || detalle.peso === undefined || detalle.peso === "" ? null : Number(detalle.peso);
      const idEspecie = parseOptionalBigInt(detalle.id_especie);

      await new sql.Request(transaction)
        .input("idCabeceraFlete", sql.BigInt, idCabecera)
        .input("idEspecie", sql.BigInt, idEspecie)
        .input("material", sql.VarChar(50), material)
        .input("descripcion", sql.VarChar(100), descripcion)
        .input("cantidad", sql.Decimal(12, 2), Number.isFinite(cantidad) ? cantidad : null)
        .input("unidad", sql.Char(3), unidad ? unidad.slice(0, 3) : null)
        .input("peso", sql.Decimal(15, 3), Number.isFinite(peso) ? peso : null)
        .input("createdAt", sql.DateTime2(0), now)
        .query(`
          INSERT INTO [cfl].[CFL_detalle_flete] (
            [id_cabecera_flete],
            [id_especie],
            [material],
            [descripcion],
            [cantidad],
            [unidad],
            [peso],
            [created_at]
          )
          VALUES (
            @idCabeceraFlete,
            @idEspecie,
            @material,
            @descripcion,
            @cantidad,
            @unidad,
            @peso,
            @createdAt
          );
        `);
    }

    await transaction.commit();

    const updatedCabecera = await fetchCabecera(pool, idCabecera);
    const updatedDetalles = await fetchDetalles(pool, idCabecera);

    res.json({
      message: "Flete actualizado",
      data: { cabecera: updatedCabecera, detalles: updatedDetalles },
    });
  } catch (error) {
    if (transaction) {
      try {
        await transaction.rollback();
      } catch (_rollbackError) {
        // no-op
      }
    }
    next(error);
  }
});

module.exports = {
  fletesRouter: router,
};
