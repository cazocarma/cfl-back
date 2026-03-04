const express = require("express");
const { getPool, sql } = require("../db");
const {
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  normalizeTipoMovimiento,
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
  resolveMovilId,
  resolveFolioForLifecycle,
} = require("../helpers");

const router = express.Router();

async function fetchCabecera(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    SELECT TOP 1 *
    FROM [cfl].[CFL_cabecera_flete]
    WHERE id_cabecera_flete = @idCabecera;
  `);

  return result.recordset[0] || null;
}

async function fetchSapCurrentDetalles(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    ;WITH sap_source AS (
      SELECT
        fe.created_at AS bridge_created_at,
        e.id_sap_entrega,
        e.sap_numero_entrega,
        e.source_system
      FROM [cfl].[CFL_flete_sap_entrega] fe
      INNER JOIN [cfl].[CFL_sap_entrega] e
        ON e.id_sap_entrega = fe.id_sap_entrega
      WHERE fe.id_cabecera_flete = @idCabecera
    ),
    sap_positions AS (
      SELECT
        row_num = ROW_NUMBER() OVER (
          ORDER BY
            src.sap_numero_entrega ASC,
            TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(lp.sap_posicion)), '')),
            lp.sap_posicion ASC
        ),
        src.bridge_created_at,
        sap_numero_entrega = src.sap_numero_entrega,
        sap_posicion = NULLIF(LTRIM(RTRIM(lp.sap_posicion)), ''),
        sap_material = NULLIF(LTRIM(RTRIM(lp.sap_material)), ''),
        sap_denominacion_material = NULLIF(LTRIM(RTRIM(lp.sap_denominacion_material)), ''),
        sap_cantidad_entregada = TRY_CONVERT(DECIMAL(12, 2), lp.sap_cantidad_entregada),
        sap_unidad_peso = NULLIF(LTRIM(RTRIM(lp.sap_unidad_peso)), ''),
        sap_posicion_superior = NULLIF(LTRIM(RTRIM(lp.sap_posicion_superior)), ''),
        sap_lote = NULLIF(LTRIM(RTRIM(lp.sap_lote)), '')
      FROM sap_source src
      INNER JOIN [cfl].[vw_cfl_sap_lips_current] lp
        ON lp.source_system = src.source_system
       AND lp.sap_numero_entrega = src.sap_numero_entrega
    ),
    existing_details AS (
      SELECT
        row_num = ROW_NUMBER() OVER (ORDER BY id_detalle_flete ASC),
        id_especie
      FROM [cfl].[CFL_detalle_flete]
      WHERE id_cabecera_flete = @idCabecera
    )
    SELECT
      id_detalle_flete = sp.row_num,
      id_cabecera_flete = @idCabecera,
      id_especie = ed.id_especie,
      material = sp.sap_material,
      descripcion = sp.sap_denominacion_material,
      cantidad = sp.sap_cantidad_entregada,
      unidad = CASE
        WHEN sp.sap_unidad_peso IS NULL THEN NULL
        ELSE LEFT(sp.sap_unidad_peso, 3)
      END,
      peso = CASE
        WHEN UPPER(COALESCE(sp.sap_unidad_peso, '')) LIKE 'KG%' THEN TRY_CONVERT(DECIMAL(15, 3), sp.sap_cantidad_entregada)
        ELSE NULL
      END,
      created_at = sp.bridge_created_at,
      sap_numero_entrega = sp.sap_numero_entrega,
      sap_posicion = sp.sap_posicion,
      sap_posicion_superior = sp.sap_posicion_superior,
      sap_lote = sp.sap_lote
    FROM sap_positions sp
    LEFT JOIN existing_details ed
      ON ed.row_num = sp.row_num
    ORDER BY sp.row_num ASC;
  `);

  return result.recordset;
}

async function fetchDetalles(pool, idCabecera) {
  const sapCurrentDetalles = await fetchSapCurrentDetalles(pool, idCabecera);
  if (sapCurrentDetalles.length > 0) {
    return sapCurrentDetalles;
  }

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
  const idCentroCosto = parseRequiredBigInt(cabeceraIn.id_centro_costo);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  // cuenta_mayor_final eliminado (redundante con id_cuenta_mayor FK)
  const guiaRemision   = toNullableTrimmedString(cabeceraIn.guia_remision);
  const numeroEntrega  = toNullableTrimmedString(cabeceraIn.numero_entrega);
  const idDetalleViaje = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idFolio = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifa = parseOptionalBigInt(cabeceraIn.id_tarifa);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCosto) {
    res.status(400).json({ error: "Falta id_centro_costo" });
    return;
  }
  if (!tipoMovimiento) {
    res.status(400).json({ error: "tipo_movimiento invalido (Despacho/Retorno)" });
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
    const idMovil = await resolveMovilId(transaction, cabeceraIn, now);
    const lifecycleFolioId = await resolveFolioForLifecycle(transaction, idFolio);
    const estado = deriveLifecycleStatus({
      requestedStatus,
      idFolio: lifecycleFolioId,
      idTipoFlete,
      idCentroCosto,
      idDetalleViaje,
      idMovil,
      idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    const insertCabeceraReq = new sql.Request(transaction);
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, idDetalleViaje);
    insertCabeceraReq.input("idFolio", sql.BigInt, idFolio);
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), toNullableTrimmedString(cabeceraIn.sap_numero_entrega));
    insertCabeceraReq.input("sapCodigoTipoFlete", sql.Char(4), toNullableTrimmedString(cabeceraIn.sap_codigo_tipo_flete));
    insertCabeceraReq.input("sapCentroCosto", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_centro_costo));
    insertCabeceraReq.input("sapCuentaMayor", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_cuenta_mayor));
    // sap_guia_remision no aplica en flete manual (sin origen SAP)
    insertCabeceraReq.input("guiaRemision", sql.Char(25), guiaRemision ? guiaRemision.slice(0, 25) : null);
    insertCabeceraReq.input("numeroEntrega", sql.VarChar(20), numeroEntrega ? numeroEntrega.slice(0, 20) : null);
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), tipoMovimiento);
    insertCabeceraReq.input("estado", sql.VarChar(20), estado);
    insertCabeceraReq.input("fechaSalida", sql.Date, fechaSalida);
    insertCabeceraReq.input("horaSalida", sql.VarChar(8), horaSalida);
    insertCabeceraReq.input("montoAplicado", sql.Decimal(18, 2), montoAplicado);
    insertCabeceraReq.input("idMovil", sql.BigInt, idMovil);
    insertCabeceraReq.input("idTarifa", sql.BigInt, idTarifa);
    insertCabeceraReq.input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones));
    insertCabeceraReq.input("idUsuarioCreador", sql.BigInt, parseOptionalBigInt(cabeceraIn.id_usuario_creador));
    insertCabeceraReq.input("idTipoFlete", sql.BigInt, idTipoFlete);
    insertCabeceraReq.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("idCentroCosto", sql.BigInt, idCentroCosto);

    const cabeceraResult = await insertCabeceraReq.query(`
      INSERT INTO [cfl].[CFL_cabecera_flete] (
        [id_detalle_viaje],
        [id_folio],
        [sap_numero_entrega],
        [sap_codigo_tipo_flete],
        [sap_centro_costo],
        [sap_cuenta_mayor],
        [guia_remision],
        [numero_entrega],
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
        [id_centro_costo]
      )
      OUTPUT INSERTED.id_cabecera_flete
      VALUES (
        @idDetalleViaje,
        @idFolio,
        @sapNumeroEntrega,
        @sapCodigoTipoFlete,
        @sapCentroCosto,
        @sapCuentaMayor,
        @guiaRemision,
        @numeroEntrega,
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
        @idCentroCosto
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
  const idCentroCosto = parseRequiredBigInt(cabeceraIn.id_centro_costo);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const idDetalleViajeIn = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idFolioIn = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifaIn = parseOptionalBigInt(cabeceraIn.id_tarifa);
  // cuenta_mayor_final eliminado (redundante con id_cuenta_mayor FK)
  const guiaRemisionIn   = toNullableTrimmedString(cabeceraIn.guia_remision);
  const numeroEntregaIn  = toNullableTrimmedString(cabeceraIn.numero_entrega);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCosto) {
    res.status(400).json({ error: "Falta id_centro_costo" });
    return;
  }
  if (!tipoMovimiento) {
    res.status(400).json({ error: "tipo_movimiento invalido (Despacho/Retorno)" });
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
    const existingResult = await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .query(`
        SELECT TOP 1 *
        FROM [cfl].[CFL_cabecera_flete]
        WHERE id_cabecera_flete = @idCabecera;
      `);

    const existing = existingResult.recordset[0] || null;
    if (!existing) {
      await transaction.rollback();
      res.status(404).json({ error: "Cabecera no encontrada" });
      return;
    }

    if (normalizeLifecycleStatus(existing.estado) === LIFECYCLE_STATUS.FACTURADO) {
      await transaction.rollback();
      res.status(409).json({ error: "El flete FACTURADO no se puede modificar" });
      return;
    }

    const idDetalleViaje = idDetalleViajeIn ?? existing.id_detalle_viaje ?? null;
    const idFolio = idFolioIn ?? existing.id_folio ?? null;
    const idTarifa = idTarifaIn ?? existing.id_tarifa ?? null;
    const idMovil = await resolveMovilId(transaction, cabeceraIn, now, existing.id_movil ?? null);
    const lifecycleFolioId = await resolveFolioForLifecycle(transaction, idFolio);
    // cuenta_mayor_final eliminado; usar id_cuenta_mayor (FK) para la cuenta contable
    const guiaRemision  = guiaRemisionIn  ?? (existing.guia_remision  ? String(existing.guia_remision)  : null);
    const numeroEntrega = numeroEntregaIn ?? (existing.numero_entrega ? String(existing.numero_entrega) : null);
    const estado = deriveLifecycleStatus({
      requestedStatus,
      idFolio: lifecycleFolioId,
      idTipoFlete,
      idCentroCosto,
      idDetalleViaje,
      idMovil,
      idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .input("tipoMovimiento", sql.VarChar(4), tipoMovimiento)
      .input("estado", sql.VarChar(20), estado)
      .input("fechaSalida", sql.Date, fechaSalida)
      .input("horaSalida", sql.VarChar(8), horaSalida)
      .input("montoAplicado", sql.Decimal(18, 2), montoAplicado)
      .input("guiaRemision", sql.Char(25), guiaRemision ? guiaRemision.slice(0, 25) : null)
      .input("numeroEntrega", sql.VarChar(20), numeroEntrega ? numeroEntrega.slice(0, 20) : null)
      .input("idDetalleViaje", sql.BigInt, idDetalleViaje)
      .input("idFolio", sql.BigInt, idFolio)
      .input("idMovil", sql.BigInt, idMovil)
      .input("idTarifa", sql.BigInt, idTarifa)
      .input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones))
      .input("idTipoFlete", sql.BigInt, idTipoFlete)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .input("updatedAt", sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET
          tipo_movimiento = @tipoMovimiento,
          estado = @estado,
          fecha_salida = @fechaSalida,
          hora_salida = CAST(@horaSalida AS TIME),
          monto_aplicado = @montoAplicado,
          guia_remision = @guiaRemision,
          numero_entrega = @numeroEntrega,
          id_detalle_viaje = @idDetalleViaje,
          id_folio = @idFolio,
          id_movil = @idMovil,
          id_tarifa = @idTarifa,
          observaciones = @observaciones,
          id_tipo_flete = @idTipoFlete,
          id_centro_costo = @idCentroCosto,
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
