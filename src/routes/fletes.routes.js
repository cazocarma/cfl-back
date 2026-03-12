const express = require("express");
const { getPool, sql } = require("../db");
const {
  toNullableTrimmedString,
  parseOptionalBigInt,
  parseRequiredBigInt,
  normalizeTipoMovimiento,
} = require("../utils/parse");
const {
  LIFECYCLE_STATUS,
  normalizeLifecycleStatus,
  deriveLifecycleStatus,
} = require("../utils/lifecycle");
const {
  resolveMovilId,
  resolveFolioForLifecycle,
  resolveImputacionFlete,
} = require("../helpers");

const router = express.Router();

async function fetchCabecera(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    SELECT TOP 1
      cf.*,
      mv.IdEmpresaTransporte,
      mv.IdChofer,
      mv.IdCamion,
      cam.IdTipoCamion,
      tfa.IdRuta,
      r.IdOrigenNodo,
      r.IdDestinoNodo,
      RutaNombre = NULLIF(LTRIM(RTRIM(r.NombreRuta)), ''),
      RutaOrigenNombre = NULLIF(LTRIM(RTRIM(no.Nombre)), ''),
      RutaDestinoNombre = NULLIF(LTRIM(RTRIM(nd.Nombre)), ''),
      TarifaMontoFijo = tfa.MontoFijo,
      TarifaMoneda = tfa.Moneda,
      SapDestinatario = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), ''),
      ProductorIdResuelto = COALESCE(cf.IdProductor, prod_sap.IdProductor),
      ProductorCodigoProveedor = COALESCE(prod_cf.CodigoProveedor, prod_sap.CodigoProveedor),
      ProductorRut = COALESCE(prod_cf.Rut, prod_sap.Rut),
      ProductorNombre = COALESCE(prod_cf.Nombre, prod_sap.Nombre),
      ProductorEmail = COALESCE(prod_cf.Email, prod_sap.Email)
    FROM [cfl].[CabeceraFlete] cf
    LEFT JOIN [cfl].[Movil] mv ON mv.IdMovil = cf.IdMovil
    LEFT JOIN [cfl].[Camion] cam ON cam.IdCamion = mv.IdCamion
    LEFT JOIN [cfl].[Tarifa] tfa ON tfa.IdTarifa = cf.IdTarifa
    LEFT JOIN [cfl].[Ruta] r ON r.IdRuta = tfa.IdRuta
    LEFT JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo
    LEFT JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo
    LEFT JOIN [cfl].[Productor] prod_cf ON prod_cf.IdProductor = cf.IdProductor
    OUTER APPLY (
      SELECT TOP 1
        e.SapNumeroEntrega,
        e.SistemaFuente
      FROM [cfl].[FleteSapEntrega] fe
      INNER JOIN [cfl].[SapEntrega] e ON e.IdSapEntrega = fe.IdSapEntrega
      WHERE fe.IdCabeceraFlete = cf.IdCabeceraFlete
      ORDER BY CASE WHEN fe.TipoRelacion = 'PRINCIPAL' THEN 0 ELSE 1 END, fe.IdFleteSapEntrega ASC
    ) sap_rel
    LEFT JOIN [cfl].[VW_LikpActual] lk
      ON lk.SapNumeroEntrega = sap_rel.SapNumeroEntrega
     AND lk.SistemaFuente = sap_rel.SistemaFuente
    OUTER APPLY (
      SELECT TOP 1
        p.IdProductor,
        p.CodigoProveedor,
        p.Rut,
        p.Nombre,
        p.Email
      FROM [cfl].[Productor] p
      WHERE
        NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
        OR NULLIF(LTRIM(RTRIM(p.Rut)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '')
      ORDER BY
        CASE WHEN p.Activo = 1 THEN 0 ELSE 1 END,
        CASE WHEN NULLIF(LTRIM(RTRIM(p.CodigoProveedor)), '') = NULLIF(LTRIM(RTRIM(lk.SapDestinatario)), '') THEN 0 ELSE 1 END,
        p.IdProductor ASC
    ) prod_sap
    WHERE cf.IdCabeceraFlete = @idCabecera;
  `);

  return result.recordset[0] || null;
}

async function fetchSapCurrentDetalles(pool, idCabecera) {
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    ;WITH sap_source AS (
      SELECT
        fe.FechaCreacion AS BridgeFechaCreacion,
        e.IdSapEntrega,
        e.SapNumeroEntrega,
        e.SistemaFuente
      FROM [cfl].[FleteSapEntrega] fe
      INNER JOIN [cfl].[SapEntrega] e
        ON e.IdSapEntrega = fe.IdSapEntrega
      WHERE fe.IdCabeceraFlete = @idCabecera
    ),
    sap_positions AS (
      SELECT
        row_num = ROW_NUMBER() OVER (
          ORDER BY
            src.SapNumeroEntrega ASC,
            TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(lp.SapPosicion)), '')),
            lp.SapPosicion ASC
        ),
        src.BridgeFechaCreacion,
        SapNumeroEntrega = src.SapNumeroEntrega,
        SapPosicion = NULLIF(LTRIM(RTRIM(lp.SapPosicion)), ''),
        SapMaterial = NULLIF(LTRIM(RTRIM(lp.SapMaterial)), ''),
        SapDenominacionMaterial = NULLIF(LTRIM(RTRIM(lp.SapDenominacionMaterial)), ''),
        SapCantidadEntregada = TRY_CONVERT(DECIMAL(12, 2), lp.SapCantidadEntregada),
        SapUnidadPeso = NULLIF(LTRIM(RTRIM(lp.SapUnidadPeso)), ''),
        SapPosicionSuperior = NULLIF(LTRIM(RTRIM(lp.SapPosicionSuperior)), ''),
        SapLote = NULLIF(LTRIM(RTRIM(lp.SapLote)), '')
      FROM sap_source src
      INNER JOIN [cfl].[VW_LipsActual] lp
        ON lp.SistemaFuente = src.SistemaFuente
       AND lp.SapNumeroEntrega = src.SapNumeroEntrega
    ),
    existing_details AS (
      SELECT
        row_num = ROW_NUMBER() OVER (ORDER BY IdDetalleFlete ASC),
        IdEspecie
      FROM [cfl].[DetalleFlete]
      WHERE IdCabeceraFlete = @idCabecera
    )
    SELECT
      IdDetalleFlete = sp.row_num,
      IdCabeceraFlete = @idCabecera,
      IdEspecie = ed.IdEspecie,
      Material = sp.SapMaterial,
      Descripcion = sp.SapDenominacionMaterial,
      Cantidad = sp.SapCantidadEntregada,
      Unidad = CASE
        WHEN sp.SapUnidadPeso IS NULL THEN NULL
        ELSE LEFT(sp.SapUnidadPeso, 3)
      END,
      Peso = CASE
        WHEN UPPER(COALESCE(sp.SapUnidadPeso, '')) LIKE 'KG%' THEN TRY_CONVERT(DECIMAL(15, 3), sp.SapCantidadEntregada)
        ELSE NULL
      END,
      FechaCreacion = sp.BridgeFechaCreacion,
      SapNumeroEntrega = sp.SapNumeroEntrega,
      SapPosicion = sp.SapPosicion,
      SapPosicionSuperior = sp.SapPosicionSuperior,
      SapLote = sp.SapLote
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
      IdDetalleFlete,
      IdCabeceraFlete,
      IdEspecie,
      Material,
      Descripcion,
      Cantidad,
      Unidad,
      Peso,
      FechaCreacion
    FROM [cfl].[DetalleFlete]
    WHERE IdCabeceraFlete = @idCabecera
    ORDER BY IdDetalleFlete ASC;
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
  const idCentroCostoInput = parseOptionalBigInt(cabeceraIn.id_centro_costo);
  const idCuentaMayorInput = parseOptionalBigInt(cabeceraIn.id_cuenta_mayor);
  const idImputacionFleteInput = parseOptionalBigInt(cabeceraIn.id_imputacion_flete);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const guiaRemision = toNullableTrimmedString(cabeceraIn.guia_remision);
  const numeroEntrega = toNullableTrimmedString(cabeceraIn.numero_entrega);
  const idDetalleViaje = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idProductor = parseOptionalBigInt(cabeceraIn.id_productor);
  const idFolio = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifa = parseOptionalBigInt(cabeceraIn.id_tarifa);
  const sentidoFlete = toNullableTrimmedString(cabeceraIn.sentido_flete);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoInput && !idImputacionFleteInput) {
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
    const imputacion = await resolveImputacionFlete(transaction, {
      idTipoFlete,
      idCentroCosto: idCentroCostoInput,
      idCuentaMayor: idCuentaMayorInput,
      idImputacionFlete: idImputacionFleteInput,
    });
    const idCentroCosto = imputacion.idCentroCosto;
    const idCuentaMayor = imputacion.idCuentaMayor;
    const idImputacionFlete = imputacion.idImputacionFlete;

    if (!idCentroCosto) {
      await transaction.rollback();
      res.status(422).json({ error: "No se pudo resolver id_centro_costo para la cabecera de flete" });
      return;
    }

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
    insertCabeceraReq.input("idCuentaMayor", sql.BigInt, idCuentaMayor);
    insertCabeceraReq.input("idImputacionFlete", sql.BigInt, idImputacionFlete);
    insertCabeceraReq.input("idProductor", sql.BigInt, idProductor);
    insertCabeceraReq.input("guiaRemision", sql.Char(25), guiaRemision ? guiaRemision.slice(0, 25) : null);
    insertCabeceraReq.input("numeroEntrega", sql.VarChar(20), numeroEntrega ? numeroEntrega.slice(0, 20) : null);
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), tipoMovimiento);
    insertCabeceraReq.input("sentidoFlete", sql.VarChar(20), sentidoFlete ? sentidoFlete.slice(0, 20) : null);
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
      INSERT INTO [cfl].[CabeceraFlete] (
        [IdDetalleViaje],
        [IdFolio],
        [SapNumeroEntrega],
        [SapCodigoTipoFlete],
        [SapCentroCosto],
        [SapCuentaMayor],
        [IdProductor],
        [GuiaRemision],
        [NumeroEntrega],
        [TipoMovimiento],
        [SentidoFlete],
        [Estado],
        [FechaSalida],
        [HoraSalida],
        [MontoAplicado],
        [IdMovil],
        [IdTarifa],
        [Observaciones],
        [IdUsuarioCreador],
        [IdTipoFlete],
        [FechaCreacion],
        [FechaActualizacion],
        [IdCuentaMayor],
        [IdImputacionFlete],
        [IdCentroCosto]
      )
      OUTPUT INSERTED.IdCabeceraFlete
      VALUES (
        @idDetalleViaje,
        @idFolio,
        @sapNumeroEntrega,
        @sapCodigoTipoFlete,
        @sapCentroCosto,
        @sapCuentaMayor,
        @idProductor,
        @guiaRemision,
        @numeroEntrega,
        @tipoMovimiento,
        @sentidoFlete,
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
        @idCuentaMayor,
        @idImputacionFlete,
        @idCentroCosto
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].IdCabeceraFlete;

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
          INSERT INTO [cfl].[DetalleFlete] (
            [IdCabeceraFlete],
            [IdEspecie],
            [Material],
            [Descripcion],
            [Cantidad],
            [Unidad],
            [Peso],
            [FechaCreacion]
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
  const idCentroCostoInput = parseOptionalBigInt(cabeceraIn.id_centro_costo);
  const idCuentaMayorInput = parseOptionalBigInt(cabeceraIn.id_cuenta_mayor);
  const idImputacionFleteInput = parseOptionalBigInt(cabeceraIn.id_imputacion_flete);
  const tipoMovimiento = normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH");
  const requestedStatus = normalizeLifecycleStatus(cabeceraIn.estado);
  const fechaSalida = toNullableTrimmedString(cabeceraIn.fecha_salida);
  const horaSalida = toNullableTrimmedString(cabeceraIn.hora_salida);
  const montoAplicadoRaw = cabeceraIn.monto_aplicado;
  const montoAplicado = Number.isFinite(Number(montoAplicadoRaw)) ? Number(montoAplicadoRaw) : 0;
  const idDetalleViajeIn = parseOptionalBigInt(cabeceraIn.id_detalle_viaje);
  const idProductorIn = parseOptionalBigInt(cabeceraIn.id_productor);
  const idFolioIn = parseOptionalBigInt(cabeceraIn.id_folio);
  const idTarifaIn = parseOptionalBigInt(cabeceraIn.id_tarifa);
  const guiaRemisionIn = toNullableTrimmedString(cabeceraIn.guia_remision);
  const numeroEntregaIn = toNullableTrimmedString(cabeceraIn.numero_entrega);
  const sentidoFleteIn = toNullableTrimmedString(cabeceraIn.sentido_flete);

  if (!idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return;
  }
  if (!idCentroCostoInput && !idImputacionFleteInput) {
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
        FROM [cfl].[CabeceraFlete]
        WHERE IdCabeceraFlete = @idCabecera;
      `);

    const existing = existingResult.recordset[0] || null;
    if (!existing) {
      await transaction.rollback();
      res.status(404).json({ error: "Cabecera no encontrada" });
      return;
    }

    if (normalizeLifecycleStatus(existing.Estado) === LIFECYCLE_STATUS.FACTURADO) {
      await transaction.rollback();
      res.status(409).json({ error: "El flete FACTURADO no se puede modificar" });
      return;
    }

    const idDetalleViaje = idDetalleViajeIn ?? existing.IdDetalleViaje ?? null;
    const idProductor = idProductorIn ?? existing.IdProductor ?? null;
    const idFolio = idFolioIn ?? existing.IdFolio ?? null;
    const idTarifa = idTarifaIn ?? existing.IdTarifa ?? null;
    const imputacion = await resolveImputacionFlete(transaction, {
      idTipoFlete,
      idCentroCosto: idCentroCostoInput ?? existing.IdCentroCosto ?? null,
      idCuentaMayor: idCuentaMayorInput ?? existing.IdCuentaMayor ?? null,
      idImputacionFlete: idImputacionFleteInput ?? existing.IdImputacionFlete ?? null,
    });
    const idCentroCosto = imputacion.idCentroCosto;
    const idCuentaMayor = imputacion.idCuentaMayor;
    const idImputacionFlete = imputacion.idImputacionFlete;

    if (!idCentroCosto) {
      await transaction.rollback();
      res.status(422).json({ error: "No se pudo resolver id_centro_costo para la cabecera de flete" });
      return;
    }

    const idMovil = await resolveMovilId(transaction, cabeceraIn, now, existing.IdMovil ?? null);
    const lifecycleFolioId = await resolveFolioForLifecycle(transaction, idFolio);
    const guiaRemision  = guiaRemisionIn  ?? (existing.GuiaRemision  ? String(existing.GuiaRemision)  : null);
    const numeroEntrega = numeroEntregaIn ?? (existing.NumeroEntrega ? String(existing.NumeroEntrega) : null);
    const sentidoFlete = sentidoFleteIn ?? (existing.SentidoFlete ? String(existing.SentidoFlete) : null);
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
      .input("idCuentaMayor", sql.BigInt, idCuentaMayor)
      .input("idImputacionFlete", sql.BigInt, idImputacionFlete)
      .input("idProductor", sql.BigInt, idProductor)
      .input("sentidoFlete", sql.VarChar(20), sentidoFlete ? sentidoFlete.slice(0, 20) : null)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .input("updatedAt", sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET
          TipoMovimiento = @tipoMovimiento,
          Estado = @estado,
          FechaSalida = @fechaSalida,
          HoraSalida = CAST(@horaSalida AS TIME),
          MontoAplicado = @montoAplicado,
          GuiaRemision = @guiaRemision,
          NumeroEntrega = @numeroEntrega,
          IdDetalleViaje = @idDetalleViaje,
          IdFolio = @idFolio,
          IdMovil = @idMovil,
          IdTarifa = @idTarifa,
          Observaciones = @observaciones,
          IdTipoFlete = @idTipoFlete,
          IdCuentaMayor = @idCuentaMayor,
          IdImputacionFlete = @idImputacionFlete,
          IdProductor = @idProductor,
          SentidoFlete = @sentidoFlete,
          IdCentroCosto = @idCentroCosto,
          FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idCabecera;
      `);

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .query(`
        DELETE FROM [cfl].[DetalleFlete]
        WHERE IdCabeceraFlete = @idCabecera;
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
          INSERT INTO [cfl].[DetalleFlete] (
            [IdCabeceraFlete],
            [IdEspecie],
            [Material],
            [Descripcion],
            [Cantidad],
            [Unidad],
            [Peso],
            [FechaCreacion]
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
