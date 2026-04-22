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
  resolveImputacionFlete,
} = require("../helpers");
const { applyTransportIntent } = require("../modules/cfl-flete-save-pipeline");
const { validate } = require("../middleware/validate.middleware");
const { requirePermission } = require("../middleware/authz.middleware");
const { fleteManualBody, fleteIdParam } = require("../schemas/fletes.schemas");
const { logger } = require("../logger");

const router = express.Router();

function parseFleteInput(cabeceraIn) {
  return {
    idTipoFlete: parseRequiredBigInt(cabeceraIn.id_tipo_flete),
    idCentroCostoInput: parseOptionalBigInt(cabeceraIn.id_centro_costo),
    idCuentaMayorInput: parseOptionalBigInt(cabeceraIn.id_cuenta_mayor),
    idImputacionFleteInput: parseOptionalBigInt(cabeceraIn.id_imputacion_flete),
    tipoMovimiento: normalizeTipoMovimiento(cabeceraIn.tipo_movimiento || "PUSH"),
    requestedStatus: normalizeLifecycleStatus(cabeceraIn.estado),
    fechaSalida: toNullableTrimmedString(cabeceraIn.fecha_salida),
    horaSalida: toNullableTrimmedString(cabeceraIn.hora_salida),
    montoAplicado: Number.isFinite(Number(cabeceraIn.monto_aplicado)) ? Number(cabeceraIn.monto_aplicado) : 0,
    montoExtra: Number.isFinite(Number(cabeceraIn.monto_extra)) ? Number(cabeceraIn.monto_extra) : 0,
    guiaRemision: toNullableTrimmedString(cabeceraIn.guia_remision),
    numeroEntrega: toNullableTrimmedString(cabeceraIn.numero_entrega),
    idDetalleViaje: parseOptionalBigInt(cabeceraIn.id_detalle_viaje),
    idProductor: parseOptionalBigInt(cabeceraIn.id_productor),
    idTarifa: parseOptionalBigInt(cabeceraIn.id_tarifa),
    sentidoFlete: toNullableTrimmedString(cabeceraIn.sentido_flete),
    idEspecie: parseOptionalBigInt(cabeceraIn.id_especie),
  };
}

function validateFleteInput(parsed, res) {
  if (!parsed.idTipoFlete) {
    res.status(400).json({ error: "Falta id_tipo_flete" });
    return false;
  }
  if (!parsed.idCentroCostoInput && !parsed.idImputacionFleteInput) {
    res.status(400).json({ error: "Falta id_centro_costo" });
    return false;
  }
  if (!parsed.tipoMovimiento) {
    res.status(400).json({ error: "tipo_movimiento invalido (Despacho/Retorno)" });
    return false;
  }
  if (!parsed.fechaSalida) {
    res.status(400).json({ error: "Falta fecha_salida (YYYY-MM-DD)" });
    return false;
  }
  if (!parsed.horaSalida) {
    res.status(400).json({ error: "Falta hora_salida (HH:MM[:SS])" });
    return false;
  }
  return true;
}

async function insertFleteDetalles(transaction, idCabeceraFlete, detallesIn, now) {
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
}

async function safeRollback(transaction) {
  if (!transaction) return;
  try {
    await transaction.rollback();
  } catch (rollbackError) {
    logger.error({ err: rollbackError.message }, "transaction rollback failed");
  }
}

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
      -- Hints de transporte desde SAP: permiten al modal sugerir empresa/chofer/camion
      -- incluso cuando el flete fue guardado sin IdMovil. SapIdFiscalChofer alimenta
      -- directamente el campo sap_id_fiscal del chofer pending_create (sin heurística
      -- de regex sobre el nombre).
      SapEmpresaTransporte = NULLIF(LTRIM(RTRIM(lk.SapEmpresaTransporte)), ''),
      SapNombreChofer = NULLIF(LTRIM(RTRIM(lk.SapNombreChofer)), ''),
      SapIdFiscalChofer = NULLIF(LTRIM(RTRIM(lk.SapIdFiscalChofer)), ''),
      SapPatente = NULLIF(LTRIM(RTRIM(lk.SapPatente)), ''),
      SapCarro = NULLIF(LTRIM(RTRIM(lk.SapCarro)), ''),
      -- Hints de transporte desde Romana (fletes que vienen de Recepcion).
      RomanaConductor = NULLIF(LTRIM(RTRIM(rc.Conductor)), ''),
      RomanaPatente = NULLIF(LTRIM(RTRIM(rc.Patente)), ''),
      RomanaCarro = NULLIF(LTRIM(RTRIM(rc.Carro)), ''),
      ProductorIdResuelto = COALESCE(cf.IdProductor, prod_sap.IdProductor),
      ProductorCodigoProveedor = COALESCE(prod_cf.CodigoProveedor, prod_sap.CodigoProveedor),
      ProductorRut = COALESCE(prod_cf.Rut, prod_sap.Rut),
      ProductorNombre = COALESCE(prod_cf.Nombre, prod_sap.Nombre),
      ProductorEmail = COALESCE(prod_cf.Email, prod_sap.Email),
      UsuarioCreadorUsername = usr_creador.Username,
      UsuarioCreadorEmail    = usr_creador.Email,
      UsuarioCreadorNombre   = usr_creador.Nombre,
      UsuarioCreadorApellido = usr_creador.Apellido
    FROM [cfl].[CabeceraFlete] cf
    LEFT JOIN [cfl].[Usuario] usr_creador ON usr_creador.IdUsuario = cf.IdUsuarioCreador
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
        re.NumeroPartida,
        re.GuiaDespacho,
        re.SistemaFuente
      FROM [cfl].[FleteRomanaEntrega] fre
      INNER JOIN [cfl].[RomanaEntrega] re ON re.IdRomanaEntrega = fre.IdRomanaEntrega
      WHERE fre.IdCabeceraFlete = cf.IdCabeceraFlete
      ORDER BY CASE WHEN fre.TipoRelacion = 'PRINCIPAL' THEN 0 ELSE 1 END, fre.IdFleteRomanaEntrega ASC
    ) romana_rel
    LEFT JOIN [cfl].[VW_RomanaCabeceraActual] rc
      ON rc.NumeroPartida = romana_rel.NumeroPartida
     AND rc.GuiaDespacho = romana_rel.GuiaDespacho
     AND rc.SistemaFuente = romana_rel.SistemaFuente
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

async function fetchRomanaCurrentDetalles(pool, idCabecera) {
  // Simétrico a fetchSapCurrentDetalles pero desde VW_RomanaDetalleActual. Se
  // usa como fallback cuando el flete está vinculado a RomanaEntrega y no a SAP.
  // IdEspecie se deriva directo del CodigoEspecie (el invariante del dominio
  // dice que Especie.IdEspecie === Number(CodigoEspecie) sin ceros a la izquierda).
  // Preservamos el IdEspecie guardado en DetalleFlete si existe (por row_num).
  const result = await pool.request().input("idCabecera", sql.BigInt, idCabecera).query(`
    ;WITH romana_source AS (
      SELECT
        fre.FechaCreacion AS BridgeFechaCreacion,
        re.IdRomanaEntrega,
        re.NumeroPartida,
        re.GuiaDespacho,
        re.SistemaFuente
      FROM [cfl].[FleteRomanaEntrega] fre
      INNER JOIN [cfl].[RomanaEntrega] re
        ON re.IdRomanaEntrega = fre.IdRomanaEntrega
      WHERE fre.IdCabeceraFlete = @idCabecera
    ),
    romana_positions AS (
      SELECT
        row_num = ROW_NUMBER() OVER (
          ORDER BY
            src.NumeroPartida ASC,
            src.GuiaDespacho ASC,
            TRY_CONVERT(INT, NULLIF(LTRIM(RTRIM(rd.Posicion)), '')),
            rd.Posicion ASC
        ),
        src.BridgeFechaCreacion,
        NumeroPartida = src.NumeroPartida,
        GuiaDespacho = src.GuiaDespacho,
        Posicion = NULLIF(LTRIM(RTRIM(rd.Posicion)), ''),
        Material = NULLIF(LTRIM(RTRIM(rd.Material)), ''),
        MaterialDescripcion = NULLIF(LTRIM(RTRIM(rd.MaterialDescripcion)), ''),
        CodigoEspecie = NULLIF(LTRIM(RTRIM(rd.CodigoEspecie)), ''),
        EspecieDescripcion = NULLIF(LTRIM(RTRIM(rd.EspecieDescripcion)), ''),
        CantidadSubEnvaseL = rd.CantidadSubEnvaseL,
        UnidadMedida = NULLIF(LTRIM(RTRIM(rd.UnidadMedida)), ''),
        PesoReal = rd.PesoReal,
        Lote = NULLIF(LTRIM(RTRIM(rd.Lote)), '')
      FROM romana_source src
      INNER JOIN [cfl].[VW_RomanaDetalleActual] rd
        ON rd.SistemaFuente = src.SistemaFuente
       AND rd.NumeroPartida = src.NumeroPartida
       AND rd.GuiaDespacho = src.GuiaDespacho
    ),
    existing_details AS (
      SELECT
        row_num = ROW_NUMBER() OVER (ORDER BY IdDetalleFlete ASC),
        IdEspecie
      FROM [cfl].[DetalleFlete]
      WHERE IdCabeceraFlete = @idCabecera
    )
    SELECT
      IdDetalleFlete = rp.row_num,
      IdCabeceraFlete = @idCabecera,
      -- Prioridad: IdEspecie guardado en DetalleFlete; si no, derivado del CodigoEspecie.
      IdEspecie = COALESCE(ed.IdEspecie, TRY_CONVERT(BIGINT, rp.CodigoEspecie)),
      Material = rp.Material,
      Descripcion = COALESCE(rp.MaterialDescripcion, rp.EspecieDescripcion),
      Cantidad = rp.CantidadSubEnvaseL,
      Unidad = CASE
        WHEN rp.UnidadMedida IS NULL THEN NULL
        ELSE LEFT(rp.UnidadMedida, 3)
      END,
      Peso = rp.PesoReal,
      FechaCreacion = rp.BridgeFechaCreacion,
      RomanaNumeroPartida = rp.NumeroPartida,
      RomanaGuiaDespacho = rp.GuiaDespacho,
      RomanaPosicion = rp.Posicion,
      RomanaLote = rp.Lote
    FROM romana_positions rp
    LEFT JOIN existing_details ed
      ON ed.row_num = rp.row_num
    ORDER BY rp.row_num ASC;
  `);

  return result.recordset;
}

async function fetchDetalles(pool, idCabecera) {
  const sapCurrentDetalles = await fetchSapCurrentDetalles(pool, idCabecera);
  if (sapCurrentDetalles.length > 0) {
    return sapCurrentDetalles;
  }

  const romanaCurrentDetalles = await fetchRomanaCurrentDetalles(pool, idCabecera);
  if (romanaCurrentDetalles.length > 0) {
    return romanaCurrentDetalles;
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

router.get("/:id_cabecera_flete", requirePermission("fletes.candidatos.view", "fletes.editar"), validate({ params: fleteIdParam }), async (req, res, next) => {
  const idCabecera = req.params.id_cabecera_flete;

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

router.post("/manual", requirePermission("fletes.crear"), validate({ body: fleteManualBody }), async (req, res, next) => {
  const { cabecera: cabeceraIn, detalles: detallesIn, transport: transportIntent } = req.body;
  const parsed = parseFleteInput(cabeceraIn);
  if (!validateFleteInput(parsed, res)) return;

  let transaction;

  try {
    const pool = await getPool();
    transaction = new sql.Transaction(pool);
    await transaction.begin();

    const now = new Date();

    // Transporte inteligente: crea/actualiza empresa/chofer/camion y opcionalmente
    // recalcula tarifa. Muta cabeceraIn con los ids resueltos y tarifa recalculada.
    const transportResult = await applyTransportIntent(transaction, { cabeceraIn, transportIntent, now });
    // Re-parsear para que parsed.idTarifa, parsed.montoAplicado reflejen los cambios
    // aplicados por applyTransportIntent (puede haber sobreescrito id_tarifa/monto_aplicado).
    Object.assign(parsed, parseFleteInput(cabeceraIn));

    const imputacion = await resolveImputacionFlete(transaction, {
      idTipoFlete: parsed.idTipoFlete,
      idCentroCosto: parsed.idCentroCostoInput,
      idCuentaMayor: parsed.idCuentaMayorInput,
      idImputacionFlete: parsed.idImputacionFleteInput,
    });
    const { idCentroCosto, idCuentaMayor, idImputacionFlete } = imputacion;

    // Defensa en profundidad: resolveImputacionFlete ya lanza error dominio
    // si no puede resolver una imputación completa. Este check nunca debería
    // disparar, pero garantiza invariantes antes del INSERT/UPDATE.
    if (!idCentroCosto || !idCuentaMayor || !idImputacionFlete) {
      await transaction.rollback();
      res.status(422).json({
        error: "No se pudo resolver la imputación completa del flete (tipo + centro + cuenta).",
      });
      return;
    }

    const idMovil = await resolveMovilId(transaction, cabeceraIn, now);
    const estado = deriveLifecycleStatus({
      requestedStatus: parsed.requestedStatus,
      idTipoFlete: parsed.idTipoFlete,
      idCentroCosto,
      idDetalleViaje: parsed.idDetalleViaje,
      idMovil,
      idTarifa: parsed.idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    const insertCabeceraReq = new sql.Request(transaction);
    insertCabeceraReq.input("idDetalleViaje", sql.BigInt, parsed.idDetalleViaje);
    insertCabeceraReq.input("sapNumeroEntrega", sql.VarChar(20), toNullableTrimmedString(cabeceraIn.sap_numero_entrega));
    insertCabeceraReq.input("sapCodigoTipoFlete", sql.Char(4), toNullableTrimmedString(cabeceraIn.sap_codigo_tipo_flete));
    insertCabeceraReq.input("sapCentroCosto", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_centro_costo));
    insertCabeceraReq.input("sapCuentaMayor", sql.Char(10), toNullableTrimmedString(cabeceraIn.sap_cuenta_mayor));
    insertCabeceraReq.input("idCuentaMayor", sql.BigInt, idCuentaMayor);
    insertCabeceraReq.input("idImputacionFlete", sql.BigInt, idImputacionFlete);
    insertCabeceraReq.input("idProductor", sql.BigInt, parsed.idProductor);
    insertCabeceraReq.input("guiaRemision", sql.Char(25), parsed.guiaRemision ? parsed.guiaRemision.slice(0, 25) : null);
    insertCabeceraReq.input("numeroEntrega", sql.VarChar(20), parsed.numeroEntrega ? parsed.numeroEntrega.slice(0, 20) : null);
    insertCabeceraReq.input("tipoMovimiento", sql.VarChar(4), parsed.tipoMovimiento);
    insertCabeceraReq.input("sentidoFlete", sql.VarChar(20), parsed.sentidoFlete ? parsed.sentidoFlete.slice(0, 20) : null);
    insertCabeceraReq.input("estado", sql.VarChar(20), estado);
    insertCabeceraReq.input("fechaSalida", sql.Date, parsed.fechaSalida);
    insertCabeceraReq.input("horaSalida", sql.VarChar(8), parsed.horaSalida);
    insertCabeceraReq.input("montoAplicado", sql.Decimal(18, 2), parsed.montoAplicado);
    insertCabeceraReq.input("montoExtra", sql.Decimal(18, 2), parsed.montoExtra);
    insertCabeceraReq.input("idMovil", sql.BigInt, idMovil);
    insertCabeceraReq.input("idTarifa", sql.BigInt, parsed.idTarifa);
    insertCabeceraReq.input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones));
    // Creador derivado del token. Nunca se acepta del body: el cliente no puede
    // decidir quién "creó" el flete; lo dicta la sesión autenticada.
    insertCabeceraReq.input("idUsuarioCreador", sql.BigInt, parseOptionalBigInt(req.authnClaims?.id_usuario));
    insertCabeceraReq.input("idTipoFlete", sql.BigInt, parsed.idTipoFlete);
    insertCabeceraReq.input("createdAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("updatedAt", sql.DateTime2(0), now);
    insertCabeceraReq.input("idCentroCosto", sql.BigInt, idCentroCosto);
    insertCabeceraReq.input("idEspecieCab", sql.BigInt, parsed.idEspecie);

    const cabeceraResult = await insertCabeceraReq.query(`
      INSERT INTO [cfl].[CabeceraFlete] (
        [IdDetalleViaje],
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
        [MontoExtra],
        [IdMovil],
        [IdTarifa],
        [Observaciones],
        [IdUsuarioCreador],
        [IdTipoFlete],
        [FechaCreacion],
        [FechaActualizacion],
        [IdCuentaMayor],
        [IdImputacionFlete],
        [IdCentroCosto],
        [IdEspecie]
      )
      OUTPUT INSERTED.IdCabeceraFlete
      VALUES (
        @idDetalleViaje,
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
        @montoExtra,
        @idMovil,
        @idTarifa,
        @observaciones,
        @idUsuarioCreador,
        @idTipoFlete,
        @createdAt,
        @updatedAt,
        @idCuentaMayor,
        @idImputacionFlete,
        @idCentroCosto,
        @idEspecieCab
      );
    `);

    const idCabeceraFlete = cabeceraResult.recordset[0].IdCabeceraFlete;

    await insertFleteDetalles(transaction, idCabeceraFlete, detallesIn, now);

    await transaction.commit();

    res.status(201).json({
      message: "Flete manual creado",
      data: {
        id_cabecera_flete: idCabeceraFlete,
        resolved: {
          id_empresa_transporte: cabeceraIn.id_empresa_transporte ?? null,
          id_chofer: cabeceraIn.id_chofer ?? null,
          id_camion: cabeceraIn.id_camion ?? null,
          id_tarifa: parsed.idTarifa,
          id_movil: idMovil,
        },
        warnings: transportResult.warnings,
        tipo_camion_changed: transportResult.tipoCamionChanged,
      },
    });
  } catch (error) {
    await safeRollback(transaction);
    next(error);
  }
});

router.put("/:id_cabecera_flete", requirePermission("fletes.editar"), validate({ params: fleteIdParam, body: fleteManualBody }), async (req, res, next) => {
  const idCabecera = req.params.id_cabecera_flete;
  const { cabecera: cabeceraIn, detalles: detallesIn, transport: transportIntent } = req.body;
  const parsed = parseFleteInput(cabeceraIn);
  if (!validateFleteInput(parsed, res)) return;

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

    const estadoActual = normalizeLifecycleStatus(existing.Estado);
    if (estadoActual === LIFECYCLE_STATUS.FACTURADO || estadoActual === LIFECYCLE_STATUS.PREFACTURADO) {
      await transaction.rollback();
      res.status(409).json({ error: `El flete ${estadoActual} no se puede modificar` });
      return;
    }

    // Transporte inteligente: crea/actualiza empresa/chofer/camion y opcionalmente
    // recalcula tarifa. Muta cabeceraIn.
    const transportResult = await applyTransportIntent(transaction, { cabeceraIn, transportIntent, now });
    Object.assign(parsed, parseFleteInput(cabeceraIn));

    const idDetalleViaje = parsed.idDetalleViaje ?? existing.IdDetalleViaje ?? null;
    const idProductor = parsed.idProductor ?? existing.IdProductor ?? null;
    // Si applyTransportIntent recalculo tarifa, parsed.idTarifa ya refleja el nuevo valor
    // (o null si no hubo match). Si no recalculo, cae al existing.IdTarifa como fallback.
    const recalculoTarifa = transportIntent?.recalc_tarifa === true || transportResult.tipoCamionChanged;
    const idTarifa = recalculoTarifa ? parsed.idTarifa : (parsed.idTarifa ?? existing.IdTarifa ?? null);
    const imputacion = await resolveImputacionFlete(transaction, {
      idTipoFlete: parsed.idTipoFlete,
      idCentroCosto: parsed.idCentroCostoInput ?? existing.IdCentroCosto ?? null,
      idCuentaMayor: parsed.idCuentaMayorInput ?? existing.IdCuentaMayor ?? null,
      idImputacionFlete: parsed.idImputacionFleteInput ?? existing.IdImputacionFlete ?? null,
    });
    const { idCentroCosto, idCuentaMayor, idImputacionFlete } = imputacion;

    // Defensa en profundidad: resolveImputacionFlete ya lanza error dominio
    // si no puede resolver una imputación completa. Este check nunca debería
    // disparar, pero garantiza invariantes antes del INSERT/UPDATE.
    if (!idCentroCosto || !idCuentaMayor || !idImputacionFlete) {
      await transaction.rollback();
      res.status(422).json({
        error: "No se pudo resolver la imputación completa del flete (tipo + centro + cuenta).",
      });
      return;
    }

    const idMovil = await resolveMovilId(transaction, cabeceraIn, now, existing.IdMovil ?? null);
    const guiaRemision  = parsed.guiaRemision  ?? (existing.GuiaRemision  ? String(existing.GuiaRemision)  : null);
    const numeroEntrega = parsed.numeroEntrega ?? (existing.NumeroEntrega ? String(existing.NumeroEntrega) : null);
    const sentidoFlete = parsed.sentidoFlete ?? (existing.SentidoFlete ? String(existing.SentidoFlete) : null);
    const estado = deriveLifecycleStatus({
      currentStatus: estadoActual,
      requestedStatus: parsed.requestedStatus,
      idTipoFlete: parsed.idTipoFlete,
      idCentroCosto,
      idDetalleViaje,
      idMovil,
      idTarifa,
      hasDetalles: detallesIn.length > 0,
    });

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .input("tipoMovimiento", sql.VarChar(4), parsed.tipoMovimiento)
      .input("estado", sql.VarChar(20), estado)
      .input("fechaSalida", sql.Date, parsed.fechaSalida)
      .input("horaSalida", sql.VarChar(8), parsed.horaSalida)
      .input("montoAplicado", sql.Decimal(18, 2), parsed.montoAplicado)
      .input("montoExtra", sql.Decimal(18, 2), parsed.montoExtra)
      .input("guiaRemision", sql.Char(25), guiaRemision ? guiaRemision.slice(0, 25) : null)
      .input("numeroEntrega", sql.VarChar(20), numeroEntrega ? numeroEntrega.slice(0, 20) : null)
      .input("idDetalleViaje", sql.BigInt, idDetalleViaje)
      .input("idMovil", sql.BigInt, idMovil)
      .input("idTarifa", sql.BigInt, idTarifa)
      .input("observaciones", sql.VarChar(200), toNullableTrimmedString(cabeceraIn.observaciones))
      .input("idTipoFlete", sql.BigInt, parsed.idTipoFlete)
      .input("idCuentaMayor", sql.BigInt, idCuentaMayor)
      .input("idImputacionFlete", sql.BigInt, idImputacionFlete)
      .input("idProductor", sql.BigInt, idProductor)
      .input("sentidoFlete", sql.VarChar(20), sentidoFlete ? sentidoFlete.slice(0, 20) : null)
      .input("idCentroCosto", sql.BigInt, idCentroCosto)
      .input("idEspecieCab", sql.BigInt, parsed.idEspecie ?? existing.IdEspecie ?? null)
      .input("updatedAt", sql.DateTime2(0), now)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET
          TipoMovimiento = @tipoMovimiento,
          Estado = @estado,
          FechaSalida = @fechaSalida,
          HoraSalida = CAST(@horaSalida AS TIME),
          MontoAplicado = @montoAplicado,
          MontoExtra = @montoExtra,
          GuiaRemision = @guiaRemision,
          NumeroEntrega = @numeroEntrega,
          IdDetalleViaje = @idDetalleViaje,
          IdMovil = @idMovil,
          IdTarifa = @idTarifa,
          Observaciones = @observaciones,
          IdTipoFlete = @idTipoFlete,
          IdCuentaMayor = @idCuentaMayor,
          IdImputacionFlete = @idImputacionFlete,
          IdProductor = @idProductor,
          SentidoFlete = @sentidoFlete,
          IdCentroCosto = @idCentroCosto,
          IdEspecie = @idEspecieCab,
          FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idCabecera;
      `);

    await new sql.Request(transaction)
      .input("idCabecera", sql.BigInt, idCabecera)
      .query(`
        DELETE FROM [cfl].[DetalleFlete]
        WHERE IdCabeceraFlete = @idCabecera;
      `);

    await insertFleteDetalles(transaction, idCabecera, detallesIn, now);

    await transaction.commit();

    const updatedCabecera = await fetchCabecera(pool, idCabecera);
    const updatedDetalles = await fetchDetalles(pool, idCabecera);

    res.json({
      message: "Flete actualizado",
      data: {
        cabecera: updatedCabecera,
        detalles: updatedDetalles,
        warnings: transportResult.warnings,
        tipo_camion_changed: transportResult.tipoCamionChanged,
      },
    });
  } catch (error) {
    await safeRollback(transaction);
    next(error);
  }
});

module.exports = {
  fletesRouter: router,
};
