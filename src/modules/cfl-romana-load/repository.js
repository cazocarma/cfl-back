const crypto = require("crypto");
const { getPool, sql } = require("../../db");

function sha256Buffer(input) {
  return crypto.createHash("sha256").update(input || "", "utf8").digest();
}

function parseOdataDate(value) {
  if (!value) return null;
  const s = String(value).trim();
  const m = s.match(/\/Date\((\d+)\)\//);
  if (m) return new Date(Number(m[1]));
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

// ---------------------------------------------------------------------------
// Transform: OData → staging rows with hash
// ---------------------------------------------------------------------------

function transformCabeceraRows(rows, sourceSystem, executionId, extractedAt, createdAt) {
  return rows.map((r) => {
    const hashInput = [
      sourceSystem, r.numeroPartida, r.guiaDespacho, r.idRomana,
      r.tipoDocumento, r.estadoRomana, r.patente, r.carro, r.conductor,
      r.ordenCompra, r.codigoProductor, r.centro, r.temporada,
    ].join("|");

    return {
      ...r,
      idEjecucion: executionId,
      fechaExtraccion: extractedAt,
      sistemaFuente: sourceSystem,
      hashFila: sha256Buffer(hashInput),
      estadoFila: "ACTIVE",
      fechaCreacion: createdAt,
      fechaCreacionSap: parseOdataDate(r.fechaCreacionSap),
      fechaModificacionSap: parseOdataDate(r.fechaModificacionSap),
      peticionBorrado: r.peticionBorrado ? 1 : 0,
    };
  });
}

function transformDetalleRows(rows, sourceSystem, executionId, extractedAt, createdAt) {
  return rows.map((r) => {
    const hashInput = [
      sourceSystem, r.numeroPartida, r.guiaDespacho, r.posicion,
      r.material, r.lote, String(r.pesoReal || 0),
      r.unidadMedida, r.posicionOrdenCompra, r.codigoEspecie, r.especieDescripcion,
      r.centro, r.almacen,
    ].join("|");

    return {
      ...r,
      idEjecucion: executionId,
      fechaExtraccion: extractedAt,
      sistemaFuente: sourceSystem,
      hashFila: sha256Buffer(hashInput),
      estadoFila: "ACTIVE",
      fechaCreacion: createdAt,
      fechaCosecha: parseOdataDate(r.fechaCosecha),
    };
  });
}

// ---------------------------------------------------------------------------
// Bulk insert to staging
// ---------------------------------------------------------------------------

function addCabeceraColumns(table) {
  table.columns.add("IdEjecucion", sql.UniqueIdentifier, { nullable: false });
  table.columns.add("FechaExtraccion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SistemaFuente", sql.NVarChar(50), { nullable: false });
  table.columns.add("HashFila", sql.VarBinary(32), { nullable: false });
  table.columns.add("EstadoFila", sql.NVarChar(20), { nullable: false });
  table.columns.add("FechaCreacion", sql.DateTime2(0), { nullable: false });
  table.columns.add("IdRomana", sql.NVarChar(20), { nullable: false });
  table.columns.add("NumeroPartida", sql.NVarChar(20), { nullable: true });
  table.columns.add("GuiaDespacho", sql.NVarChar(30), { nullable: true });
  table.columns.add("TipoDocumento", sql.NVarChar(10), { nullable: true });
  table.columns.add("TipoDocumentoTexto", sql.NVarChar(40), { nullable: true });
  table.columns.add("EstadoRomana", sql.NVarChar(10), { nullable: true });
  table.columns.add("EstadoRomanaTexto", sql.NVarChar(40), { nullable: true });
  table.columns.add("Patente", sql.NVarChar(20), { nullable: true });
  table.columns.add("Carro", sql.NVarChar(20), { nullable: true });
  table.columns.add("Conductor", sql.NVarChar(80), { nullable: true });
  table.columns.add("CreadoPor", sql.NVarChar(12), { nullable: true });
  table.columns.add("CreadoPorNombre", sql.NVarChar(80), { nullable: true });
  table.columns.add("FechaCreacionSap", sql.Date, { nullable: true });
  table.columns.add("FechaModificacionSap", sql.Date, { nullable: true });
  table.columns.add("OrdenCompra", sql.NVarChar(20), { nullable: true });
  table.columns.add("CodigoProductor", sql.NVarChar(20), { nullable: true });
  table.columns.add("Centro", sql.NVarChar(10), { nullable: true });
  table.columns.add("CentroNombre", sql.NVarChar(40), { nullable: true });
  table.columns.add("PlantaDestino", sql.NVarChar(10), { nullable: true });
  table.columns.add("PlantaDestinoNombre", sql.NVarChar(40), { nullable: true });
  table.columns.add("AlmacenDestino", sql.NVarChar(10), { nullable: true });
  table.columns.add("AlmacenDestinoNombre", sql.NVarChar(40), { nullable: true });
  table.columns.add("Temporada", sql.NVarChar(10), { nullable: true });
  table.columns.add("CSG", sql.NVarChar(20), { nullable: true });
  table.columns.add("GuiaAlterna", sql.NVarChar(30), { nullable: true });
  table.columns.add("ProductorDescripcion", sql.NVarChar(80), { nullable: true });
  table.columns.add("ProductorDireccion", sql.NVarChar(150), { nullable: true });
  table.columns.add("ProductorComuna", sql.NVarChar(60), { nullable: true });
  table.columns.add("ProductorProvincia", sql.NVarChar(60), { nullable: true });
  table.columns.add("PeticionBorrado", sql.Bit, { nullable: false });
  table.columns.add("ActualizadoPor", sql.NVarChar(12), { nullable: true });
  table.columns.add("ActualizadoPorNombre", sql.NVarChar(80), { nullable: true });
}

function addCabeceraRow(table, r) {
  table.rows.add(
    r.idEjecucion, r.fechaExtraccion, r.sistemaFuente, r.hashFila, r.estadoFila, r.fechaCreacion,
    r.idRomana, r.numeroPartida, r.guiaDespacho,
    r.tipoDocumento, r.tipoDocumentoTexto, r.estadoRomana, r.estadoRomanaTexto,
    r.patente, r.carro, r.conductor, r.creadoPor, r.creadoPorNombre,
    r.fechaCreacionSap, r.fechaModificacionSap,
    r.ordenCompra, r.codigoProductor, r.centro, r.centroNombre,
    r.plantaDestino, r.plantaDestinoNombre, r.almacenDestino, r.almacenDestinoNombre,
    r.temporada, r.csg, r.guiaAlterna,
    r.productorDescripcion, r.productorDireccion, r.productorComuna, r.productorProvincia,
    r.peticionBorrado, r.actualizadoPor, r.actualizadoPorNombre
  );
}

function addDetalleColumns(table) {
  table.columns.add("IdEjecucion", sql.UniqueIdentifier, { nullable: false });
  table.columns.add("FechaExtraccion", sql.DateTime2(0), { nullable: false });
  table.columns.add("SistemaFuente", sql.NVarChar(50), { nullable: false });
  table.columns.add("HashFila", sql.VarBinary(32), { nullable: false });
  table.columns.add("EstadoFila", sql.NVarChar(20), { nullable: false });
  table.columns.add("FechaCreacion", sql.DateTime2(0), { nullable: false });
  table.columns.add("NumeroPartida", sql.NVarChar(20), { nullable: false });
  table.columns.add("GuiaDespacho", sql.NVarChar(30), { nullable: false });
  table.columns.add("Posicion", sql.NVarChar(10), { nullable: false });
  table.columns.add("Material", sql.NVarChar(40), { nullable: true });
  table.columns.add("MaterialDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("Lote", sql.NVarChar(20), { nullable: true });
  table.columns.add("PesoReal", sql.Decimal(15, 3), { nullable: false });
  table.columns.add("UnidadMedida", sql.NVarChar(5), { nullable: true });
  table.columns.add("Envase", sql.NVarChar(20), { nullable: true });
  table.columns.add("EnvaseDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("SubEnvase", sql.NVarChar(20), { nullable: true });
  table.columns.add("SubEnvaseDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("PosicionOrdenCompra", sql.NVarChar(10), { nullable: true });
  table.columns.add("CodigoEspecie", sql.NVarChar(10), { nullable: true });
  table.columns.add("EspecieDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("CodigoGrupoVariedad", sql.NVarChar(10), { nullable: true });
  table.columns.add("GrupoVariedadDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("CodigoManejo", sql.NVarChar(10), { nullable: true });
  table.columns.add("ManejoDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("Centro", sql.NVarChar(10), { nullable: true });
  table.columns.add("Almacen", sql.NVarChar(10), { nullable: true });
  table.columns.add("AlmacenDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("VariedadAgronomica", sql.NVarChar(10), { nullable: true });
  table.columns.add("VariedadAgronomicaDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("TipoVariedad", sql.NVarChar(10), { nullable: true });
  table.columns.add("TipoVariedadDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("TipoFrio", sql.NVarChar(10), { nullable: true });
  table.columns.add("TipoFrioDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("Destino", sql.NVarChar(10), { nullable: true });
  table.columns.add("DestinoDescripcion", sql.NVarChar(40), { nullable: true });
  table.columns.add("LineaProduccion", sql.NVarChar(20), { nullable: true });
  table.columns.add("FechaCosecha", sql.Date, { nullable: true });
  table.columns.add("PSA", sql.NVarChar(20), { nullable: true });
  table.columns.add("GGN", sql.NVarChar(20), { nullable: true });
  table.columns.add("SDP", sql.NVarChar(10), { nullable: true });
  table.columns.add("UnidadMadurez", sql.NVarChar(20), { nullable: true });
  table.columns.add("Cuartel", sql.NVarChar(20), { nullable: true });
  table.columns.add("ExportadorMP", sql.NVarChar(20), { nullable: true });
  table.columns.add("ExportadorMPDescripcion", sql.NVarChar(80), { nullable: true });
  table.columns.add("PesoPromedioEnvase", sql.NVarChar(20), { nullable: true });
  table.columns.add("PesoRealEnvase", sql.NVarChar(20), { nullable: true });
  table.columns.add("CantidadSubEnvaseL", sql.Decimal(15, 3), { nullable: true });
  table.columns.add("PesoEnvase", sql.Decimal(15, 3), { nullable: true });
  table.columns.add("PesoSubEnvase", sql.Decimal(15, 3), { nullable: true });
  table.columns.add("CantidadSubEnvaseV", sql.Decimal(15, 3), { nullable: true });
}

function addDetalleRow(table, r) {
  table.rows.add(
    r.idEjecucion, r.fechaExtraccion, r.sistemaFuente, r.hashFila, r.estadoFila, r.fechaCreacion,
    r.numeroPartida, r.guiaDespacho, r.posicion,
    r.material, r.materialDescripcion, r.lote, r.pesoReal, r.unidadMedida,
    r.envase, r.envaseDescripcion, r.subEnvase, r.subEnvaseDescripcion,
    r.posicionOrdenCompra, r.codigoEspecie, r.especieDescripcion,
    r.codigoGrupoVariedad, r.grupoVariedadDescripcion,
    r.codigoManejo, r.manejoDescripcion,
    r.centro, r.almacen, r.almacenDescripcion,
    r.variedadAgronomica, r.variedadAgronomicaDescripcion,
    r.tipoVariedad, r.tipoVariedadDescripcion,
    r.tipoFrio, r.tipoFrioDescripcion,
    r.destino, r.destinoDescripcion,
    r.lineaProduccion, r.fechaCosecha,
    r.psa, r.ggn, r.sdp, r.unidadMadurez, r.cuartel,
    r.exportadorMP, r.exportadorMPDescripcion,
    r.pesoPromedioEnvase, r.pesoRealEnvase,
    r.cantidadSubEnvaseL, r.pesoEnvase, r.pesoSubEnvase, r.cantidadSubEnvaseV
  );
}

async function bulkInsertStageRows(transaction, cabeceraRows, detalleRows) {
  if (cabeceraRows.length > 0) {
    const t = new sql.Table("[cfl].[StgRomanaCabecera]");
    t.create = false;
    addCabeceraColumns(t);
    for (const r of cabeceraRows) addCabeceraRow(t, r);
    await new sql.Request(transaction).bulk(t);
  }
  if (detalleRows.length > 0) {
    const t = new sql.Table("[cfl].[StgRomanaDetalle]");
    t.create = false;
    addDetalleColumns(t);
    for (const r of detalleRows) addDetalleRow(t, r);
    await new sql.Request(transaction).bulk(t);
  }
}

// ---------------------------------------------------------------------------
// Dedup: staging → raw
// ---------------------------------------------------------------------------

async function insertCabeceraDeduped(transaction, executionId) {
  const result = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .query(`
      ;WITH s AS (
        SELECT *, rn = ROW_NUMBER() OVER (PARTITION BY SistemaFuente, NumeroPartida, GuiaDespacho, HashFila ORDER BY (SELECT 1))
        FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid
      )
      INSERT INTO [cfl].[RomanaCabeceraRaw] (
        IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
        IdRomana, NumeroPartida, GuiaDespacho,
        TipoDocumento, TipoDocumentoTexto, EstadoRomana, EstadoRomanaTexto,
        Patente, Carro, Conductor, CreadoPor, CreadoPorNombre,
        FechaCreacionSap, FechaModificacionSap,
        OrdenCompra, CodigoProductor, Centro, CentroNombre,
        PlantaDestino, PlantaDestinoNombre, AlmacenDestino, AlmacenDestinoNombre,
        Temporada, CSG, GuiaAlterna,
        ProductorDescripcion, ProductorDireccion, ProductorComuna, ProductorProvincia,
        PeticionBorrado, ActualizadoPor, ActualizadoPorNombre
      )
      SELECT
        IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
        IdRomana, NumeroPartida, GuiaDespacho,
        TipoDocumento, TipoDocumentoTexto, EstadoRomana, EstadoRomanaTexto,
        Patente, Carro, Conductor, CreadoPor, CreadoPorNombre,
        FechaCreacionSap, FechaModificacionSap,
        OrdenCompra, CodigoProductor, Centro, CentroNombre,
        PlantaDestino, PlantaDestinoNombre, AlmacenDestino, AlmacenDestinoNombre,
        Temporada, CSG, GuiaAlterna,
        ProductorDescripcion, ProductorDireccion, ProductorComuna, ProductorProvincia,
        PeticionBorrado, ActualizadoPor, ActualizadoPorNombre
      FROM s
      WHERE rn = 1
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[RomanaCabeceraRaw] t
          WHERE t.SistemaFuente = s.SistemaFuente
            AND t.NumeroPartida = s.NumeroPartida AND t.GuiaDespacho = s.GuiaDespacho
            AND t.HashFila = s.HashFila
        );
      SELECT affected = @@ROWCOUNT;
    `);
  return result.recordset[0]?.affected || 0;
}

async function insertDetalleDeduped(transaction, executionId) {
  const result = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .query(`
      ;WITH s AS (
        SELECT *, rn = ROW_NUMBER() OVER (PARTITION BY SistemaFuente, NumeroPartida, GuiaDespacho, Posicion, HashFila ORDER BY (SELECT 1))
        FROM [cfl].[StgRomanaDetalle] WHERE IdEjecucion = @eid
      )
      INSERT INTO [cfl].[RomanaDetalleRaw] (
        IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
        NumeroPartida, GuiaDespacho, Posicion,
        Material, MaterialDescripcion, Lote, PesoReal, UnidadMedida,
        Envase, EnvaseDescripcion, SubEnvase, SubEnvaseDescripcion,
        PosicionOrdenCompra, CodigoEspecie, EspecieDescripcion,
        CodigoGrupoVariedad, GrupoVariedadDescripcion, CodigoManejo, ManejoDescripcion,
        Centro, Almacen, AlmacenDescripcion,
        VariedadAgronomica, VariedadAgronomicaDescripcion,
        TipoVariedad, TipoVariedadDescripcion, TipoFrio, TipoFrioDescripcion,
        Destino, DestinoDescripcion, LineaProduccion, FechaCosecha,
        PSA, GGN, SDP, UnidadMadurez, Cuartel,
        ExportadorMP, ExportadorMPDescripcion,
        PesoPromedioEnvase, PesoRealEnvase,
        CantidadSubEnvaseL, PesoEnvase, PesoSubEnvase, CantidadSubEnvaseV
      )
      SELECT
        IdEjecucion, FechaExtraccion, SistemaFuente, HashFila, EstadoFila, FechaCreacion,
        NumeroPartida, GuiaDespacho, Posicion,
        Material, MaterialDescripcion, Lote, PesoReal, UnidadMedida,
        Envase, EnvaseDescripcion, SubEnvase, SubEnvaseDescripcion,
        PosicionOrdenCompra, CodigoEspecie, EspecieDescripcion,
        CodigoGrupoVariedad, GrupoVariedadDescripcion, CodigoManejo, ManejoDescripcion,
        Centro, Almacen, AlmacenDescripcion,
        VariedadAgronomica, VariedadAgronomicaDescripcion,
        TipoVariedad, TipoVariedadDescripcion, TipoFrio, TipoFrioDescripcion,
        Destino, DestinoDescripcion, LineaProduccion, FechaCosecha,
        PSA, GGN, SDP, UnidadMadurez, Cuartel,
        ExportadorMP, ExportadorMPDescripcion,
        PesoPromedioEnvase, PesoRealEnvase,
        CantidadSubEnvaseL, PesoEnvase, PesoSubEnvase, CantidadSubEnvaseV
      FROM s
      WHERE rn = 1
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[RomanaDetalleRaw] t
          WHERE t.SistemaFuente = s.SistemaFuente
            AND t.NumeroPartida = s.NumeroPartida AND t.GuiaDespacho = s.GuiaDespacho
            AND t.Posicion = s.Posicion AND t.HashFila = s.HashFila
        );
      SELECT affected = @@ROWCOUNT;
    `);
  return result.recordset[0]?.affected || 0;
}

// ---------------------------------------------------------------------------
// Build canonical
// ---------------------------------------------------------------------------

async function buildCanonical(transaction, executionId, nowUtc) {
  const entregaInserted = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      INSERT INTO [cfl].[RomanaEntrega] (NumeroPartida, GuiaDespacho, SistemaFuente, FechaCreacion, FechaActualizacion)
      SELECT DISTINCT s.NumeroPartida, s.GuiaDespacho, s.SistemaFuente, @now, @now
      FROM [cfl].[StgRomanaCabecera] s
      WHERE s.IdEjecucion = @eid
        AND NOT EXISTS (
          SELECT 1 FROM [cfl].[RomanaEntrega] e
          WHERE e.SistemaFuente = s.SistemaFuente AND e.NumeroPartida = s.NumeroPartida AND e.GuiaDespacho = s.GuiaDespacho
        );
      SELECT affected = @@ROWCOUNT;
    `);

  const histInserted = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      INSERT INTO [cfl].[RomanaEntregaHistorial] (IdRomanaEntrega, IdRomanaCabeceraRaw, IdEjecucion, FechaExtraccion, FechaCreacion)
      SELECT e.IdRomanaEntrega, raw.IdRomanaCabeceraRaw, raw.IdEjecucion, raw.FechaExtraccion, @now
      FROM [cfl].[RomanaCabeceraRaw] raw
      INNER JOIN (SELECT DISTINCT SistemaFuente, NumeroPartida, GuiaDespacho FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid) s
        ON s.SistemaFuente = raw.SistemaFuente AND s.NumeroPartida = raw.NumeroPartida AND s.GuiaDespacho = raw.GuiaDespacho
      INNER JOIN [cfl].[RomanaEntrega] e
        ON e.SistemaFuente = raw.SistemaFuente AND e.NumeroPartida = raw.NumeroPartida AND e.GuiaDespacho = raw.GuiaDespacho
      WHERE NOT EXISTS (SELECT 1 FROM [cfl].[RomanaEntregaHistorial] h WHERE h.IdRomanaCabeceraRaw = raw.IdRomanaCabeceraRaw);
      SELECT affected = @@ROWCOUNT;
    `);

  await new sql.Request(transaction).input("now", sql.DateTime2(0), nowUtc).query(`
    UPDATE e SET e.FechaActualizacion = @now
    FROM [cfl].[RomanaEntrega] e
    WHERE EXISTS (SELECT 1 FROM [cfl].[RomanaEntregaHistorial] h WHERE h.IdRomanaEntrega = e.IdRomanaEntrega AND h.FechaCreacion = @now);
  `);

  const posInserted = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      ;WITH x AS (
        SELECT DISTINCT e.IdRomanaEntrega, d.Posicion
        FROM [cfl].[RomanaDetalleRaw] d
        INNER JOIN (SELECT DISTINCT SistemaFuente, NumeroPartida, GuiaDespacho FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid) s
          ON s.SistemaFuente = d.SistemaFuente AND s.NumeroPartida = d.NumeroPartida AND s.GuiaDespacho = d.GuiaDespacho
        INNER JOIN [cfl].[RomanaEntrega] e
          ON e.SistemaFuente = d.SistemaFuente AND e.NumeroPartida = d.NumeroPartida AND e.GuiaDespacho = d.GuiaDespacho
      )
      INSERT INTO [cfl].[RomanaEntregaPosicion] (IdRomanaEntrega, Posicion, FechaCreacion, FechaActualizacion)
      SELECT x.IdRomanaEntrega, x.Posicion, @now, @now FROM x
      WHERE NOT EXISTS (SELECT 1 FROM [cfl].[RomanaEntregaPosicion] p WHERE p.IdRomanaEntrega = x.IdRomanaEntrega AND p.Posicion = x.Posicion);
      SELECT affected = @@ROWCOUNT;
    `);

  const posHistInserted = await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      INSERT INTO [cfl].[RomanaEntregaPosicionHistorial] (IdRomanaEntregaPosicion, IdRomanaDetalleRaw, IdEjecucion, FechaExtraccion, FechaCreacion)
      SELECT p.IdRomanaEntregaPosicion, d.IdRomanaDetalleRaw, d.IdEjecucion, d.FechaExtraccion, @now
      FROM [cfl].[RomanaDetalleRaw] d
      INNER JOIN (SELECT DISTINCT SistemaFuente, NumeroPartida, GuiaDespacho FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid) s
        ON s.SistemaFuente = d.SistemaFuente AND s.NumeroPartida = d.NumeroPartida AND s.GuiaDespacho = d.GuiaDespacho
      INNER JOIN [cfl].[RomanaEntrega] e
        ON e.SistemaFuente = d.SistemaFuente AND e.NumeroPartida = d.NumeroPartida AND e.GuiaDespacho = d.GuiaDespacho
      INNER JOIN [cfl].[RomanaEntregaPosicion] p
        ON p.IdRomanaEntrega = e.IdRomanaEntrega AND p.Posicion = d.Posicion
      WHERE NOT EXISTS (SELECT 1 FROM [cfl].[RomanaEntregaPosicionHistorial] h WHERE h.IdRomanaDetalleRaw = d.IdRomanaDetalleRaw);
      SELECT affected = @@ROWCOUNT;
    `);

  return {
    entregas_insertadas: entregaInserted.recordset[0]?.affected || 0,
    entregas_actualizadas: 0,
    entregas_historial_insertadas: histInserted.recordset[0]?.affected || 0,
    posiciones_insertadas: posInserted.recordset[0]?.affected || 0,
    posiciones_actualizadas: 0,
    posiciones_historial_insertadas: posHistInserted.recordset[0]?.affected || 0,
  };
}

// ---------------------------------------------------------------------------
// Restore discarded + cleanup
// ---------------------------------------------------------------------------

async function restoreDiscardedOnReimport(transaction, executionId, nowUtc) {
  await new sql.Request(transaction)
    .input("eid", sql.UniqueIdentifier, executionId)
    .input("now", sql.DateTime2(0), nowUtc)
    .query(`
      UPDATE sd SET sd.Activo = 0, sd.FechaRestauracion = @now, sd.FechaActualizacion = @now
      FROM [cfl].[RomanaEntregaDescarte] sd
      INNER JOIN [cfl].[RomanaEntrega] e ON e.IdRomanaEntrega = sd.IdRomanaEntrega
      INNER JOIN (SELECT DISTINCT SistemaFuente, NumeroPartida, GuiaDespacho FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid) s
        ON s.SistemaFuente = e.SistemaFuente AND s.NumeroPartida = e.NumeroPartida AND s.GuiaDespacho = e.GuiaDespacho
      WHERE sd.Activo = 1
        AND NOT EXISTS (SELECT 1 FROM [cfl].[FleteRomanaEntrega] fe WHERE fe.IdRomanaEntrega = e.IdRomanaEntrega);
    `);
}

async function cleanupStageRows(transaction, executionId) {
  await new sql.Request(transaction).input("eid", sql.UniqueIdentifier, executionId).query(`
    DELETE FROM [cfl].[StgRomanaCabecera] WHERE IdEjecucion = @eid;
    DELETE FROM [cfl].[StgRomanaDetalle] WHERE IdEjecucion = @eid;
  `);
}

// ---------------------------------------------------------------------------
// Main orchestrator
// ---------------------------------------------------------------------------

async function persistExtraction(jobId, sourceSystem, extraction) {
  const extractedAt = new Date();
  const createdAt = new Date();
  const stageCab = transformCabeceraRows(extraction.cabecera_rows, sourceSystem, jobId, extractedAt, createdAt);
  const stageDet = transformDetalleRows(extraction.detalle_rows, sourceSystem, jobId, extractedAt, createdAt);

  const pool = await getPool();
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    await bulkInsertStageRows(transaction, stageCab, stageDet);
    const cabInserted = await insertCabeceraDeduped(transaction, jobId);
    const detInserted = await insertDetalleDeduped(transaction, jobId);
    const canonical = await buildCanonical(transaction, jobId, createdAt);
    await restoreDiscardedOnReimport(transaction, jobId, createdAt);
    await cleanupStageRows(transaction, jobId);
    await transaction.commit();

    return {
      extracted_at: extractedAt.toISOString(),
      raw: { cabecera_rows_extracted: stageCab.length, detalle_rows_extracted: stageDet.length, cabecera_rows_inserted: cabInserted, detalle_rows_inserted: detInserted },
      canonical,
      totals: {
        filas_extraidas: stageCab.length + stageDet.length,
        filas_insertadas: cabInserted + detInserted + canonical.entregas_insertadas + canonical.entregas_historial_insertadas + canonical.posiciones_insertadas + canonical.posiciones_historial_insertadas,
        filas_actualizadas: 0,
        filas_sin_cambio: (stageCab.length - cabInserted) + (stageDet.length - detInserted),
      },
    };
  } catch (error) {
    try { await transaction.rollback(); } catch { /* no-op */ }
    throw error;
  }
}

module.exports = { persistExtraction };
