const express = require("express");
const bcrypt = require("bcryptjs");
const { getPool } = require("../db");
const { MAINTAINERS } = require("../mantenedores-config");
const { hasAnyPermission, resolveAuthContext } = require("../authz");
const { normalizeLifecycleStatus } = require("../helpers");

const router = express.Router();

function maintainerReadPermissions(entityKey) {
  return [
    "mantenedores.admin",
    "mantenedores.view",
    `mantenedores.view.${entityKey}`,
  ];
}

function maintainerWritePermissions(entityKey) {
  return [
    "mantenedores.admin",
    "mantenedores.edit",
    `mantenedores.edit.${entityKey}`,
  ];
}

function normalizePermissionEntityKey(entityKey) {
  if (entityKey === "transportistas") {
    return "empresas-transporte";
  }
  return entityKey;
}

function getEntityConfig(entity) {
  if (entity === "transportistas") {
    return MAINTAINERS["empresas-transporte"] || null;
  }
  return MAINTAINERS[entity] || null;
}

function toBool(value) {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return ["1", "true", "t", "yes", "si", "y"].includes(normalized);
  }

  return false;
}

function normalizeValue(fieldName, value) {
  if (value === undefined) {
    return value;
  }

  const lower = fieldName.toLowerCase();
  const isBooleanField =
    lower.startsWith("activo") ||
    lower.startsWith("activa") ||
    lower.startsWith("cerrada") ||
    lower.includes("requiere") ||
    lower === "bloqueado";

  if (isBooleanField) {
    return toBool(value);
  }

  return value;
}

function toSnakeCaseField(fieldName) {
  return String(fieldName || "")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[\s\-]+/g, "_")
    .toLowerCase();
}

function collectPayload(body, allowedFields) {
  const payload = {};

  for (const fieldName of allowedFields) {
    const snakeField = toSnakeCaseField(fieldName);
    const hasPascal = Object.prototype.hasOwnProperty.call(body, fieldName);
    const hasSnake = Object.prototype.hasOwnProperty.call(body, snakeField);

    if (hasPascal || hasSnake) {
      const rawValue = hasPascal ? body[fieldName] : body[snakeField];
      payload[fieldName] = normalizeValue(fieldName, rawValue);
    }
  }

  return payload;
}

function requireEntity(req, res) {
  const entity = req.params.entity;
  const entityConfig = getEntityConfig(entity);

  if (!entityConfig) {
    res.status(404).json({ error: `Mantenedor no soportado: ${entity}` });
    return null;
  }

  return entityConfig;
}

function parseEntityId(req, res, entityConfig) {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: `ID invalido para ${entityConfig.title}` });
    return null;
  }

  return id;
}

function buildBaseFrom(entityConfig) {
  if (entityConfig.from) {
    return entityConfig.from;
  }

  return `${entityConfig.table} ${entityConfig.alias}`;
}

function isInvalidObjectNameError(error) {
  if (!error || typeof error !== "object") return false;
  const number = Number(error.number || error.code || 0);
  const message = String(error.message || "");
  return number === 208 || /invalid object name/i.test(message);
}

async function fetchTiposFleteRows(pool) {
  const sqlNuevo = `
    SELECT
      t.IdTipoFlete,
      t.SapCodigo,
      t.Nombre,
      t.Activo,
      t.IdCentroCosto,
      cc.SapCodigo AS CentroCostoSapCodigo,
      cc.Nombre AS CentroCostoNombre
    FROM [cfl].[TipoFlete] t
    INNER JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = t.IdCentroCosto
    ORDER BY t.Nombre ASC;
  `;

  try {
    const result = await pool.request().query(sqlNuevo);
    return result.recordset;
  } catch (error) {
    if (!isInvalidObjectNameError(error)) {
      throw error;
    }
  }

  // Compatibilidad temporal para ambientes aun no migrados al modelo PascalCase
  const sqlLegado = `
    SELECT
      t.id_tipo_flete AS IdTipoFlete,
      t.sap_codigo AS SapCodigo,
      t.nombre AS Nombre,
      t.activo AS Activo,
      t.id_centro_costo AS IdCentroCosto,
      cc.sap_codigo AS CentroCostoSapCodigo,
      cc.nombre AS CentroCostoNombre
    FROM [cfl].[CFL_tipo_flete] t
    INNER JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = t.id_centro_costo
    ORDER BY t.nombre ASC;
  `;

  const legacyResult = await pool.request().query(sqlLegado);
  return legacyResult.recordset;
}

async function fetchFolioEstado(pool, idFolio) {
  const result = await pool
    .request()
    .input("idFolio", idFolio)
    .query(`
      SELECT TOP 1 Estado, FolioNumero, Bloqueado
      FROM [cfl].[Folio]
      WHERE IdFolio = @idFolio;
    `);
  return result.recordset[0] || null;
}

function isFolioBlocked(folio) {
  return folio?.Bloqueado === true || folio?.Bloqueado === 1;
}

async function resolveDefaultFolioId(pool) {
  const result = await pool.request().query(`
    SELECT TOP 1 IdFolio
    FROM [cfl].[Folio]
    WHERE LTRIM(RTRIM(CAST(FolioNumero AS NVARCHAR(50)))) = '0'
    ORDER BY IdFolio ASC;
  `);
  return result.recordset[0] ? Number(result.recordset[0].IdFolio) : null;
}

async function fetchEntityById(pool, entityConfig, id) {
  const sql = `
    SELECT ${entityConfig.listColumns.join(", ")}
    FROM ${buildBaseFrom(entityConfig)}
    WHERE ${entityConfig.alias}.${entityConfig.idColumn} = @id;
  `;

  const result = await pool.request().input("id", id).query(sql);
  return result.recordset[0] || null;
}

async function ensureCanWrite(req, res, entityKey) {
  const permissionEntityKey = normalizePermissionEntityKey(entityKey);
  const auth = await resolveAuthContext(req);
  if (hasAnyPermission(auth, maintainerWritePermissions(permissionEntityKey))) {
    return auth;
  }

  res.status(403).json({
    error: "No tienes permisos para modificar este mantenedor",
    role: auth?.primaryRole || null,
    entity: entityKey,
  });

  return null;
}

async function getRelationsForEntity(pool, entity, id) {
  if (entity === "choferes") {
    const sql = `
      SELECT
        m.IdMovil,
        m.Activo AS MovilActivo,
        m.FechaCreacion AS MovilFechaCreacion,
        m.FechaActualizacion AS MovilFechaActualizacion,
        e.IdEmpresa,
        e.SapCodigo AS EmpresaSapCodigo,
        e.Rut AS EmpresaRut,
        e.RazonSocial AS EmpresaRazonSocial,
        e.Activo AS EmpresaActiva,
        c.IdCamion,
        c.SapPatente,
        c.SapCarro,
        c.Activo AS CamionActivo,
        tc.IdTipoCamion,
        tc.Nombre AS TipoCamionNombre,
        MIN(cf.FechaSalida) AS PeriodoDesde,
        MAX(cf.FechaSalida) AS PeriodoHasta,
        COUNT(cf.IdCabeceraFlete) AS Viajes
      FROM [cfl].[Movil] m
      INNER JOIN [cfl].[EmpresaTransporte] e ON e.IdEmpresa = m.IdEmpresaTransporte
      INNER JOIN [cfl].[Camion] c ON c.IdCamion = m.IdCamion
      INNER JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = c.IdTipoCamion
      LEFT JOIN [cfl].[CabeceraFlete] cf ON cf.IdMovil = m.IdMovil
      WHERE m.IdChofer = @id
      GROUP BY
        m.IdMovil,
        m.Activo,
        m.FechaCreacion,
        m.FechaActualizacion,
        e.IdEmpresa,
        e.SapCodigo,
        e.Rut,
        e.RazonSocial,
        e.Activo,
        c.IdCamion,
        c.SapPatente,
        c.SapCarro,
        c.Activo,
        tc.IdTipoCamion,
        tc.Nombre
      ORDER BY e.RazonSocial ASC, c.SapPatente ASC, c.SapCarro ASC;
    `;

    const result = await pool.request().input("id", id).query(sql);
    return {
      mode: "chofer",
      rows: result.recordset,
    };
  }

  if (entity === "empresas-transporte" || entity === "transportistas") {
    const sql = `
      SELECT
        m.IdMovil,
        m.Activo AS MovilActivo,
        m.FechaCreacion AS MovilFechaCreacion,
        m.FechaActualizacion AS MovilFechaActualizacion,
        ch.IdChofer,
        ch.SapIdFiscal AS ChoferSapIdFiscal,
        ch.SapNombre AS ChoferNombre,
        ch.Telefono AS ChoferTelefono,
        ch.Activo AS ChoferActivo,
        c.IdCamion,
        c.SapPatente,
        c.SapCarro,
        c.Activo AS CamionActivo,
        tc.IdTipoCamion,
        tc.Nombre AS TipoCamionNombre,
        MIN(cf.FechaSalida) AS PeriodoDesde,
        MAX(cf.FechaSalida) AS PeriodoHasta,
        COUNT(cf.IdCabeceraFlete) AS Viajes
      FROM [cfl].[Movil] m
      INNER JOIN [cfl].[Chofer] ch ON ch.IdChofer = m.IdChofer
      INNER JOIN [cfl].[Camion] c ON c.IdCamion = m.IdCamion
      INNER JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = c.IdTipoCamion
      LEFT JOIN [cfl].[CabeceraFlete] cf ON cf.IdMovil = m.IdMovil
      WHERE m.IdEmpresaTransporte = @id
      GROUP BY
        m.IdMovil,
        m.Activo,
        m.FechaCreacion,
        m.FechaActualizacion,
        ch.IdChofer,
        ch.SapIdFiscal,
        ch.SapNombre,
        ch.Telefono,
        ch.Activo,
        c.IdCamion,
        c.SapPatente,
        c.SapCarro,
        c.Activo,
        tc.IdTipoCamion,
        tc.Nombre
      ORDER BY ch.SapNombre ASC, c.SapPatente ASC, c.SapCarro ASC;
    `;

    const result = await pool.request().input("id", id).query(sql);
    return {
      mode: "empresa",
      rows: result.recordset,
    };
  }

  if (entity === "camiones") {
    const sql = `
      SELECT
        m.IdMovil,
        m.Activo AS MovilActivo,
        m.FechaCreacion AS MovilFechaCreacion,
        m.FechaActualizacion AS MovilFechaActualizacion,
        e.IdEmpresa,
        e.SapCodigo AS EmpresaSapCodigo,
        e.Rut AS EmpresaRut,
        e.RazonSocial AS EmpresaRazonSocial,
        e.Activo AS EmpresaActiva,
        ch.IdChofer,
        ch.SapIdFiscal AS ChoferSapIdFiscal,
        ch.SapNombre AS ChoferNombre,
        ch.Telefono AS ChoferTelefono,
        ch.Activo AS ChoferActivo,
        MIN(cf.FechaSalida) AS PeriodoDesde,
        MAX(cf.FechaSalida) AS PeriodoHasta,
        COUNT(cf.IdCabeceraFlete) AS Viajes
      FROM [cfl].[Movil] m
      INNER JOIN [cfl].[EmpresaTransporte] e ON e.IdEmpresa = m.IdEmpresaTransporte
      INNER JOIN [cfl].[Chofer] ch ON ch.IdChofer = m.IdChofer
      LEFT JOIN [cfl].[CabeceraFlete] cf ON cf.IdMovil = m.IdMovil
      WHERE m.IdCamion = @id
      GROUP BY
        m.IdMovil,
        m.Activo,
        m.FechaCreacion,
        m.FechaActualizacion,
        e.IdEmpresa,
        e.SapCodigo,
        e.Rut,
        e.RazonSocial,
        e.Activo,
        ch.IdChofer,
        ch.SapIdFiscal,
        ch.SapNombre,
        ch.Telefono,
        ch.Activo
      ORDER BY e.RazonSocial ASC, ch.SapNombre ASC;
    `;

    const result = await pool.request().input("id", id).query(sql);
    return {
      mode: "camion",
      rows: result.recordset,
    };
  }

  return null;
}

router.get("/folios/:id/movimientos", async (req, res, next) => {
  const idFolio = Number(req.params.id);
  if (!Number.isInteger(idFolio) || idFolio <= 0) {
    res.status(400).json({ error: "ID invalido para Folios" });
    return;
  }

  try {
    const auth = await resolveAuthContext(req);
    if (!hasAnyPermission(auth, maintainerReadPermissions("folios"))) {
      res.status(403).json({
        error: "No tienes permisos para consultar movimientos de folio",
        role: auth?.primaryRole || null,
      });
      return;
    }

    const pool = await getPool();
    const folio = await fetchFolioEstado(pool, idFolio);
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    const query = `
      SELECT
        cf.IdCabeceraFlete,
        SapNumeroEntrega = COALESCE(se.SapNumeroEntrega, cf.SapNumeroEntrega),
        se.SistemaFuente,
        cf.Estado,
        cf.FechaSalida,
        cf.HoraSalida,
        cf.MontoAplicado,
        tf.Nombre AS TipoFleteNombre,
        cc.Nombre AS CentroCostoNombre
      FROM [cfl].[CabeceraFlete] cf
      LEFT JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = cf.IdTipoFlete
      LEFT JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = cf.IdCentroCosto
      LEFT JOIN [cfl].[FleteSapEntrega] fe ON fe.IdCabeceraFlete = cf.IdCabeceraFlete
      LEFT JOIN [cfl].[SapEntrega] se ON se.IdSapEntrega = fe.IdSapEntrega
      WHERE cf.IdFolio = @idFolio
      ORDER BY cf.FechaActualizacion DESC, cf.IdCabeceraFlete DESC;
    `;

    const result = await pool.request().input("idFolio", idFolio).query(query);
    const data = result.recordset.map((row) => {
      const normalized = normalizeLifecycleStatus(row.Estado);
      return {
        ...row,
        Estado: normalized,
        can_desasignar: normalized === "ASIGNADO_FOLIO",
      };
    });

    res.json({
      id_folio: idFolio,
      estado_folio: String(folio.Estado || "").toUpperCase(),
      data,
      total: data.length,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/folios/:id/movimientos/asignar-sap", async (req, res, next) => {
  const idFolio = Number(req.params.id);
  if (!Number.isInteger(idFolio) || idFolio <= 0) {
    res.status(400).json({ error: "ID invalido para Folios" });
    return;
  }

  const sapNumeroEntrega = String(req.body?.sap_numero_entrega || "").trim();
  if (!sapNumeroEntrega) {
    res.status(400).json({ error: "Falta sap_numero_entrega" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "folios");
    if (!auth) return;

    const pool = await getPool();
    const folio = await fetchFolioEstado(pool, idFolio);
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    if (String(folio.Estado || "").toUpperCase() !== "ABIERTO") {
      res.status(409).json({ error: "Solo se pueden asignar movimientos a folios en estado ABIERTO" });
      return;
    }
    if (isFolioBlocked(folio)) {
      res.status(409).json({ error: "El folio esta bloqueado y no permite cambios" });
      return;
    }
    const targetFolioNumero = String(folio.FolioNumero || "").trim();
    if (targetFolioNumero === "0") {
      res.status(409).json({ error: "El folio 0 es reservado y no permite asignaciones manuales" });
      return;
    }
    const targetEstado = targetFolioNumero === "0" ? "COMPLETADO" : "ASIGNADO_FOLIO";

    const lookup = await pool
      .request()
      .input("sapNumeroEntrega", sapNumeroEntrega)
      .query(`
        SELECT TOP 1
          cf.IdCabeceraFlete,
          cf.IdFolio,
          cf.Estado,
          SapNumeroEntrega = COALESCE(se.SapNumeroEntrega, cf.SapNumeroEntrega)
        FROM [cfl].[CabeceraFlete] cf
        LEFT JOIN [cfl].[FleteSapEntrega] fe ON fe.IdCabeceraFlete = cf.IdCabeceraFlete
        LEFT JOIN [cfl].[SapEntrega] se ON se.IdSapEntrega = fe.IdSapEntrega
        WHERE COALESCE(se.SapNumeroEntrega, cf.SapNumeroEntrega) = @sapNumeroEntrega
        ORDER BY cf.FechaActualizacion DESC, cf.IdCabeceraFlete DESC;
      `);

    const target = lookup.recordset[0] || null;
    if (!target) {
      res.status(404).json({ error: "No se encontro movimiento para el codigo SAP indicado" });
      return;
    }

    const normalizedStatus = normalizeLifecycleStatus(target.Estado);
    if (!["ASIGNADO_FOLIO", "COMPLETADO"].includes(normalizedStatus)) {
      res.status(409).json({
        error: "El movimiento debe estar en estado COMPLETADO o ASIGNADO_FOLIO para gestion manual de folio",
        estado_actual: normalizedStatus || null,
      });
      return;
    }

    const now = new Date();
    await pool
      .request()
      .input("idCabeceraFlete", Number(target.IdCabeceraFlete))
      .input("idFolio", idFolio)
      .input("targetEstado", targetEstado)
      .input("updatedAt", now)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET
          IdFolio = @idFolio,
          Estado = @targetEstado,
          FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idCabeceraFlete;
      `);

    res.status(201).json({
      message: "Movimiento asignado al folio",
      role: auth.primaryRole,
      data: {
        id_folio: idFolio,
        id_cabecera_flete: Number(target.IdCabeceraFlete),
        sap_numero_entrega: String(target.SapNumeroEntrega || sapNumeroEntrega),
        estado: targetEstado,
      },
    });
  } catch (error) {
    next(error);
  }
});

router.patch("/folios/:id/movimientos/:id_cabecera_flete/desasignar", async (req, res, next) => {
  const idFolio = Number(req.params.id);
  const idCabeceraFlete = Number(req.params.id_cabecera_flete);

  if (!Number.isInteger(idFolio) || idFolio <= 0) {
    res.status(400).json({ error: "ID invalido para Folios" });
    return;
  }
  if (!Number.isInteger(idCabeceraFlete) || idCabeceraFlete <= 0) {
    res.status(400).json({ error: "id_cabecera_flete invalido" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "folios");
    if (!auth) return;

    const pool = await getPool();
    const folio = await fetchFolioEstado(pool, idFolio);
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }
    if (String(folio.Estado || "").toUpperCase() !== "ABIERTO") {
      res.status(409).json({ error: "Solo se pueden desasignar movimientos desde folios ABIERTO" });
      return;
    }
    if (isFolioBlocked(folio)) {
      res.status(409).json({ error: "El folio esta bloqueado y no permite cambios" });
      return;
    }

    const lookup = await pool
      .request()
      .input("idCabeceraFlete", idCabeceraFlete)
      .input("idFolio", idFolio)
      .query(`
        SELECT TOP 1
          IdCabeceraFlete,
          IdFolio,
          Estado
        FROM [cfl].[CabeceraFlete]
        WHERE IdCabeceraFlete = @idCabeceraFlete
          AND IdFolio = @idFolio;
      `);

    const target = lookup.recordset[0] || null;
    if (!target) {
      res.status(404).json({ error: "El movimiento no esta asignado al folio indicado" });
      return;
    }

    const normalizedStatus = normalizeLifecycleStatus(target.Estado);
    if (!["ASIGNADO_FOLIO", "COMPLETADO"].includes(normalizedStatus)) {
      res.status(409).json({
        error: "Solo se pueden desasignar movimientos en estado ASIGNADO_FOLIO o COMPLETADO",
        estado_actual: normalizedStatus || null,
      });
      return;
    }

    const defaultFolioId = await resolveDefaultFolioId(pool);
    if (!defaultFolioId) {
      res.status(409).json({ error: "No existe folio por defecto (FolioNumero = 0)" });
      return;
    }

    const defaultFolio = await fetchFolioEstado(pool, defaultFolioId);
    const defaultFolioNumero = String(defaultFolio?.FolioNumero || "").trim();
    const targetEstado = defaultFolioNumero === "0" ? "COMPLETADO" : "ASIGNADO_FOLIO";

    const now = new Date();
    await pool
      .request()
      .input("idCabeceraFlete", idCabeceraFlete)
      .input("defaultFolioId", defaultFolioId)
      .input("targetEstado", targetEstado)
      .input("updatedAt", now)
      .query(`
        UPDATE [cfl].[CabeceraFlete]
        SET
          IdFolio = @defaultFolioId,
          Estado = @targetEstado,
          FechaActualizacion = @updatedAt
        WHERE IdCabeceraFlete = @idCabeceraFlete;
      `);

    res.json({
      message: "Movimiento desasignado (folio 0)",
      role: auth.primaryRole,
      data: {
        id_cabecera_flete: idCabeceraFlete,
        id_folio: defaultFolioId,
        estado: targetEstado,
      },
    });
  } catch (error) {
    next(error);
  }
});

router.patch("/folios/:id/bloqueo", async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID invalido para Folios" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "folios");
    if (!auth) return;

    const pool = await getPool();
    const folio = await fetchFolioEstado(pool, id);
    if (!folio) {
      res.status(404).json({ error: "Folio no encontrado" });
      return;
    }

    const folioNumero = String(folio.FolioNumero || "").trim();
    if (folioNumero === "0") {
      res.status(409).json({ error: "El folio 0 es reservado y no se puede bloquear" });
      return;
    }

    const bloqueado = req.body?.bloqueado;
    if (bloqueado === undefined) {
      res.status(400).json({ error: "Debes enviar el campo bloqueado" });
      return;
    }

    const nuevoBloqueado = toBool(bloqueado);
    const request = pool.request();
    request.input("id", id);
    request.input("bloqueado", nuevoBloqueado);
    request.input("updatedAt", new Date());

    await request.query(`
      UPDATE [cfl].[Folio]
      SET
        [Bloqueado] = @bloqueado,
        [FechaActualizacion] = @updatedAt
      WHERE [IdFolio] = @id;
    `);

    const updatedRow = await fetchEntityById(pool, getEntityConfig("folios"), id);
    res.json({
      message: `Folio ${nuevoBloqueado ? "bloqueado" : "desbloqueado"}`,
      role: auth.primaryRole,
      data: updatedRow,
    });
  } catch (error) {
    next(error);
  }
});

// ── Temporada activa ─────────────────────────────────────────────────────────
router.get("/temporadas/activa", async (req, res, next) => {
  try {
    const auth = await resolveAuthContext(req);
    if (!hasAnyPermission(auth, maintainerReadPermissions("temporadas"))) {
      res.status(403).json({ error: "Sin permisos para consultar temporadas", role: auth?.primaryRole || null });
      return;
    }

    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT TOP 1
        IdTemporada, Codigo, Nombre, FechaInicio, FechaFin, Activa, Cerrada
      FROM [cfl].[Temporada]
      WHERE Activa = 1 AND Cerrada = 0
      ORDER BY FechaInicio DESC;
    `);

    if (!result.recordset[0]) {
      res.status(404).json({ error: "No hay temporada activa" });
      return;
    }

    res.json({ data: result.recordset[0] });
  } catch (error) {
    next(error);
  }
});

// ── Tarifas con filtro de temporada ──────────────────────────────────────────
// Debe ir ANTES de router.get('/:entity') para sobreescribir el genérico
router.get("/tarifas", async (req, res, next) => {
  const entityConfig = MAINTAINERS["tarifas"];

  try {
    const auth = await resolveAuthContext(req);
    if (!hasAnyPermission(auth, maintainerReadPermissions("tarifas"))) {
      res.status(403).json({ error: "Sin permisos para consultar tarifas", role: auth?.primaryRole || null, entity: "tarifas" });
      return;
    }

    const pool = await getPool();

    let temporadaId = req.query.temporada_id ? Number(req.query.temporada_id) : null;

    // Si no se pasó temporada_id, buscar la temporada activa
    if (!temporadaId) {
      const activeResult = await pool.request().query(`
        SELECT TOP 1 IdTemporada FROM [cfl].[Temporada]
        WHERE Activa = 1 AND Cerrada = 0 ORDER BY FechaInicio DESC;
      `);
      temporadaId = activeResult.recordset[0]?.IdTemporada || null;
    }

    let sql;
    let request = pool.request();

    if (temporadaId) {
      request.input("temporadaId", temporadaId);
      sql = `
        SELECT ${entityConfig.listColumns.join(", ")}
        FROM ${buildBaseFrom(entityConfig)}
        WHERE t.IdTemporada = @temporadaId
        ORDER BY ${entityConfig.orderBy};
      `;
    } else {
      sql = `
        SELECT ${entityConfig.listColumns.join(", ")}
        FROM ${buildBaseFrom(entityConfig)}
        ORDER BY ${entityConfig.orderBy};
      `;
    }

    const result = await request.query(sql);
    res.json({
      data: result.recordset,
      total: result.recordset.length,
      temporada_id: temporadaId || null,
      permissions: {
        role: auth.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(auth, maintainerWritePermissions("tarifas")),
      },
    });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: obtener roles asignados ────────────────────────────────────────
router.get("/usuarios/:id/roles", async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  try {
    const auth = await resolveAuthContext(req);
    if (!hasAnyPermission(auth, maintainerReadPermissions("usuarios"))) {
      res.status(403).json({ error: "Sin permisos para consultar usuarios", role: auth?.primaryRole || null });
      return;
    }

    const pool = await getPool();
    const result = await pool.request().input("id", id).query(`
      SELECT r.IdRol, r.Nombre, r.Descripcion, r.Activo
      FROM [cfl].[UsuarioRol] ur
      INNER JOIN [cfl].[Rol] r ON r.IdRol = ur.IdRol
      WHERE ur.IdUsuario = @id
      ORDER BY r.Nombre ASC;
    `);

    res.json({ id_usuario: id, data: result.recordset, total: result.recordset.length });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: asignar rol ────────────────────────────────────────────────────
router.post("/usuarios/:id/roles", async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  const idRol = Number(req.body?.id_rol);
  if (!Number.isInteger(idRol) || idRol <= 0) {
    res.status(400).json({ error: "id_rol inválido" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "usuarios");
    if (!auth) return;

    const pool = await getPool();

    // Verificar que el usuario existe
    const userCheck = await pool.request().input("id", id)
      .query(`SELECT TOP 1 IdUsuario FROM [cfl].[Usuario] WHERE IdUsuario = @id;`);
    if (!userCheck.recordset[0]) {
      res.status(404).json({ error: "Usuario no encontrado" });
      return;
    }

    // Verificar que el rol existe
    const rolCheck = await pool.request().input("idRol", idRol)
      .query(`SELECT TOP 1 IdRol FROM [cfl].[Rol] WHERE IdRol = @idRol AND Activo = 1;`);
    if (!rolCheck.recordset[0]) {
      res.status(404).json({ error: "Rol no encontrado o inactivo" });
      return;
    }

    // Insertar relación (si no existe)
    await pool.request()
      .input("id", id)
      .input("idRol", idRol)
      .query(`
        IF NOT EXISTS (
          SELECT 1 FROM [cfl].[UsuarioRol] WHERE IdUsuario = @id AND IdRol = @idRol
        )
        INSERT INTO [cfl].[UsuarioRol] (IdUsuario, IdRol) VALUES (@id, @idRol);
      `);

    res.status(201).json({ message: "Rol asignado al usuario", id_usuario: id, id_rol: idRol });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: quitar rol ─────────────────────────────────────────────────────
router.delete("/usuarios/:id/roles/:id_rol", async (req, res, next) => {
  const id = Number(req.params.id);
  const idRol = Number(req.params.id_rol);

  if (!Number.isInteger(id) || id <= 0 || !Number.isInteger(idRol) || idRol <= 0) {
    res.status(400).json({ error: "IDs inválidos" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "usuarios");
    if (!auth) return;

    const pool = await getPool();
    const result = await pool.request()
      .input("id", id)
      .input("idRol", idRol)
      .query(`
        DELETE FROM [cfl].[UsuarioRol]
        WHERE IdUsuario = @id AND IdRol = @idRol;
      `);

    if (result.rowsAffected[0] === 0) {
      res.status(404).json({ error: "Asignación no encontrada" });
      return;
    }

    res.json({ message: "Rol quitado del usuario", id_usuario: id, id_rol: idRol });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: toggle estado activo/inactivo ──────────────────────────────────
router.patch("/usuarios/:id/estado", async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  const nuevoEstado = req.body?.activo;
  if (typeof nuevoEstado !== "boolean" && nuevoEstado !== 0 && nuevoEstado !== 1) {
    res.status(400).json({ error: "Falta campo 'activo' (boolean)" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "usuarios");
    if (!auth) return;

    const pool = await getPool();
    const result = await pool.request()
      .input("id", id)
      .input("activo", toBool(nuevoEstado))
      .input("updatedAt", new Date())
      .query(`
        UPDATE [cfl].[Usuario]
        SET Activo = @activo, FechaActualizacion = @updatedAt
        WHERE IdUsuario = @id;
      `);

    if (result.rowsAffected[0] === 0) {
      res.status(404).json({ error: "Usuario no encontrado" });
      return;
    }

    res.json({ message: "Estado de usuario actualizado", id_usuario: id, activo: toBool(nuevoEstado) });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: crear con hash bcrypt ──────────────────────────────────────────
// Debe ir ANTES de router.post('/:entity') para sobreescribir el genérico
router.post("/usuarios", async (req, res, next) => {
  try {
    const auth = await ensureCanWrite(req, res, "usuarios");
    if (!auth) return;

    const { username, email, password, nombre, apellido, activo, id_rol } = req.body || {};

    if (!username || !email || !password) {
      res.status(400).json({ error: "Faltan campos requeridos", missing_fields: ["username", "email", "password"].filter(f => !req.body?.[f]) });
      return;
    }

    if (password.length < 8) {
      res.status(400).json({ error: "La contraseña debe tener al menos 8 caracteres" });
      return;
    }

    const passwordHash = await bcrypt.hash(password, 12);
    const now = new Date();

    const pool = await getPool();
    const insertResult = await pool.request()
      .input("username", username)
      .input("email", email)
      .input("passwordHash", passwordHash)
      .input("nombre", nombre || null)
      .input("apellido", apellido || null)
      .input("activo", activo !== undefined ? toBool(activo) : true)
      .input("createdAt", now)
      .input("updatedAt", now)
      .query(`
        INSERT INTO [cfl].[Usuario]
          (Username, Email, PasswordHash, Nombre, Apellido, Activo, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdUsuario AS id
        VALUES (@username, @email, @passwordHash, @nombre, @apellido, @activo, @createdAt, @updatedAt);
      `);

    const insertedId = insertResult.recordset[0].id;

    // Asignar rol si se proveyó
    if (id_rol && Number.isInteger(Number(id_rol))) {
      await pool.request()
        .input("idUsuario", insertedId)
        .input("idRol", Number(id_rol))
        .query(`
          INSERT INTO [cfl].[UsuarioRol] (IdUsuario, IdRol) VALUES (@idUsuario, @idRol);
        `);
    }

    // Devolver el usuario creado (sin PasswordHash)
    const entityConfig = MAINTAINERS["usuarios"];
    const insertedRow = await fetchEntityById(pool, entityConfig, insertedId);

    res.status(201).json({
      message: "Usuario creado",
      role: auth.primaryRole,
      data: insertedRow || { IdUsuario: insertedId },
    });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: editar con hash bcrypt opcional ─────────────────────────────────
// Debe ir ANTES de router.put('/:entity/:id') para sobreescribir el genérico
router.put("/usuarios/:id", async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, "usuarios");
    if (!auth) return;

    const { username, email, password, nombre, apellido, activo, id_rol } = req.body || {};
    const now = new Date();

    // Construir payload excluyendo PasswordHash (se maneja aparte)
    const allowedFields = ["username", "email", "nombre", "apellido", "activo"];
    const payload = collectPayload(req.body || {}, allowedFields);

    // Mapear claves de request body a columnas PascalCase
    const dbPayload = {};
    if (payload.username !== undefined) dbPayload["Username"] = payload.username;
    if (payload.email !== undefined) dbPayload["Email"] = payload.email;
    if (payload.nombre !== undefined) dbPayload["Nombre"] = payload.nombre;
    if (payload.apellido !== undefined) dbPayload["Apellido"] = payload.apellido;
    if (payload.activo !== undefined) dbPayload["Activo"] = payload.activo;

    // Si se envía password, hacer hash
    if (password) {
      if (password.length < 8) {
        res.status(400).json({ error: "La contraseña debe tener al menos 8 caracteres" });
        return;
      }
      dbPayload["PasswordHash"] = await bcrypt.hash(password, 12);
    }

    dbPayload["FechaActualizacion"] = now;

    const fields = Object.keys(dbPayload);
    if (fields.length === 0) {
      res.status(400).json({ error: "No se recibieron campos para actualizar" });
      return;
    }

    const pool = await getPool();
    const request = pool.request();
    request.input("id", id);

    const setClause = fields.map((fieldName, index) => {
      request.input(`p${index}`, dbPayload[fieldName]);
      return `[${fieldName}] = @p${index}`;
    }).join(", ");

    const updateResult = await request.query(`
      UPDATE [cfl].[Usuario]
      SET ${setClause}
      WHERE IdUsuario = @id;
    `);

    if (updateResult.rowsAffected[0] === 0) {
      res.status(404).json({ error: "Usuario no encontrado" });
      return;
    }

    // Actualizar rol si se proveyó
    if (id_rol !== undefined && id_rol !== null && id_rol !== "") {
      const idRolNum = Number(id_rol);
      if (Number.isInteger(idRolNum) && idRolNum > 0) {
        // Reemplazar todos los roles del usuario
        await pool.request().input("id", id).query(`
          DELETE FROM [cfl].[UsuarioRol] WHERE IdUsuario = @id;
        `);
        await pool.request()
          .input("id", id)
          .input("idRol", idRolNum)
          .query(`
            INSERT INTO [cfl].[UsuarioRol] (IdUsuario, IdRol) VALUES (@id, @idRol);
          `);
      }
    }

    const entityConfig = MAINTAINERS["usuarios"];
    const updatedRow = await fetchEntityById(pool, entityConfig, id);

    res.json({
      message: "Usuario actualizado",
      role: auth.primaryRole,
      data: updatedRow,
    });
  } catch (error) {
    next(error);
  }
});

router.get("/resumen", async (req, res, next) => {
  try {
    const auth = await resolveAuthContext(req);
    if (!hasAnyPermission(auth, maintainerReadPermissions("resumen"))) {
      res.status(403).json({
        error: "No tienes permisos para consultar mantenedores",
        role: auth?.primaryRole || null,
      });
      return;
    }

    const pool = await getPool();

    // Ejecuta todos los COUNT en paralelo en lugar de secuencialmente (evita N+1)
    const entries = Object.entries(MAINTAINERS);
    const counts = await Promise.all(
      entries.map(([, entityConfig]) =>
        pool.request().query(`SELECT COUNT_BIG(1) AS total FROM ${entityConfig.table};`)
      )
    );

    const summary = entries.map(([key, entityConfig], index) => ({
      key,
      title: entityConfig.title,
      total: Number(counts[index].recordset[0].total),
    }));

    res.json({
      data: summary,
      role: auth?.primaryRole || null,
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

// Compatibilidad dedicada para catalogo critico de bandeja y mantenedor
router.get("/tipos-flete", async (req, res, next) => {
  try {
    const auth = await resolveAuthContext(req);
    const permissionEntityKey = normalizePermissionEntityKey("tipos-flete");
    if (!hasAnyPermission(auth, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar este mantenedor",
        role: auth?.primaryRole || null,
        entity: "tipos-flete",
      });
      return;
    }

    const pool = await getPool();
    const rows = await fetchTiposFleteRows(pool);

    res.json({
      data: rows,
      total: rows.length,
      permissions: {
        role: auth.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(auth, maintainerWritePermissions(permissionEntityKey)),
      },
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:entity", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const entityKey = req.params.entity;

  try {
    const auth = await resolveAuthContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(entityKey);
    if (!hasAnyPermission(auth, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar este mantenedor",
        role: auth?.primaryRole || null,
        entity: entityKey,
      });
      return;
    }

    const pool = await getPool();
    const sql = `
      SELECT ${entityConfig.listColumns.join(", ")}
      FROM ${buildBaseFrom(entityConfig)}
      ORDER BY ${entityConfig.orderBy};
    `;

    const result = await pool.request().query(sql);
    res.json({
      data: result.recordset,
      total: result.recordset.length,
      permissions: {
        role: auth.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(auth, maintainerWritePermissions(permissionEntityKey)),
      },
    });
  } catch (error) {
    next(error);
  }
});

// [NUEVO ENDPOINT] GET /:entity/:id
// Expone fetchEntityById como endpoint público. El frontend puede obtener
// un registro específico sin tener que cargar toda la lista.
router.get("/:entity/:id", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) return;

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) return;

  try {
    const auth = await resolveAuthContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(req.params.entity);
    if (!hasAnyPermission(auth, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar este mantenedor",
        role: auth?.primaryRole || null,
        entity: req.params.entity,
      });
      return;
    }

    const pool = await getPool();
    const row = await fetchEntityById(pool, entityConfig, id);
    if (!row) {
      res.status(404).json({ error: `${entityConfig.title} no encontrado` });
      return;
    }

    res.json({
      data: row,
      permissions: {
        role: auth.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(auth, maintainerWritePermissions(permissionEntityKey)),
      },
    });
  } catch (error) {
    next(error);
  }
});

router.get("/:entity/:id/relaciones", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) {
    return;
  }

  try {
    const auth = await resolveAuthContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(req.params.entity);
    if (!hasAnyPermission(auth, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar relaciones",
        role: auth?.primaryRole || null,
        entity: req.params.entity,
      });
      return;
    }

    const pool = await getPool();
    const relationData = await getRelationsForEntity(pool, req.params.entity, id);

    if (!relationData) {
      res.status(404).json({
        error: `Relaciones no soportadas para ${req.params.entity}`,
      });
      return;
    }

    res.json({
      entity: req.params.entity,
      id,
      mode: relationData.mode,
      data: relationData.rows,
      total: relationData.rows.length,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/:entity", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, req.params.entity);
    if (!auth) return;

    const allowedFields = [...entityConfig.create.required, ...entityConfig.create.optional];
    const payload = collectPayload(req.body || {}, allowedFields);

    if (req.params.entity === "folios" && payload.Bloqueado === undefined) {
      payload.Bloqueado = false;
    }

    const missingRequired = entityConfig.create.required.filter(
      (fieldName) =>
        payload[fieldName] === undefined || payload[fieldName] === null || payload[fieldName] === ""
    );

    if (missingRequired.length > 0) {
      res.status(400).json({
        error: "Faltan campos requeridos",
        missing_fields: missingRequired,
      });
      return;
    }

    if (entityConfig.timestamps?.created) {
      payload[entityConfig.timestamps.created] = new Date();
    }
    if (entityConfig.timestamps?.updated) {
      payload[entityConfig.timestamps.updated] = new Date();
    }

    const fields = Object.keys(payload);
    const pool = await getPool();
    const request = pool.request();

    fields.forEach((fieldName, index) => {
      request.input(`p${index}`, payload[fieldName]);
    });

    const insertSql = `
      INSERT INTO ${entityConfig.table} (${fields.map((field) => `[${field}]`).join(", ")})
      OUTPUT INSERTED.[${entityConfig.idColumn}] AS id
      VALUES (${fields.map((_, index) => `@p${index}`).join(", ")});
    `;

    const insertResult = await request.query(insertSql);
    const insertedId = insertResult.recordset[0].id;
    const insertedRow = await fetchEntityById(pool, entityConfig, insertedId);

    res.status(201).json({
      message: `${entityConfig.title} creado`,
      role: auth.primaryRole,
      data: insertedRow || { [entityConfig.idColumn]: insertedId },
    });
  } catch (error) {
    next(error);
  }
});

router.put("/:entity/:id", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) {
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, req.params.entity);
    if (!auth) return;

    const payload = collectPayload(req.body || {}, entityConfig.update.allowed);

    if (entityConfig.timestamps?.updated) {
      payload[entityConfig.timestamps.updated] = new Date();
    }

    const fields = Object.keys(payload);
    if (fields.length === 0) {
      res.status(400).json({
        error: "No se recibieron campos para actualizar",
      });
      return;
    }

    const pool = await getPool();
    if (req.params.entity === "folios") {
      const folio = await fetchFolioEstado(pool, id);
      if (!folio) {
        res.status(404).json({ error: `${entityConfig.title} no encontrado` });
        return;
      }
      const folioNumero = String(folio.FolioNumero || "").trim();
      if (folioNumero === "0") {
        res.status(409).json({ error: "El folio 0 es reservado y no se puede modificar" });
        return;
      }
      if (isFolioBlocked(folio)) {
        res.status(409).json({ error: "El folio esta bloqueado y no se puede editar" });
        return;
      }
    }

    const request = pool.request();
    request.input("id", id);

    const setClause = fields
      .map((fieldName, index) => {
        request.input(`p${index}`, payload[fieldName]);
        return `[${fieldName}] = @p${index}`;
      })
      .join(", ");

    const updateSql = `
      UPDATE ${entityConfig.table}
      SET ${setClause}
      WHERE [${entityConfig.idColumn}] = @id;
    `;

    const updateResult = await request.query(updateSql);
    if (updateResult.rowsAffected[0] === 0) {
      res.status(404).json({ error: `${entityConfig.title} no encontrado` });
      return;
    }

    const updatedRow = await fetchEntityById(pool, entityConfig, id);
    res.json({
      message: `${entityConfig.title} actualizado`,
      role: auth.primaryRole,
      data: updatedRow,
    });
  } catch (error) {
    next(error);
  }
});

router.delete("/:entity/:id", async (req, res, next) => {
  const entityConfig = requireEntity(req, res);
  if (!entityConfig) {
    return;
  }

  const id = parseEntityId(req, res, entityConfig);
  if (id === null) {
    return;
  }

  try {
    const auth = await ensureCanWrite(req, res, req.params.entity);
    if (!auth) return;

    const pool = await getPool();
    if (req.params.entity === "folios") {
      const folio = await fetchFolioEstado(pool, id);
      if (folio) {
        const folioNumero = String(folio.FolioNumero || "").trim();
        if (folioNumero === "0") {
          res.status(409).json({ error: "El folio 0 es reservado y no se puede eliminar" });
          return;
        }
        if (isFolioBlocked(folio)) {
          res.status(409).json({ error: "El folio esta bloqueado y no se puede eliminar" });
          return;
        }
      }
    }

    if (entityConfig.softDeleteColumn) {
      const request = pool.request();
      request.input("id", id);
      request.input("active", false);

      let setClause = `[${entityConfig.softDeleteColumn}] = @active`;
      if (entityConfig.timestamps?.updated) {
        request.input("updatedAt", new Date());
        setClause += `, [${entityConfig.timestamps.updated}] = @updatedAt`;
      }

      const sql = `
        UPDATE ${entityConfig.table}
        SET ${setClause}
        WHERE [${entityConfig.idColumn}] = @id;
      `;

      const result = await request.query(sql);
      if (result.rowsAffected[0] === 0) {
        res.status(404).json({ error: `${entityConfig.title} no encontrado` });
        return;
      }
    } else {
      const sql = `
        DELETE FROM ${entityConfig.table}
        WHERE [${entityConfig.idColumn}] = @id;
      `;
      const result = await pool.request().input("id", id).query(sql);

      if (result.rowsAffected[0] === 0) {
        res.status(404).json({ error: `${entityConfig.title} no encontrado` });
        return;
      }
    }

    res.json({
      message: `${entityConfig.title} eliminado`,
      role: auth.primaryRole,
      id,
    });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  mantenedoresRouter: router,
};
