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
    lower.includes("requiere_") ||
    lower === "bloqueado";

  if (isBooleanField) {
    return toBool(value);
  }

  return value;
}

function collectPayload(body, allowedFields) {
  const payload = {};

  for (const fieldName of allowedFields) {
    if (Object.prototype.hasOwnProperty.call(body, fieldName)) {
      payload[fieldName] = normalizeValue(fieldName, body[fieldName]);
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

async function fetchFolioEstado(pool, idFolio) {
  const result = await pool
    .request()
    .input("idFolio", idFolio)
    .query(`
      SELECT TOP 1 estado, folio_numero, bloqueado
      FROM [cfl].[CFL_folio]
      WHERE id_folio = @idFolio;
    `);
  return result.recordset[0] || null;
}

function isFolioBlocked(folio) {
  return folio?.bloqueado === true || folio?.bloqueado === 1;
}

async function resolveDefaultFolioId(pool) {
  const result = await pool.request().query(`
    SELECT TOP 1 id_folio
    FROM [cfl].[CFL_folio]
    WHERE LTRIM(RTRIM(CAST(folio_numero AS NVARCHAR(50)))) = '0'
    ORDER BY id_folio ASC;
  `);
  return result.recordset[0] ? Number(result.recordset[0].id_folio) : null;
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
        m.id_movil,
        m.activo AS movil_activo,
        m.created_at AS movil_created_at,
        m.updated_at AS movil_updated_at,
        e.id_empresa,
        e.sap_codigo AS empresa_sap_codigo,
        e.rut AS empresa_rut,
        e.razon_social AS empresa_razon_social,
        e.activo AS empresa_activa,
        c.id_camion,
        c.sap_patente,
        c.sap_carro,
        c.activo AS camion_activo,
        tc.id_tipo_camion,
        tc.nombre AS tipo_camion_nombre,
        MIN(cf.fecha_salida) AS periodo_desde,
        MAX(cf.fecha_salida) AS periodo_hasta,
        COUNT(cf.id_cabecera_flete) AS viajes
      FROM [cfl].[CFL_movil] m
      INNER JOIN [cfl].[CFL_empresa_transporte] e ON e.id_empresa = m.id_empresa_transporte
      INNER JOIN [cfl].[CFL_camion] c ON c.id_camion = m.id_camion
      INNER JOIN [cfl].[CFL_tipo_camion] tc ON tc.id_tipo_camion = c.id_tipo_camion
      LEFT JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_movil = m.id_movil
      WHERE m.id_chofer = @id
      GROUP BY
        m.id_movil,
        m.activo,
        m.created_at,
        m.updated_at,
        e.id_empresa,
        e.sap_codigo,
        e.rut,
        e.razon_social,
        e.activo,
        c.id_camion,
        c.sap_patente,
        c.sap_carro,
        c.activo,
        tc.id_tipo_camion,
        tc.nombre
      ORDER BY e.razon_social ASC, c.sap_patente ASC, c.sap_carro ASC;
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
        m.id_movil,
        m.activo AS movil_activo,
        m.created_at AS movil_created_at,
        m.updated_at AS movil_updated_at,
        ch.id_chofer,
        ch.sap_id_fiscal AS chofer_sap_id_fiscal,
        ch.sap_nombre AS chofer_nombre,
        ch.telefono AS chofer_telefono,
        ch.activo AS chofer_activo,
        c.id_camion,
        c.sap_patente,
        c.sap_carro,
        c.activo AS camion_activo,
        tc.id_tipo_camion,
        tc.nombre AS tipo_camion_nombre,
        MIN(cf.fecha_salida) AS periodo_desde,
        MAX(cf.fecha_salida) AS periodo_hasta,
        COUNT(cf.id_cabecera_flete) AS viajes
      FROM [cfl].[CFL_movil] m
      INNER JOIN [cfl].[CFL_chofer] ch ON ch.id_chofer = m.id_chofer
      INNER JOIN [cfl].[CFL_camion] c ON c.id_camion = m.id_camion
      INNER JOIN [cfl].[CFL_tipo_camion] tc ON tc.id_tipo_camion = c.id_tipo_camion
      LEFT JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_movil = m.id_movil
      WHERE m.id_empresa_transporte = @id
      GROUP BY
        m.id_movil,
        m.activo,
        m.created_at,
        m.updated_at,
        ch.id_chofer,
        ch.sap_id_fiscal,
        ch.sap_nombre,
        ch.telefono,
        ch.activo,
        c.id_camion,
        c.sap_patente,
        c.sap_carro,
        c.activo,
        tc.id_tipo_camion,
        tc.nombre
      ORDER BY ch.sap_nombre ASC, c.sap_patente ASC, c.sap_carro ASC;
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
        m.id_movil,
        m.activo AS movil_activo,
        m.created_at AS movil_created_at,
        m.updated_at AS movil_updated_at,
        e.id_empresa,
        e.sap_codigo AS empresa_sap_codigo,
        e.rut AS empresa_rut,
        e.razon_social AS empresa_razon_social,
        e.activo AS empresa_activa,
        ch.id_chofer,
        ch.sap_id_fiscal AS chofer_sap_id_fiscal,
        ch.sap_nombre AS chofer_nombre,
        ch.telefono AS chofer_telefono,
        ch.activo AS chofer_activo,
        MIN(cf.fecha_salida) AS periodo_desde,
        MAX(cf.fecha_salida) AS periodo_hasta,
        COUNT(cf.id_cabecera_flete) AS viajes
      FROM [cfl].[CFL_movil] m
      INNER JOIN [cfl].[CFL_empresa_transporte] e ON e.id_empresa = m.id_empresa_transporte
      INNER JOIN [cfl].[CFL_chofer] ch ON ch.id_chofer = m.id_chofer
      LEFT JOIN [cfl].[CFL_cabecera_flete] cf ON cf.id_movil = m.id_movil
      WHERE m.id_camion = @id
      GROUP BY
        m.id_movil,
        m.activo,
        m.created_at,
        m.updated_at,
        e.id_empresa,
        e.sap_codigo,
        e.rut,
        e.razon_social,
        e.activo,
        ch.id_chofer,
        ch.sap_id_fiscal,
        ch.sap_nombre,
        ch.telefono,
        ch.activo
      ORDER BY e.razon_social ASC, ch.sap_nombre ASC;
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
        cf.id_cabecera_flete,
        sap_numero_entrega = COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega),
        se.source_system,
        cf.estado,
        cf.fecha_salida,
        cf.hora_salida,
        cf.monto_aplicado,
        tf.nombre AS tipo_flete_nombre,
        cc.nombre AS centro_costo_nombre
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo
      LEFT JOIN [cfl].[CFL_flete_sap_entrega] fe ON fe.id_cabecera_flete = cf.id_cabecera_flete
      LEFT JOIN [cfl].[CFL_sap_entrega] se ON se.id_sap_entrega = fe.id_sap_entrega
      WHERE cf.id_folio = @idFolio
      ORDER BY cf.updated_at DESC, cf.id_cabecera_flete DESC;
    `;

    const result = await pool.request().input("idFolio", idFolio).query(query);
    const data = result.recordset.map((row) => {
      const normalized = normalizeLifecycleStatus(row.estado);
      return {
        ...row,
        estado: normalized,
        can_desasignar: normalized === "ASIGNADO_FOLIO",
      };
    });

    res.json({
      id_folio: idFolio,
      estado_folio: String(folio.estado || "").toUpperCase(),
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

    if (String(folio.estado || "").toUpperCase() !== "ABIERTO") {
      res.status(409).json({ error: "Solo se pueden asignar movimientos a folios en estado ABIERTO" });
      return;
    }
    if (isFolioBlocked(folio)) {
      res.status(409).json({ error: "El folio esta bloqueado y no permite cambios" });
      return;
    }
    const targetFolioNumero = String(folio.folio_numero || "").trim();
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
          cf.id_cabecera_flete,
          cf.id_folio,
          cf.estado,
          sap_numero_entrega = COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega)
        FROM [cfl].[CFL_cabecera_flete] cf
        LEFT JOIN [cfl].[CFL_flete_sap_entrega] fe ON fe.id_cabecera_flete = cf.id_cabecera_flete
        LEFT JOIN [cfl].[CFL_sap_entrega] se ON se.id_sap_entrega = fe.id_sap_entrega
        WHERE COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega) = @sapNumeroEntrega
        ORDER BY cf.updated_at DESC, cf.id_cabecera_flete DESC;
      `);

    const target = lookup.recordset[0] || null;
    if (!target) {
      res.status(404).json({ error: "No se encontro movimiento para el codigo SAP indicado" });
      return;
    }

    const normalizedStatus = normalizeLifecycleStatus(target.estado);
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
      .input("idCabeceraFlete", Number(target.id_cabecera_flete))
      .input("idFolio", idFolio)
      .input("targetEstado", targetEstado)
      .input("updatedAt", now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET
          id_folio = @idFolio,
          estado = @targetEstado,
          updated_at = @updatedAt
        WHERE id_cabecera_flete = @idCabeceraFlete;
      `);

    res.status(201).json({
      message: "Movimiento asignado al folio",
      role: auth.primaryRole,
      data: {
        id_folio: idFolio,
        id_cabecera_flete: Number(target.id_cabecera_flete),
        sap_numero_entrega: String(target.sap_numero_entrega || sapNumeroEntrega),
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
    if (String(folio.estado || "").toUpperCase() !== "ABIERTO") {
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
          id_cabecera_flete,
          id_folio,
          estado
        FROM [cfl].[CFL_cabecera_flete]
        WHERE id_cabecera_flete = @idCabeceraFlete
          AND id_folio = @idFolio;
      `);

    const target = lookup.recordset[0] || null;
    if (!target) {
      res.status(404).json({ error: "El movimiento no esta asignado al folio indicado" });
      return;
    }

    const normalizedStatus = normalizeLifecycleStatus(target.estado);
    if (!["ASIGNADO_FOLIO", "COMPLETADO"].includes(normalizedStatus)) {
      res.status(409).json({
        error: "Solo se pueden desasignar movimientos en estado ASIGNADO_FOLIO o COMPLETADO",
        estado_actual: normalizedStatus || null,
      });
      return;
    }

    const defaultFolioId = await resolveDefaultFolioId(pool);
    if (!defaultFolioId) {
      res.status(409).json({ error: "No existe folio por defecto (folio_numero = 0)" });
      return;
    }

    const defaultFolio = await fetchFolioEstado(pool, defaultFolioId);
    const defaultFolioNumero = String(defaultFolio?.folio_numero || "").trim();
    const targetEstado = defaultFolioNumero === "0" ? "COMPLETADO" : "ASIGNADO_FOLIO";

    const now = new Date();
    await pool
      .request()
      .input("idCabeceraFlete", idCabeceraFlete)
      .input("defaultFolioId", defaultFolioId)
      .input("targetEstado", targetEstado)
      .input("updatedAt", now)
      .query(`
        UPDATE [cfl].[CFL_cabecera_flete]
        SET
          id_folio = @defaultFolioId,
          estado = @targetEstado,
          updated_at = @updatedAt
        WHERE id_cabecera_flete = @idCabeceraFlete;
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

    const folioNumero = String(folio.folio_numero || "").trim();
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
      UPDATE [cfl].[CFL_folio]
      SET
        [bloqueado] = @bloqueado,
        [updated_at] = @updatedAt
      WHERE [id_folio] = @id;
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
        id_temporada, codigo, nombre, fecha_inicio, fecha_fin, activa, cerrada
      FROM [cfl].[CFL_temporada]
      WHERE activa = 1 AND cerrada = 0
      ORDER BY fecha_inicio DESC;
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
        SELECT TOP 1 id_temporada FROM [cfl].[CFL_temporada]
        WHERE activa = 1 AND cerrada = 0 ORDER BY fecha_inicio DESC;
      `);
      temporadaId = activeResult.recordset[0]?.id_temporada || null;
    }

    let sql;
    let request = pool.request();

    if (temporadaId) {
      request.input("temporadaId", temporadaId);
      sql = `
        SELECT ${entityConfig.listColumns.join(", ")}
        FROM ${buildBaseFrom(entityConfig)}
        WHERE t.id_temporada = @temporadaId
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
      SELECT r.id_rol, r.nombre, r.descripcion, r.activo
      FROM [cfl].[CFL_usuario_rol] ur
      INNER JOIN [cfl].[CFL_rol] r ON r.id_rol = ur.id_rol
      WHERE ur.id_usuario = @id
      ORDER BY r.nombre ASC;
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
      .query(`SELECT TOP 1 id_usuario FROM [cfl].[CFL_usuario] WHERE id_usuario = @id;`);
    if (!userCheck.recordset[0]) {
      res.status(404).json({ error: "Usuario no encontrado" });
      return;
    }

    // Verificar que el rol existe
    const rolCheck = await pool.request().input("idRol", idRol)
      .query(`SELECT TOP 1 id_rol FROM [cfl].[CFL_rol] WHERE id_rol = @idRol AND activo = 1;`);
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
          SELECT 1 FROM [cfl].[CFL_usuario_rol] WHERE id_usuario = @id AND id_rol = @idRol
        )
        INSERT INTO [cfl].[CFL_usuario_rol] (id_usuario, id_rol) VALUES (@id, @idRol);
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
        DELETE FROM [cfl].[CFL_usuario_rol]
        WHERE id_usuario = @id AND id_rol = @idRol;
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
        UPDATE [cfl].[CFL_usuario]
        SET activo = @activo, updated_at = @updatedAt
        WHERE id_usuario = @id;
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

    const password_hash = await bcrypt.hash(password, 12);
    const now = new Date();

    const pool = await getPool();
    const insertResult = await pool.request()
      .input("username", username)
      .input("email", email)
      .input("password_hash", password_hash)
      .input("nombre", nombre || null)
      .input("apellido", apellido || null)
      .input("activo", activo !== undefined ? toBool(activo) : true)
      .input("createdAt", now)
      .input("updatedAt", now)
      .query(`
        INSERT INTO [cfl].[CFL_usuario]
          (username, email, password_hash, nombre, apellido, activo, created_at, updated_at)
        OUTPUT INSERTED.id_usuario AS id
        VALUES (@username, @email, @password_hash, @nombre, @apellido, @activo, @createdAt, @updatedAt);
      `);

    const insertedId = insertResult.recordset[0].id;

    // Asignar rol si se proveyó
    if (id_rol && Number.isInteger(Number(id_rol))) {
      await pool.request()
        .input("idUsuario", insertedId)
        .input("idRol", Number(id_rol))
        .query(`
          INSERT INTO [cfl].[CFL_usuario_rol] (id_usuario, id_rol) VALUES (@idUsuario, @idRol);
        `);
    }

    // Devolver el usuario creado (sin password_hash)
    const entityConfig = MAINTAINERS["usuarios"];
    const insertedRow = await fetchEntityById(pool, entityConfig, insertedId);

    res.status(201).json({
      message: "Usuario creado",
      role: auth.primaryRole,
      data: insertedRow || { id_usuario: insertedId },
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

    // Construir payload excluyendo password_hash (se maneja aparte)
    const allowedFields = ["username", "email", "nombre", "apellido", "activo"];
    const payload = collectPayload(req.body || {}, allowedFields);

    // Si se envía password, hacer hash
    if (password) {
      if (password.length < 8) {
        res.status(400).json({ error: "La contraseña debe tener al menos 8 caracteres" });
        return;
      }
      payload["password_hash"] = await bcrypt.hash(password, 12);
    }

    payload["updated_at"] = now;

    const fields = Object.keys(payload);
    if (fields.length === 0) {
      res.status(400).json({ error: "No se recibieron campos para actualizar" });
      return;
    }

    const pool = await getPool();
    const request = pool.request();
    request.input("id", id);

    const setClause = fields.map((fieldName, index) => {
      request.input(`p${index}`, payload[fieldName]);
      return `[${fieldName}] = @p${index}`;
    }).join(", ");

    const updateResult = await request.query(`
      UPDATE [cfl].[CFL_usuario]
      SET ${setClause}
      WHERE id_usuario = @id;
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
          DELETE FROM [cfl].[CFL_usuario_rol] WHERE id_usuario = @id;
        `);
        await pool.request()
          .input("id", id)
          .input("idRol", idRolNum)
          .query(`
            INSERT INTO [cfl].[CFL_usuario_rol] (id_usuario, id_rol) VALUES (@id, @idRol);
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

    if (req.params.entity === "folios" && payload.bloqueado === undefined) {
      payload.bloqueado = false;
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
      const folioNumero = String(folio.folio_numero || "").trim();
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
        const folioNumero = String(folio.folio_numero || "").trim();
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
