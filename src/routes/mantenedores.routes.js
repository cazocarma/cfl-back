const express = require("express");
const { getPool } = require("../db");
const { MAINTAINERS } = require("../mantenedores-config");
const { hasAnyPermission, resolveAuthContext } = require("../authz");

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

function normalizeLifecycleStatus(status) {
  const normalized = String(status || "").trim().toUpperCase();
  if (normalized === "VALIDADO") return "ASIGNADO_FOLIO";
  if (normalized === "COMPLETO") return "COMPLETADO";
  return normalized;
}

async function fetchFolioEstado(pool, idFolio) {
  const result = await pool
    .request()
    .input("idFolio", idFolio)
    .query(`
      SELECT TOP 1 estado, folio_numero
      FROM [cfl].[CFL_folio]
      WHERE id_folio = @idFolio;
    `);
  return result.recordset[0] || null;
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
        sap_numero_entrega = COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega_sugerido),
        se.source_system,
        cf.estado,
        cf.fecha_salida,
        cf.hora_salida,
        cf.monto_aplicado,
        tf.nombre AS tipo_flete_nombre,
        cc.nombre AS centro_costo_nombre
      FROM [cfl].[CFL_cabecera_flete] cf
      LEFT JOIN [cfl].[CFL_tipo_flete] tf ON tf.id_tipo_flete = cf.id_tipo_flete
      LEFT JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = cf.id_centro_costo_final
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
          sap_numero_entrega = COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega_sugerido)
        FROM [cfl].[CFL_cabecera_flete] cf
        LEFT JOIN [cfl].[CFL_flete_sap_entrega] fe ON fe.id_cabecera_flete = cf.id_cabecera_flete
        LEFT JOIN [cfl].[CFL_sap_entrega] se ON se.id_sap_entrega = fe.id_sap_entrega
        WHERE COALESCE(se.sap_numero_entrega, cf.sap_numero_entrega_sugerido) = @sapNumeroEntrega
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
    const summary = [];

    for (const [key, entityConfig] of Object.entries(MAINTAINERS)) {
      const countSql = `SELECT COUNT_BIG(1) AS total FROM ${entityConfig.table};`;
      const countResult = await pool.request().query(countSql);
      summary.push({
        key,
        title: entityConfig.title,
        total: Number(countResult.recordset[0].total),
      });
    }

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
