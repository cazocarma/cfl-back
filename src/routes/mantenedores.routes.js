const express = require("express");
const bcrypt = require("bcryptjs");
const { getPool } = require("../db");
const { MAINTAINERS } = require("../mantenedores-config");
const { hasAnyPermission, resolveAuthzContext } = require("../authz");
const { normalizeLifecycleStatus } = require("../utils/lifecycle");
const { validate } = require("../middleware/validate.middleware");
const { requirePermission } = require("../middleware/authz.middleware");
const { crearUsuarioBody, actualizarUsuarioBody } = require("../schemas/usuarios.schemas");

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

async function fetchTiposFleteRows(pool, activoFilter = 'todos') {
  const whereClause = activoFilter === 'si' ? 'WHERE t.Activo = 1'
    : activoFilter === 'no' ? 'WHERE t.Activo = 0' : '';
  const sqlNuevo = `
    SELECT
      t.IdTipoFlete,
      t.SapCodigo,
      t.Nombre,
      t.Activo,
      CantidadImputacionesActivas = COALESCE(im.CantidadImputacionesActivas, 0)
    FROM [cfl].[TipoFlete] t
    OUTER APPLY (
      SELECT COUNT_BIG(1) AS CantidadImputacionesActivas
      FROM [cfl].[ImputacionFlete] i
      WHERE i.IdTipoFlete = t.IdTipoFlete
        AND i.Activo = 1
    ) im
    ${whereClause}
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
      CAST(NULL AS BIGINT) AS CantidadImputacionesActivas
    FROM [cfl].[CFL_tipo_flete] t
    ORDER BY t.nombre ASC;
  `;

  const legacyResult = await pool.request().query(sqlLegado);
  return legacyResult.recordset;
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
  const authzContext = await resolveAuthzContext(req);
  if (
    hasAnyPermission(
      authzContext,
      maintainerWritePermissions(permissionEntityKey)
    )
  ) {
    return authzContext;
  }

  res.status(403).json({
    error: "No tienes permisos para modificar este mantenedor",
    role: authzContext?.primaryRole || null,
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

// ── Temporada activa ─────────────────────────────────────────────────────────
router.get("/temporadas/activa", requirePermission("mantenedores.view"), async (req, res, next) => {
  try {
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
router.get("/tarifas", requirePermission("mantenedores.view"), async (req, res, next) => {
  const entityConfig = MAINTAINERS["tarifas"];

  try {
    const authzContext = req.authzContext;

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
        WHERE t.IdTemporada = @temporadaId AND t.Activo = 1
        ORDER BY ${entityConfig.orderBy};
      `;
    } else {
      sql = `
        SELECT ${entityConfig.listColumns.join(", ")}
        FROM ${buildBaseFrom(entityConfig)}
        WHERE t.Activo = 1
        ORDER BY ${entityConfig.orderBy};
      `;
    }

    const result = await request.query(sql);
    res.json({
      data: result.recordset,
      total: result.recordset.length,
      temporada_id: temporadaId || null,
      permissions: {
        role: authzContext.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(authzContext, maintainerWritePermissions("tarifas")),
      },
    });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: obtener roles asignados ────────────────────────────────────────
router.get("/usuarios/:id/roles", requirePermission("mantenedores.view"), async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  try {
    const pool = await getPool();
    const result = await pool.request().input("id", id).query(`
      SELECT r.IdRol, r.Nombre, r.Descripcion, r.Activo
      FROM [cfl].[UsuarioRol] ur
      INNER JOIN [cfl].[Rol] r ON r.IdRol = ur.IdRol
      WHERE ur.IdUsuario = @id AND r.Activo = 1
      ORDER BY r.Nombre ASC;
    `);

    res.json({ id_usuario: id, data: result.recordset, total: result.recordset.length });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: asignar rol ────────────────────────────────────────────────────
router.post("/usuarios/:id/roles", requirePermission("mantenedores.admin", "mantenedores.edit.usuarios"), async (req, res, next) => {
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
    const authzContext = await ensureCanWrite(req, res, "usuarios");
    if (!authzContext) return;

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
router.delete("/usuarios/:id/roles/:id_rol", requirePermission("mantenedores.admin", "mantenedores.edit.usuarios"), async (req, res, next) => {
  const id = Number(req.params.id);
  const idRol = Number(req.params.id_rol);

  if (!Number.isInteger(id) || id <= 0 || !Number.isInteger(idRol) || idRol <= 0) {
    res.status(400).json({ error: "IDs inválidos" });
    return;
  }

  try {
    const authzContext = await ensureCanWrite(req, res, "usuarios");
    if (!authzContext) return;

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
router.patch("/usuarios/:id/estado", requirePermission("mantenedores.admin", "mantenedores.edit.usuarios"), async (req, res, next) => {
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
    const authzContext = await ensureCanWrite(req, res, "usuarios");
    if (!authzContext) return;

    if (!toBool(nuevoEstado) && Number(req.authnClaims?.id_usuario) === id) {
      res.status(400).json({ error: "No puedes desactivar tu propio usuario." });
      return;
    }

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
router.post("/usuarios", requirePermission("mantenedores.admin", "mantenedores.edit.usuarios"), validate({ body: crearUsuarioBody }), async (req, res, next) => {
  try {
    const authzContext = await ensureCanWrite(req, res, "usuarios");
    if (!authzContext) return;

    const { username, email, password, nombre, apellido, activo, id_rol } = req.body || {};

    if (!username || !email || !password) {
      res.status(400).json({ error: "Faltan campos requeridos" });
      return;
    }

    if (password.length < 8) {
      res.status(400).json({ error: "La contraseña debe tener al menos 8 caracteres" });
      return;
    }

    if (!/[A-Z]/.test(password) || !/[a-z]/.test(password) || !/\d/.test(password)) {
      res.status(400).json({ error: "La contraseña debe contener al menos una mayúscula, una minúscula y un dígito" });
      return;
    }

    const passwordHash = await bcrypt.hash(password, 12);
    const now = new Date();

    // ID explícito opcional (mismo contrato que el resto de mantenedores)
    const rawExplicitId = req.body?.IdUsuario ?? req.body?.id_usuario;
    const explicitId =
      rawExplicitId === undefined || rawExplicitId === null || rawExplicitId === ""
        ? null
        : Number(rawExplicitId);
    if (explicitId !== null && (!Number.isInteger(explicitId) || explicitId <= 0)) {
      res.status(400).json({ error: "El ID de usuario debe ser un entero positivo." });
      return;
    }

    const pool = await getPool();
    const request = pool.request()
      .input("username", username)
      .input("email", email)
      .input("passwordHash", passwordHash)
      .input("nombre", nombre || null)
      .input("apellido", apellido || null)
      .input("activo", activo !== undefined ? toBool(activo) : true)
      .input("createdAt", now)
      .input("updatedAt", now);

    const insertCore = explicitId !== null
      ? `
        INSERT INTO [cfl].[Usuario]
          (IdUsuario, Username, Email, PasswordHash, Nombre, Apellido, Activo, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdUsuario AS id
        VALUES (@idUsuario, @username, @email, @passwordHash, @nombre, @apellido, @activo, @createdAt, @updatedAt);
      `
      : `
        INSERT INTO [cfl].[Usuario]
          (Username, Email, PasswordHash, Nombre, Apellido, Activo, FechaCreacion, FechaActualizacion)
        OUTPUT INSERTED.IdUsuario AS id
        VALUES (@username, @email, @passwordHash, @nombre, @apellido, @activo, @createdAt, @updatedAt);
      `;

    if (explicitId !== null) {
      request.input("idUsuario", explicitId);
    }

    const insertSql = explicitId !== null
      ? `SET IDENTITY_INSERT [cfl].[Usuario] ON;\n${insertCore}\nSET IDENTITY_INSERT [cfl].[Usuario] OFF;`
      : insertCore;

    const insertResult = await request.query(insertSql);
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
      role: authzContext.primaryRole,
      data: insertedRow || { IdUsuario: insertedId },
    });
  } catch (error) {
    next(error);
  }
});

// ── Usuarios: editar con hash bcrypt opcional ─────────────────────────────────
// Debe ir ANTES de router.put('/:entity/:id') para sobreescribir el genérico
router.put("/usuarios/:id", requirePermission("mantenedores.admin", "mantenedores.edit.usuarios"), validate({ body: actualizarUsuarioBody }), async (req, res, next) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ error: "ID de usuario inválido" });
    return;
  }

  try {
    const authzContext = await ensureCanWrite(req, res, "usuarios");
    if (!authzContext) return;

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
      role: authzContext.primaryRole,
      data: updatedRow,
    });
  } catch (error) {
    next(error);
  }
});

// ── Sync productores desde SAP OData ──────────────────────────────────────
router.post("/productores/sync-sap", requirePermission("mantenedores.admin"), async (req, res, next) => {
  try {
    const { syncProductores } = require("../modules/sap-sync/sync-productores");
    const result = await syncProductores();

    req.auditContext = { entity: "mantenedores.productores", action: "sync-sap" };

    res.json({
      message: `Sincronizacion completada: ${result.inserted} nuevos, ${result.updated} actualizados, ${result.unchanged} sin cambios`,
      data: result,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/choferes/sync-sap", requirePermission("mantenedores.admin"), async (req, res, next) => {
  try {
    const { syncChoferes } = require("../modules/sap-sync/sync-transport-catalogs");
    const result = await syncChoferes();

    req.auditContext = { entity: "mantenedores.choferes", action: "sync-sap" };

    res.json({
      message: `Sincronizacion completada: ${result.inserted} nuevos, ${result.updated} actualizados, ${result.unchanged} sin cambios`,
      data: result,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/camiones/sync-sap", requirePermission("mantenedores.admin"), async (req, res, next) => {
  try {
    const { syncCamiones } = require("../modules/sap-sync/sync-transport-catalogs");
    const result = await syncCamiones();

    req.auditContext = { entity: "mantenedores.camiones", action: "sync-sap" };

    res.json({
      message: `Sincronizacion completada: ${result.inserted} nuevos, ${result.updated} actualizados, ${result.unchanged} sin cambios`,
      data: result,
    });
  } catch (error) {
    next(error);
  }
});

router.get("/resumen", requirePermission("mantenedores.view"), async (req, res, next) => {
  try {
    const authzContext = req.authzContext;

    const pool = await getPool();

    // Ejecuta todos los COUNT en paralelo en lugar de secuencialmente (evita N+1)
    const entries = Object.entries(MAINTAINERS);
    const counts = await Promise.all(
      entries.map(([, entityConfig]) => {
        return pool.request().query(`SELECT COUNT_BIG(1) AS total FROM ${entityConfig.table};`);
      })
    );

    const summary = entries.map(([key, entityConfig], index) => ({
      key,
      title: entityConfig.title,
      total: Number(counts[index].recordset[0].total),
    }));

    res.json({
      data: summary,
      role: authzContext?.primaryRole || null,
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

// Compatibilidad dedicada para catalogo critico de bandeja y mantenedor
router.get("/tipos-flete", requirePermission("mantenedores.view"), async (req, res, next) => {
  try {
    const authzContext = req.authzContext;
    const permissionEntityKey = normalizePermissionEntityKey("tipos-flete");

    const pool = await getPool();
    const activoParam = String(req.query.activo || "todos").toLowerCase();
    const rows = await fetchTiposFleteRows(pool, activoParam);

    res.json({
      data: rows,
      total: rows.length,
      permissions: {
        role: authzContext.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(authzContext, maintainerWritePermissions(permissionEntityKey)),
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
    const authzContext = await resolveAuthzContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(entityKey);
    if (!hasAnyPermission(authzContext, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar este mantenedor",
        role: authzContext?.primaryRole || null,
        entity: entityKey,
      });
      return;
    }

    const pool = await getPool();
    const activoParam = String(req.query.activo || "todos").toLowerCase();
    let activeFilter = "";
    if (entityConfig.softDeleteColumn) {
      if (activoParam === "si") {
        activeFilter = `WHERE ${entityConfig.alias}.${entityConfig.softDeleteColumn} = 1`;
      } else if (activoParam === "no") {
        activeFilter = `WHERE ${entityConfig.alias}.${entityConfig.softDeleteColumn} = 0`;
      }
      // "todos" → sin filtro
    }
    const maxRows = 5000; // limite de seguridad para evitar DoS
    const sql = `
      SELECT TOP ${maxRows} ${entityConfig.listColumns.join(", ")}
      FROM ${buildBaseFrom(entityConfig)}
      ${activeFilter}
      ORDER BY ${entityConfig.orderBy};
    `;

    const result = await pool.request().query(sql);
    res.json({
      data: result.recordset,
      total: result.recordset.length,
      permissions: {
        role: authzContext.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(authzContext, maintainerWritePermissions(permissionEntityKey)),
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
    const authzContext = await resolveAuthzContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(req.params.entity);
    if (!hasAnyPermission(authzContext, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar este mantenedor",
        role: authzContext?.primaryRole || null,
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
        role: authzContext.primaryRole,
        can_view: true,
        can_edit: hasAnyPermission(authzContext, maintainerWritePermissions(permissionEntityKey)),
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
    const authzContext = await resolveAuthzContext(req);
    const permissionEntityKey = normalizePermissionEntityKey(req.params.entity);
    if (!hasAnyPermission(authzContext, maintainerReadPermissions(permissionEntityKey))) {
      res.status(403).json({
        error: "No tienes permisos para consultar relaciones",
        role: authzContext?.primaryRole || null,
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
    const authzContext = await ensureCanWrite(req, res, req.params.entity);
    if (!authzContext) return;

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

    // ID explícito opcional: si el body trae la PK y es un entero válido,
    // se incluye en el INSERT envuelto en IDENTITY_INSERT ON/OFF. Si no,
    // la BD auto-asigna el siguiente valor IDENTITY.
    // Aceptamos tanto la key Pascal (ej. IdEmpresa) como la variante snake
    // (ej. id_empresa) porque el frontend usa snake_case en su config.
    const idColumnSnake = entityConfig.idColumn
      .replace(/([A-Z])([A-Z][a-z])/g, '$1_$2')
      .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
      .toLowerCase();
    const rawExplicitId = req.body?.[entityConfig.idColumn] ?? req.body?.[idColumnSnake];
    const explicitId =
      rawExplicitId === undefined || rawExplicitId === null || rawExplicitId === ""
        ? null
        : Number(rawExplicitId);
    if (explicitId !== null && (!Number.isInteger(explicitId) || explicitId <= 0)) {
      res.status(400).json({ error: `El ID '${entityConfig.idColumn}' debe ser un entero positivo.` });
      return;
    }
    if (explicitId !== null) {
      payload[entityConfig.idColumn] = explicitId;
    }

    const fields = Object.keys(payload);
    const pool = await getPool();
    const request = pool.request();

    fields.forEach((fieldName, index) => {
      request.input(`p${index}`, payload[fieldName]);
    });

    const insertCore = `
      INSERT INTO ${entityConfig.table} (${fields.map((field) => `[${field}]`).join(", ")})
      OUTPUT INSERTED.[${entityConfig.idColumn}] AS id
      VALUES (${fields.map((_, index) => `@p${index}`).join(", ")});
    `;

    const insertSql = explicitId !== null
      ? `SET IDENTITY_INSERT ${entityConfig.table} ON;\n${insertCore}\nSET IDENTITY_INSERT ${entityConfig.table} OFF;`
      : insertCore;

    const insertResult = await request.query(insertSql);
    const insertedId = insertResult.recordset[0].id;
    const insertedRow = await fetchEntityById(pool, entityConfig, insertedId);

    res.status(201).json({
      message: `${entityConfig.title} creado`,
      role: authzContext.primaryRole,
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
    const authzContext = await ensureCanWrite(req, res, req.params.entity);
    if (!authzContext) return;

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
      role: authzContext.primaryRole,
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
    const authzContext = await ensureCanWrite(req, res, req.params.entity);
    if (!authzContext) return;

    const pool = await getPool();

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
      role: authzContext.primaryRole,
      id,
    });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  mantenedoresRouter: router,
};
