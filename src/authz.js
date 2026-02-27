const { getPool } = require("./db");

const CACHE_TTL_MS = 30 * 1000;
const cache = new Map();

function normalizeText(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
}

function buildCacheKey(kind, value) {
  return `${kind}:${String(value).trim().toLowerCase()}`;
}

function getCached(key) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expiresAt) {
    cache.delete(key);
    return null;
  }
  return item.value;
}

function setCached(key, value) {
  cache.set(key, {
    value,
    expiresAt: Date.now() + CACHE_TTL_MS,
  });
}

function hydrateAuthContext(rows, source) {
  if (!rows || rows.length === 0) {
    return null;
  }

  const roleNames = [];
  const seenRoles = new Set();
  const permissions = new Set();

  for (const row of rows) {
    const roleName = normalizeText(row.role_nombre);
    if (roleName && !seenRoles.has(roleName.toLowerCase())) {
      seenRoles.add(roleName.toLowerCase());
      roleNames.push(roleName);
    }

    const permissionKey = normalizeText(row.permiso_clave);
    if (permissionKey) {
      permissions.add(permissionKey.toLowerCase());
    }
  }

  return {
    source,
    roleNames,
    primaryRole: roleNames[0] || null,
    permissions,
  };
}

async function fetchAuthByRoleName(roleName) {
  const normalizedRole = normalizeText(roleName);
  if (!normalizedRole) return null;

  const cacheKey = buildCacheKey("role", normalizedRole);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.nombre AS role_nombre,
      p.clave AS permiso_clave
    FROM [cfl].[CFL_rol] r
    LEFT JOIN [cfl].[CFL_rol_permiso] rp
      ON rp.id_rol = r.id_rol
    LEFT JOIN [cfl].[CFL_permiso] p
      ON p.id_permiso = rp.id_permiso
     AND p.activo = 1
    WHERE r.activo = 1
      AND LOWER(r.nombre) = LOWER(@roleName);
  `;

  const result = await pool.request().input("roleName", normalizedRole).query(query);
  const context = hydrateAuthContext(result.recordset, "role_name");
  if (context) setCached(cacheKey, context);
  return context;
}

async function fetchAuthByUserId(userId) {
  const parsedUserId = Number(userId);
  if (!Number.isInteger(parsedUserId) || parsedUserId <= 0) return null;

  const cacheKey = buildCacheKey("user_id", parsedUserId);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.nombre AS role_nombre,
      p.clave AS permiso_clave
    FROM [cfl].[CFL_usuario] u
    INNER JOIN [cfl].[CFL_usuario_rol] ur
      ON ur.id_usuario = u.id_usuario
    INNER JOIN [cfl].[CFL_rol] r
      ON r.id_rol = ur.id_rol
     AND r.activo = 1
    LEFT JOIN [cfl].[CFL_rol_permiso] rp
      ON rp.id_rol = r.id_rol
    LEFT JOIN [cfl].[CFL_permiso] p
      ON p.id_permiso = rp.id_permiso
     AND p.activo = 1
    WHERE u.id_usuario = @userId
      AND u.activo = 1
    ORDER BY r.nombre ASC;
  `;

  const result = await pool.request().input("userId", parsedUserId).query(query);
  const context = hydrateAuthContext(result.recordset, "user_id");
  if (context) setCached(cacheKey, context);
  return context;
}

async function fetchAuthByUsername(username) {
  const normalizedUsername = normalizeText(username);
  if (!normalizedUsername) return null;

  const cacheKey = buildCacheKey("username", normalizedUsername);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.nombre AS role_nombre,
      p.clave AS permiso_clave
    FROM [cfl].[CFL_usuario] u
    INNER JOIN [cfl].[CFL_usuario_rol] ur
      ON ur.id_usuario = u.id_usuario
    INNER JOIN [cfl].[CFL_rol] r
      ON r.id_rol = ur.id_rol
     AND r.activo = 1
    LEFT JOIN [cfl].[CFL_rol_permiso] rp
      ON rp.id_rol = r.id_rol
    LEFT JOIN [cfl].[CFL_permiso] p
      ON p.id_permiso = rp.id_permiso
     AND p.activo = 1
    WHERE u.username = @username
      AND u.activo = 1
    ORDER BY r.nombre ASC;
  `;

  const result = await pool.request().input("username", normalizedUsername).query(query);
  const context = hydrateAuthContext(result.recordset, "username");
  if (context) setCached(cacheKey, context);
  return context;
}

async function resolveAuthContext(req) {
  const roleName = normalizeText(req.header("x-cfl-role") || req.header("x-user-role") || req.query.role);
  const userId = normalizeText(req.header("x-cfl-user-id") || req.header("x-user-id") || req.query.user_id);
  const username = normalizeText(req.header("x-cfl-username") || req.header("x-username") || req.query.username);

  if (userId) {
    const byUserId = await fetchAuthByUserId(userId);
    if (byUserId) return byUserId;
  }

  if (username) {
    const byUsername = await fetchAuthByUsername(username);
    if (byUsername) return byUsername;
  }

  if (roleName) {
    const byRole = await fetchAuthByRoleName(roleName);
    if (byRole) return byRole;
  }

  return null;
}

function hasPermission(context, permissionKey) {
  if (!context || !permissionKey) {
    return false;
  }
  return context.permissions.has(String(permissionKey).toLowerCase());
}

function hasAnyPermission(context, permissionKeys) {
  if (!context || !Array.isArray(permissionKeys) || permissionKeys.length === 0) {
    return false;
  }

  for (const key of permissionKeys) {
    if (hasPermission(context, key)) {
      return true;
    }
  }
  return false;
}

module.exports = {
  resolveAuthContext,
  hasPermission,
  hasAnyPermission,
};
