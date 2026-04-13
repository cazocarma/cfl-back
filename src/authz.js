const { getPool } = require("./db");

const ADMIN_ROLE = "administrador";

const CACHE_TTL_MS = 30 * 1000;
const MAX_CACHE_SIZE = 500;
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
  if (cache.size >= MAX_CACHE_SIZE) {
    const firstKey = cache.keys().next().value;
    cache.delete(firstKey);
  }
  cache.set(key, {
    value,
    expiresAt: Date.now() + CACHE_TTL_MS,
  });
}

function hydrateAuthzContext(rows, source) {
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

async function fetchAuthzContextByRoleName(roleName) {
  const normalizedRole = normalizeText(roleName);
  if (!normalizedRole) return null;

  const cacheKey = buildCacheKey("role", normalizedRole);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.Nombre AS role_nombre,
      p.Clave AS permiso_clave
    FROM [cfl].[Rol] r
    LEFT JOIN [cfl].[RolPermiso] rp
      ON rp.IdRol = r.IdRol
    LEFT JOIN [cfl].[Permiso] p
      ON p.IdPermiso = rp.IdPermiso
     AND p.Activo = 1
    WHERE r.Activo = 1
      AND LOWER(r.Nombre) = LOWER(@roleName);
  `;

  const result = await pool.request().input("roleName", normalizedRole).query(query);
  const authzContext = hydrateAuthzContext(result.recordset, "role_name");
  if (authzContext) setCached(cacheKey, authzContext);
  return authzContext;
}

async function fetchAuthzContextByUserId(userId) {
  const parsedUserId = Number(userId);
  if (!Number.isInteger(parsedUserId) || parsedUserId <= 0) return null;

  const cacheKey = buildCacheKey("user_id", parsedUserId);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.Nombre AS role_nombre,
      p.Clave AS permiso_clave
    FROM [cfl].[Usuario] u
    INNER JOIN [cfl].[UsuarioRol] ur
      ON ur.IdUsuario = u.IdUsuario
    INNER JOIN [cfl].[Rol] r
      ON r.IdRol = ur.IdRol
     AND r.Activo = 1
    LEFT JOIN [cfl].[RolPermiso] rp
      ON rp.IdRol = r.IdRol
    LEFT JOIN [cfl].[Permiso] p
      ON p.IdPermiso = rp.IdPermiso
     AND p.Activo = 1
    WHERE u.IdUsuario = @userId
      AND u.Activo = 1
    ORDER BY r.Nombre ASC;
  `;

  const result = await pool.request().input("userId", parsedUserId).query(query);
  const authzContext = hydrateAuthzContext(result.recordset, "user_id");
  if (authzContext) setCached(cacheKey, authzContext);
  return authzContext;
}

async function fetchAuthzContextByUsername(username) {
  const normalizedUsername = normalizeText(username);
  if (!normalizedUsername) return null;

  const cacheKey = buildCacheKey("username", normalizedUsername);
  const cached = getCached(cacheKey);
  if (cached) return cached;

  const pool = await getPool();
  const query = `
    SELECT
      r.Nombre AS role_nombre,
      p.Clave AS permiso_clave
    FROM [cfl].[Usuario] u
    INNER JOIN [cfl].[UsuarioRol] ur
      ON ur.IdUsuario = u.IdUsuario
    INNER JOIN [cfl].[Rol] r
      ON r.IdRol = ur.IdRol
     AND r.Activo = 1
    LEFT JOIN [cfl].[RolPermiso] rp
      ON rp.IdRol = r.IdRol
    LEFT JOIN [cfl].[Permiso] p
      ON p.IdPermiso = rp.IdPermiso
     AND p.Activo = 1
    WHERE u.Username = @username
      AND u.Activo = 1
    ORDER BY r.Nombre ASC;
  `;

  const result = await pool.request().input("username", normalizedUsername).query(query);
  const authzContext = hydrateAuthzContext(result.recordset, "username");
  if (authzContext) setCached(cacheKey, authzContext);
  return authzContext;
}

async function resolveAuthzContext(req) {
  if (!req.authnClaims) {
    return null;
  }

  const byAuthnUser = await fetchAuthzContextByUserId(req.authnClaims.id_usuario);
  if (byAuthnUser) return byAuthnUser;

  const byAuthnRole = await fetchAuthzContextByRoleName(req.authnClaims.role);
  if (byAuthnRole) return byAuthnRole;

  return null;
}

function hasPermission(authzContext, permissionKey) {
  if (!authzContext || !permissionKey) {
    return false;
  }

  return authzContext.permissions.has(String(permissionKey).toLowerCase());
}

function hasAnyPermission(authzContext, permissionKeys) {
  if (
    !authzContext ||
    !Array.isArray(permissionKeys) ||
    permissionKeys.length === 0
  ) {
    return false;
  }

  for (const key of permissionKeys) {
    if (hasPermission(authzContext, key)) {
      return true;
    }
  }

  return false;
}

function isAdmin(authzContext) {
  return String(authzContext?.primaryRole || "").toLowerCase() === ADMIN_ROLE;
}

module.exports = {
  ADMIN_ROLE,
  resolveAuthzContext,
  hasPermission,
  hasAnyPermission,
  isAdmin,
};
