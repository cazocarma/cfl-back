const { getPool, sql } = require("./db");

const MAX_ACTION_LENGTH = 50;
const MAX_ENTITY_LENGTH = 100;
const MAX_ENTITY_ID_LENGTH = 50;
const MAX_SUMMARY_LENGTH = 300;
const MAX_IP_LENGTH = 50;

function trimTo(value, maxLength) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }

  return normalized.length > maxLength
    ? normalized.slice(0, maxLength)
    : normalized;
}

function normalizePath(url) {
  const raw = String(url || "").trim();
  const path = raw.split("?")[0];
  return path || "/";
}

function getPathSegments(req) {
  return normalizePath(req.originalUrl || req.url)
    .split("/")
    .filter(Boolean);
}

function isIdLike(value) {
  return /^[0-9]+$/.test(String(value || "").trim());
}

function shouldAuditRequest(req) {
  if (req.auditContext?.skip === true) {
    return false;
  }

  const path = normalizePath(req.originalUrl || req.url);

  if (path === "/" || path === "/health") {
    return false;
  }

  if (path === "/api/auth/context") {
    return false;
  }

  if (path === "/api/operaciones/auditoria/overview") {
    return false;
  }

  if (String(req.method || "").toUpperCase() === "GET") {
    return false;
  }

  return path.startsWith("/api/");
}

function deriveEntity(req) {
  if (req.auditContext?.entity) {
    return trimTo(req.auditContext.entity, MAX_ENTITY_LENGTH);
  }

  const segments = getPathSegments(req);
  const moduleName = segments[1] || "api";
  let resourceName = segments[2] || moduleName;

  if (isIdLike(resourceName) && segments[3]) {
    resourceName = segments[3];
  }

  if (moduleName === "auth") {
    return "auth";
  }

  if (moduleName === "mantenedores" && resourceName === "transportistas") {
    resourceName = "empresas-transporte";
  }

  return trimTo(
    resourceName ? `${moduleName}.${resourceName}` : moduleName,
    MAX_ENTITY_LENGTH
  );
}

function deriveAction(req) {
  if (req.auditContext?.action) {
    return trimTo(req.auditContext.action, MAX_ACTION_LENGTH);
  }

  const path = normalizePath(req.originalUrl || req.url);
  if (path === "/api/auth/login") {
    return "login";
  }

  const segments = getPathSegments(req);
  const specialActions = new Set([
    "asignar",
    "asignar-nuevo",
    "desasignar",
    "bloqueo",
    "crear",
    "ingresar",
    "anular",
    "descartar",
    "restaurar",
  ]);

  for (let index = segments.length - 1; index >= 0; index -= 1) {
    const segment = segments[index];
    if (!segment || isIdLike(segment) || segment === "api") {
      continue;
    }
    if (specialActions.has(segment)) {
      return trimTo(segment, MAX_ACTION_LENGTH);
    }
    break;
  }

  switch (String(req.method || "").toUpperCase()) {
    case "GET":
      return "view";
    case "POST":
      return "create";
    case "PUT":
      return "update";
    case "PATCH":
      return "update";
    case "DELETE":
      return "delete";
    default:
      return trimTo(req.method || "request", MAX_ACTION_LENGTH);
  }
}

function detectClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (forwarded) {
    const first = String(forwarded).split(",")[0];
    const trimmed = trimTo(first, MAX_IP_LENGTH);
    if (trimmed) return trimmed;
  }

  return (
    trimTo(req.ip, MAX_IP_LENGTH) ||
    trimTo(req.socket?.remoteAddress, MAX_IP_LENGTH) ||
    null
  );
}

function toAuditId(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const normalized = String(value).trim();
  if (!normalized) {
    return null;
  }

  return trimTo(normalized, MAX_ENTITY_ID_LENGTH);
}

function findEntityId(value, depth = 0) {
  if (depth > 4 || value === null || value === undefined) {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value.slice(0, 3)) {
      const nested = findEntityId(item, depth + 1);
      if (nested) return nested;
    }
    return null;
  }

  if (typeof value !== "object") {
    return null;
  }

  const preferredKeys = [
    "id",
    "id_auditoria",
    "id_factura",
    "id_folio",
    "id_cabecera_flete",
    "id_usuario",
  ];

  for (const key of preferredKeys) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      const id = toAuditId(value[key]);
      if (id) return id;
    }
  }

  for (const [key, fieldValue] of Object.entries(value)) {
    if (key.startsWith("id_")) {
      const id = toAuditId(fieldValue);
      if (id) return id;
    }
  }

  for (const key of ["data", "user", "cabecera"]) {
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      const nested = findEntityId(value[key], depth + 1);
      if (nested) return nested;
    }
  }

  for (const fieldValue of Object.values(value)) {
    const nested = findEntityId(fieldValue, depth + 1);
    if (nested) return nested;
  }

  return null;
}

function deriveEntityId(req, responseBody) {
  if (req.auditContext?.idEntidad) {
    return toAuditId(req.auditContext.idEntidad);
  }

  if (req.params?.id) {
    return toAuditId(req.params.id);
  }

  for (const [key, value] of Object.entries(req.params || {})) {
    if (key.toLowerCase().startsWith("id")) {
      const parsed = toAuditId(value);
      if (parsed) return parsed;
    }
  }

  return findEntityId(responseBody);
}

function extractMessage(responseBody) {
  if (!responseBody || typeof responseBody !== "object") {
    return null;
  }

  const directMessage =
    trimTo(responseBody.message, MAX_SUMMARY_LENGTH) ||
    trimTo(responseBody.error, MAX_SUMMARY_LENGTH);

  if (directMessage) {
    return directMessage;
  }

  if (responseBody.data && typeof responseBody.data === "object") {
    return (
      trimTo(responseBody.data.message, MAX_SUMMARY_LENGTH) ||
      trimTo(responseBody.data.error, MAX_SUMMARY_LENGTH)
    );
  }

  return null;
}

function buildSummary(req, statusCode, responseBody, action, entity) {
  if (req.auditContext?.summary) {
    return trimTo(req.auditContext.summary, MAX_SUMMARY_LENGTH);
  }

  const path = normalizePath(req.originalUrl || req.url);
  const message = extractMessage(responseBody);
  const statusLabel = Number(statusCode) >= 400 ? `error ${statusCode}` : `ok ${statusCode}`;
  const base = `${String(req.method || "REQUEST").toUpperCase()} ${path} | ${entity} | ${action} | ${statusLabel}`;

  return trimTo(message ? `${base} | ${message}` : base, MAX_SUMMARY_LENGTH);
}

function resolveUserId(req) {
  if (req.auditContext?.userId !== undefined && req.auditContext?.userId !== null) {
    const explicit = Number(req.auditContext.userId);
    return Number.isInteger(explicit) && explicit > 0 ? explicit : null;
  }

  const byJwt = Number(req.jwtPayload?.id_usuario);
  return Number.isInteger(byJwt) && byJwt > 0 ? byJwt : null;
}

async function insertAuditRow({ idUsuario, action, entity, entityId, summary, ipAddress }) {
  if (!Number.isInteger(idUsuario) || idUsuario <= 0) {
    return false;
  }

  const pool = await getPool();
  await pool
    .request()
    .input("idUsuario", sql.BigInt, idUsuario)
    .input("fechaHora", sql.DateTime2(0), new Date())
    .input("accion", sql.VarChar(50), trimTo(action, MAX_ACTION_LENGTH) || "request")
    .input("entidad", sql.VarChar(100), trimTo(entity, MAX_ENTITY_LENGTH) || "api")
    .input("idEntidad", sql.VarChar(50), toAuditId(entityId))
    .input("resumen", sql.VarChar(300), trimTo(summary, MAX_SUMMARY_LENGTH))
    .input("ipEquipo", sql.VarChar(50), trimTo(ipAddress, MAX_IP_LENGTH))
    .query(`
      INSERT INTO [cfl].[CFL_auditoria] (
        [id_usuario],
        [fecha_hora],
        [accion],
        [entidad],
        [id_entidad],
        [resumen],
        [ip_equipo]
      )
      VALUES (
        @idUsuario,
        @fechaHora,
        @accion,
        @entidad,
        @idEntidad,
        @resumen,
        @ipEquipo
      );
    `);

  return true;
}

async function recordAuditForRequest(req, statusCode, responseBody) {
  if (!shouldAuditRequest(req)) {
    return false;
  }

  const userId = resolveUserId(req);
  if (!userId) {
    return false;
  }

  const entity = deriveEntity(req);
  const action = deriveAction(req);
  const entityId = deriveEntityId(req, responseBody);
  const summary = buildSummary(req, statusCode, responseBody, action, entity);
  const ipAddress = detectClientIp(req);

  return insertAuditRow({
    idUsuario: userId,
    action,
    entity,
    entityId,
    summary,
    ipAddress,
  });
}

module.exports = {
  shouldAuditRequest,
  recordAuditForRequest,
};
