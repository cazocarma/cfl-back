const { resolveAuthzContext, hasAnyPermission } = require("../authz");

/**
 * Middleware factory que resuelve el contexto de autorizacion y verifica permisos.
 * Adjunta req.authzContext para uso posterior en el handler.
 *
 * @param  {...string} requiredPermissions - Claves de permiso requeridas (OR).
 *   Si no se pasan permisos, solo resuelve y adjunta el contexto sin verificar.
 * @returns {Function} Express middleware
 */
function requirePermission(...requiredPermissions) {
  return async (req, res, next) => {
    try {
      const authzContext = await resolveAuthzContext(req);
      req.authzContext = authzContext;

      if (requiredPermissions.length === 0) {
        return next();
      }

      const isAdmin = String(authzContext?.primaryRole || "").toLowerCase() === "administrador";
      if (isAdmin) {
        return next();
      }

      if (hasAnyPermission(authzContext, requiredPermissions)) {
        return next();
      }

      return res.status(403).json({ error: "Sin permiso para esta operacion" });
    } catch (error) {
      return next(error);
    }
  };
}

module.exports = { requirePermission };
