const jwt = require("jsonwebtoken");
const { config } = require("../config");

/**
 * Middleware que valida el token JWT de authn en el header Authorization.
 * Si es válido, adjunta req.authnClaims y llama next().
 * Si no hay token o es inválido, responde 401.
 */
function requireJwtAuthn(req, res, next) {
  const authorizationHeader = req.headers["authorization"] || "";

  if (!authorizationHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token requerido" });
  }

  const token = authorizationHeader.slice(7);

  try {
    const authnClaims = jwt.verify(token, config.authn.jwtSecret);
    req.authnClaims = authnClaims;
    return next();
  } catch {
    return res.status(401).json({ error: "Token inválido o expirado" });
  }
}

module.exports = {
  requireJwtAuthn,
};
