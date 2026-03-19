const jwt = require("jsonwebtoken");
const { config } = require("../config");
const { isTokenRevoked } = require("../token-blocklist");

/**
 * Middleware que valida el token JWT de authn en el header Authorization.
 * Verifica firma, expiracion y blocklist.
 * Si es válido, adjunta req.authnClaims y llama next().
 * Si no hay token, es inválido o está revocado, responde 401.
 */
async function requireJwtAuthn(req, res, next) {
  const authorizationHeader = req.headers["authorization"] || "";

  if (!authorizationHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token requerido" });
  }

  const token = authorizationHeader.slice(7);

  try {
    const authnClaims = jwt.verify(token, config.authn.jwtSecret, {
      algorithms: ["HS256"],
    });

    if (authnClaims.jti && (await isTokenRevoked(authnClaims.jti))) {
      return res.status(401).json({ error: "Token revocado" });
    }

    req.authnClaims = authnClaims;
    return next();
  } catch (error) {
    if (error.name === "TokenExpiredError" || error.name === "JsonWebTokenError") {
      return res.status(401).json({ error: "Token inválido o expirado" });
    }
    return next(error);
  }
}

module.exports = {
  requireJwtAuthn,
};
