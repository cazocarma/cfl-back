const jwt = require("jsonwebtoken");

const JWT_SECRET = process.env.JWT_SECRET || "cfl-dev-secret";

/**
 * Middleware que valida el token JWT en el header Authorization.
 * Si es válido, adjunta req.jwtPayload y llama next().
 * Si no hay token o es inválido, responde 401.
 */
function jwtMiddleware(req, res, next) {
  const authHeader = req.headers["authorization"] || "";

  if (!authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token requerido" });
  }

  const token = authHeader.slice(7);

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    req.jwtPayload = payload;
    return next();
  } catch {
    return res.status(401).json({ error: "Token inválido o expirado" });
  }
}

module.exports = { jwtMiddleware, JWT_SECRET };
