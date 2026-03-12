const jwt = require("jsonwebtoken");

const DEV_SECRET = "cfl-dev-secret";
const JWT_SECRET = process.env.JWT_SECRET || DEV_SECRET;

if (JWT_SECRET === DEV_SECRET && process.env.NODE_ENV === "production") {
  console.error(
    "[SEGURIDAD] JWT_SECRET no está configurado. Se está usando el secreto de desarrollo en producción. " +
    "Configura la variable de entorno JWT_SECRET con un valor seguro."
  );
}

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
