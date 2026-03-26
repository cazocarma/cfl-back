const crypto = require("crypto");
const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { getPool } = require("../db");
const { config } = require("../config");
const { resolveAuthzContext } = require("../authz");
const { revokeToken } = require("../token-blocklist");
const { loginLimiter } = require("../middleware/rate-limit.middleware");
const { logger } = require("../logger");
const { validate } = require("../middleware/validate.middleware");
const { loginBody } = require("../schemas/authn.schemas");

const authnRouter = express.Router();

authnRouter.get("/context", async (req, res, next) => {
  try {
    const authzContext = await resolveAuthzContext(req);
    if (!authzContext) {
      res.status(403).json({
        error: "No se pudo resolver el contexto de autorización",
      });
      return;
    }

    res.json({
      data: {
        role: authzContext.primaryRole,
        roles: authzContext.roleNames,
        permissions: Array.from(authzContext.permissions).sort(),
        source: authzContext.source,
      },
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/authn/login
 * Autentica con email + password; devuelve token JWT de 8h.
 */
authnRouter.post(
  "/login",
  loginLimiter,
  validate({ body: loginBody }),
  async (req, res, next) => {
    try {
      const { email, password } = req.body;

      const pool = await getPool();

      const userResult = await pool.request().input("email", email).query(`
        SELECT IdUsuario, Username, Email, Nombre, Apellido, PasswordHash
        FROM [cfl].[Usuario]
        WHERE Email = @email
          AND Activo = 1;
      `);

      const user = userResult.recordset[0] || null;
      const invalidCredentialsMessage = "Credenciales incorrectas";

      if (!user) {
        logger.warn({ email, ip: req.ip }, "login_failed: usuario no encontrado");
        return res.status(401).json({ error: invalidCredentialsMessage });
      }

      const passwordIsValid = await bcrypt.compare(password, user.PasswordHash);
      if (!passwordIsValid) {
        logger.warn({ email, ip: req.ip, userId: user.IdUsuario }, "login_failed: password incorrecta");
        return res.status(401).json({ error: invalidCredentialsMessage });
      }

      const roleResult = await pool.request().input("userId", user.IdUsuario)
        .query(`
        SELECT TOP 1 r.Nombre AS role_nombre
        FROM [cfl].[UsuarioRol] ur
        INNER JOIN [cfl].[Rol] r ON r.IdRol = ur.IdRol AND r.Activo = 1
        WHERE ur.IdUsuario = @userId
        ORDER BY r.Nombre ASC;
      `);

      const primaryRole = roleResult.recordset[0]?.role_nombre || null;

      const authnClaims = {
        jti: crypto.randomUUID(),
        id_usuario: Number(user.IdUsuario),
        username: user.Username,
        email: user.Email,
        nombre: user.Nombre || null,
        apellido: user.Apellido || null,
        role: primaryRole,
      };

      const token = jwt.sign(authnClaims, config.authn.jwtSecret, {
        algorithm: "HS256",
        expiresIn: "8h",
      });

      req.auditContext = {
        userId: authnClaims.id_usuario,
        action: "login",
        entity: "authn",
        idEntidad: authnClaims.id_usuario,
        summary: `Inicio de sesion exitoso para ${authnClaims.username}`,
      };

      return res.json({
        token,
        user: {
          id_usuario: authnClaims.id_usuario,
          username: authnClaims.username,
          email: authnClaims.email,
          nombre: authnClaims.nombre,
          apellido: authnClaims.apellido,
          role: authnClaims.role,
        },
      });
    } catch (error) {
      next(error);
    }
  }
);

/**
 * POST /api/authn/logout
 * Revoca el token actual del usuario (requiere authn).
 */
authnRouter.post("/logout", async (req, res, next) => {
  try {
    const claims = req.authnClaims;
    if (claims && claims.jti && claims.exp) {
      await revokeToken(
        claims.jti,
        claims.id_usuario,
        "logout",
        new Date(claims.exp * 1000)
      );
    }

    req.auditContext = {
      userId: claims?.id_usuario,
      action: "logout",
      entity: "authn",
      idEntidad: claims?.id_usuario,
      summary: `Cierre de sesion para ${claims?.username}`,
    };

    return res.json({ message: "Sesion cerrada" });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  authnRouter,
};
