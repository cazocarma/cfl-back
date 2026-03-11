const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const { getPool } = require("../db");
const { resolveAuthContext } = require("../authz");
const { JWT_SECRET } = require("../middleware/auth.middleware");

const router = express.Router();

router.get("/context", async (req, res, next) => {
  try {
    const context = await resolveAuthContext(req);
    if (!context) {
      res.status(403).json({
        error: "No se pudo resolver el contexto de autorizacion",
      });
      return;
    }

    res.json({
      data: {
        role: context.primaryRole,
        roles: context.roleNames,
        permissions: Array.from(context.permissions).sort(),
        source: context.source,
      },
      generated_at: new Date().toISOString(),
    });
  } catch (error) {
    next(error);
  }
});

/**
 * POST /api/auth/login
 * Autentica con email + password; devuelve token JWT de 8h.
 */
router.post("/login", async (req, res, next) => {
  try {
    const email = String(req.body?.email || "").trim();
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({ error: "Email y contraseña son requeridos" });
    }

    const pool = await getPool();

    // Buscar usuario activo por email
    const userResult = await pool
      .request()
      .input("email", email)
      .query(`
        SELECT IdUsuario, Username, Email, Nombre, Apellido, PasswordHash
        FROM [cfl].[Usuario]
        WHERE Email = @email
          AND Activo = 1;
      `);

    const user = userResult.recordset[0] || null;

    // Respuesta genérica para no revelar si el email existe
    const INVALID_MSG = "Credenciales incorrectas";

    if (!user) {
      return res.status(401).json({ error: INVALID_MSG });
    }

    const passwordOk = await bcrypt.compare(password, user.PasswordHash);
    if (!passwordOk) {
      return res.status(401).json({ error: INVALID_MSG });
    }

    // Obtener el rol principal del usuario
    const roleResult = await pool
      .request()
      .input("userId", user.IdUsuario)
      .query(`
        SELECT TOP 1 r.Nombre AS role_nombre
        FROM [cfl].[UsuarioRol] ur
        INNER JOIN [cfl].[Rol] r ON r.IdRol = ur.IdRol AND r.Activo = 1
        WHERE ur.IdUsuario = @userId
        ORDER BY r.Nombre ASC;
      `);

    const primaryRole = roleResult.recordset[0]?.role_nombre || null;

    const payload = {
      id_usuario: Number(user.IdUsuario),
      username: user.Username,
      email: user.Email,
      nombre: user.Nombre || null,
      apellido: user.Apellido || null,
      role: primaryRole,
    };

    const token = jwt.sign(payload, JWT_SECRET, { expiresIn: "8h" });

    req.auditContext = {
      userId: payload.id_usuario,
      action: "login",
      entity: "auth",
      idEntidad: payload.id_usuario,
      summary: `Inicio de sesion exitoso para ${payload.username}`,
    };

    return res.json({
      token,
      user: {
        id_usuario: payload.id_usuario,
        username: payload.username,
        email: payload.email,
        nombre: payload.nombre,
        apellido: payload.apellido,
        role: payload.role,
      },
    });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  authRouter: router,
};
