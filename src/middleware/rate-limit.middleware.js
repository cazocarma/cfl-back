const rateLimit = require("express-rate-limit");

/**
 * Login: 10 requests por ventana de 15 minutos por IP.
 * Se aplica a nivel de ruta en POST /api/authn/login.
 */
const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 10,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    error: "Demasiados intentos de inicio de sesion. Intenta mas tarde.",
  },
});

/**
 * Escritura: 60 requests por minuto por IP.
 * Aplica solo a POST, PUT, PATCH, DELETE.
 */
const writeLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS",
  message: {
    error: "Demasiadas solicitudes de escritura. Intenta mas tarde.",
  },
});

/**
 * Lectura: 200 requests por minuto por IP.
 * Aplica solo a GET.
 */
const readLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 200,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => req.method !== "GET",
  message: {
    error: "Demasiadas solicitudes de lectura. Intenta mas tarde.",
  },
});

module.exports = {
  loginLimiter,
  writeLimiter,
  readLimiter,
};
