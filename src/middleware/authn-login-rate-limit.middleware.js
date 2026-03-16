const AUTHN_LOGIN_WINDOW_MS = 15 * 60 * 1000;
const AUTHN_LOGIN_MAX_ATTEMPTS = 10;

const authnLoginAttempts = new Map();

function authnLoginRateLimitMiddleware(req, res, next) {
  const clientIp = req.ip || req.socket?.remoteAddress || "unknown";
  const now = Date.now();
  const entry = authnLoginAttempts.get(clientIp);

  if (entry && now < entry.resetAt) {
    if (entry.count >= AUTHN_LOGIN_MAX_ATTEMPTS) {
      const retryAfterSecs = Math.ceil((entry.resetAt - now) / 1000);
      res.setHeader("Retry-After", String(retryAfterSecs));
      return res.status(429).json({
        error: "Demasiados intentos de inicio de sesión. Intenta más tarde.",
      });
    }

    entry.count += 1;
  } else {
    authnLoginAttempts.set(clientIp, {
      count: 1,
      resetAt: now + AUTHN_LOGIN_WINDOW_MS,
    });
  }

  return next();
}

setInterval(() => {
  const now = Date.now();
  for (const [clientIp, entry] of authnLoginAttempts) {
    if (now >= entry.resetAt) {
      authnLoginAttempts.delete(clientIp);
    }
  }
}, AUTHN_LOGIN_WINDOW_MS).unref();

module.exports = {
  authnLoginRateLimitMiddleware,
};
