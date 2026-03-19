const { getPool } = require("./db");

const CACHE_TTL_MS = 60 * 1000;
const cache = new Map();

/**
 * Revoca un token insertandolo en la blocklist.
 * @param {string} jti - ID unico del token (claim jti)
 * @param {number} idUsuario - ID del usuario
 * @param {string} motivo - "logout" | "password_change" | "user_deactivated" | "admin_revoke"
 * @param {Date} expiresAt - Fecha de expiracion original del token
 */
async function revokeToken(jti, idUsuario, motivo, expiresAt) {
  const pool = await getPool();
  await pool
    .request()
    .input("jti", jti)
    .input("idUsuario", idUsuario)
    .input("motivo", motivo)
    .input("expiresAt", expiresAt)
    .query(`
      IF NOT EXISTS (SELECT 1 FROM [cfl].[TokenBlocklist] WHERE Jti = @jti)
        INSERT INTO [cfl].[TokenBlocklist] (Jti, IdUsuario, Motivo, ExpiresAt)
        VALUES (@jti, @idUsuario, @motivo, @expiresAt);
    `);
  cache.set(jti, true);
}

/**
 * Revoca todos los tokens activos de un usuario.
 * Se usa al desactivar usuario o cambiar password.
 */
async function revokeAllForUser(idUsuario, motivo) {
  const pool = await getPool();
  // No podemos invalidar tokens que no conocemos, pero podemos marcar al usuario.
  // La verificacion en el middleware revisara la tabla.
  await pool
    .request()
    .input("idUsuario", idUsuario)
    .input("motivo", motivo)
    .query(`
      INSERT INTO [cfl].[TokenBlocklist] (Jti, IdUsuario, Motivo, ExpiresAt)
      VALUES (NEWID(), @idUsuario, @motivo, DATEADD(HOUR, 8, GETUTCDATE()));
    `);
}

/**
 * Verifica si un token esta revocado.
 * Usa cache en memoria para evitar queries repetidas.
 */
async function isTokenRevoked(jti) {
  if (cache.has(jti)) return true;

  const pool = await getPool();
  const result = await pool
    .request()
    .input("jti", jti)
    .query("SELECT 1 FROM [cfl].[TokenBlocklist] WHERE Jti = @jti;");

  const revoked = result.recordset.length > 0;
  if (revoked) cache.set(jti, true);
  return revoked;
}

/**
 * Limpia tokens expirados de la blocklist y del cache.
 */
async function purgeExpired() {
  try {
    const pool = await getPool();
    await pool.request().query(
      "DELETE FROM [cfl].[TokenBlocklist] WHERE ExpiresAt < GETUTCDATE();"
    );

    for (const [jti] of cache) {
      cache.delete(jti);
    }
  } catch (error) {
    console.error("[token-blocklist] Error purging expired tokens:", error.message);
  }
}

// Purga automatica cada hora
setInterval(purgeExpired, 60 * 60 * 1000).unref();

module.exports = {
  revokeToken,
  revokeAllForUser,
  isTokenRevoked,
  purgeExpired,
};
