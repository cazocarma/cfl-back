// ────────────────────────────────────────────────────────────────────
// Normalizacion de claves para matching find-or-create. Debe estar
// alineada con las columnas computed persistidas en SQL Server:
//
//   cfl.Chofer.SapIdFiscalNorm  = UPPER + REPLACE(., -, espacio, tab, LF)
//
// Para empresas/camiones no hay columna persistida; la normalizacion
// se aplica inline en los SELECT de `resolve-empresa.js` / `resolve-camion.js`.
// ────────────────────────────────────────────────────────────────────

function normalizeRut(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[.\- \t\n]/g, "");
}

function normalizeAlphaNumeric(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function normalizePatente(value) {
  return normalizeAlphaNumeric(value);
}

function normalizeCarro(value) {
  return normalizeAlphaNumeric(value) || "SINCARRO";
}

function trimOrNull(value, maxLen) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (!s) return null;
  return maxLen ? s.slice(0, maxLen) : s;
}

function isUniqueViolation(error) {
  const number = error?.number ?? error?.originalError?.info?.number;
  return number === 2627 || number === 2601;
}

module.exports = {
  normalizeRut,
  normalizeAlphaNumeric,
  normalizePatente,
  normalizeCarro,
  trimOrNull,
  isUniqueViolation,
};
