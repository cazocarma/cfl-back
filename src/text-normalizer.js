const CP1252_UNICODE_TO_BYTE = new Map([
  [0x20ac, 0x80],
  [0x201a, 0x82],
  [0x0192, 0x83],
  [0x201e, 0x84],
  [0x2026, 0x85],
  [0x2020, 0x86],
  [0x2021, 0x87],
  [0x02c6, 0x88],
  [0x2030, 0x89],
  [0x0160, 0x8a],
  [0x2039, 0x8b],
  [0x0152, 0x8c],
  [0x017d, 0x8e],
  [0x2018, 0x91],
  [0x2019, 0x92],
  [0x201c, 0x93],
  [0x201d, 0x94],
  [0x2022, 0x95],
  [0x2013, 0x96],
  [0x2014, 0x97],
  [0x02dc, 0x98],
  [0x2122, 0x99],
  [0x0161, 0x9a],
  [0x203a, 0x9b],
  [0x0153, 0x9c],
  [0x017e, 0x9e],
  [0x0178, 0x9f],
]);

const SUSPECT_TEXT_RE =
  /[\u00c2\u00c3\u00e2\u2018\u2019\u201a\u201c\u201d\u201e\u2020\u2021\u2022\u2026\u2030\u2039\u203a\u0152\u0153\u0160\u0161\u0178\u017d\u017e\uFFFD]/u;
const MOJIBAKE_RE = /[\u00c2\u00c3\u00e2]/gu;
const REPLACEMENT_RE = /\uFFFD/gu;
const CONTROL_RE = /[\u0080-\u009f]/gu;
const SPANISH_ACCENT_RE = /[\u00c1\u00c9\u00cd\u00d1\u00d3\u00da\u00dc\u00e1\u00e9\u00ed\u00f1\u00f3\u00fa\u00fc]/gu;

function countMatches(value, regex) {
  return (value.match(regex) || []).length;
}

function scoreText(value) {
  if (typeof value !== "string") return Number.NEGATIVE_INFINITY;

  let score = 0;
  score -= countMatches(value, REPLACEMENT_RE) * 40;
  score -= countMatches(value, MOJIBAKE_RE) * 15;
  score -= countMatches(value, CONTROL_RE) * 10;
  score += countMatches(value, SPANISH_ACCENT_RE) * 2;
  return score;
}

function toCp1252Byte(codePoint) {
  if (codePoint <= 0xff) return codePoint;
  if (CP1252_UNICODE_TO_BYTE.has(codePoint)) return CP1252_UNICODE_TO_BYTE.get(codePoint);
  return null;
}

function guessByteForReplacement(previousByte, nextByte) {
  if (previousByte === 0xc3) {
    if (nextByte === 0x42 || nextByte === 0x54) return 0x8d; // I acute in uppercase names (e.g. ANIBAL/BENITEZ)
    return 0x81; // A acute by default (e.g. GONZALEZ/HERNANDEZ)
  }

  if (previousByte === 0xc2) {
    return 0xa0;
  }

  return 0x81;
}

function decodeUtf8FromMisdecodedText(value) {
  if (typeof value !== "string" || !SUSPECT_TEXT_RE.test(value)) return value;

  const bytes = [];
  for (let i = 0; i < value.length; i += 1) {
    const codePoint = value.codePointAt(i);
    if (codePoint > 0xffff) return value;

    if (codePoint === 0xfffd) {
      bytes.push(-1);
      continue;
    }

    const byte = toCp1252Byte(codePoint);
    if (byte === null || byte === undefined) {
      return value;
    }

    bytes.push(byte);
  }

  for (let i = 0; i < bytes.length; i += 1) {
    if (bytes[i] !== -1) continue;
    bytes[i] = guessByteForReplacement(bytes[i - 1], bytes[i + 1]);
  }

  return Buffer.from(bytes).toString("utf8");
}

function repairMojibakeString(value) {
  if (typeof value !== "string" || !SUSPECT_TEXT_RE.test(value)) return value;

  let current = value;

  for (let i = 0; i < 2; i += 1) {
    const candidate = decodeUtf8FromMisdecodedText(current);
    if (candidate === current) break;
    if (scoreText(candidate) < scoreText(current)) break;
    current = candidate;
  }

  return current;
}

function normalizeJsonTextPayload(payload, seen = new WeakSet()) {
  if (typeof payload === "string") {
    return repairMojibakeString(payload);
  }

  if (Array.isArray(payload)) {
    return payload.map((item) => normalizeJsonTextPayload(item, seen));
  }

  if (!payload || typeof payload !== "object") {
    return payload;
  }

  if (payload instanceof Date || Buffer.isBuffer(payload)) {
    return payload;
  }

  if (seen.has(payload)) {
    return payload;
  }

  seen.add(payload);
  const out = {};

  for (const [key, value] of Object.entries(payload)) {
    out[key] = normalizeJsonTextPayload(value, seen);
  }

  seen.delete(payload);
  return out;
}

module.exports = {
  repairMojibakeString,
  normalizeJsonTextPayload,
};
