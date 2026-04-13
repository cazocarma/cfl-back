const { JOB_TYPE } = require("./constants");

const DEFAULT_DATE = new Date(Date.UTC(1900, 0, 1));
const DEFAULT_TIME = "00:00:00";

function padOrTrim(value, length) {
  const normalized = String(value || "").trim();
  if (normalized.length > length) {
    return normalized.slice(0, length);
  }
  if (normalized.length < length) {
    return normalized.padEnd(length, " ");
  }
  return normalized;
}

function trunc(value, maxLength) {
  const normalized = String(value || "").trim();
  return normalized.length > maxLength ? normalized.slice(0, maxLength) : normalized;
}

function parseIsoDate(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value.trim())) {
    return null;
  }

  const [year, month, day] = value.trim().split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }

  return date;
}

function parseSapDate(value, fallback = DEFAULT_DATE) {
  const normalized = String(value || "").trim();
  if (normalized.length !== 8 || normalized === "00000000") {
    return new Date(fallback.getTime());
  }

  const year = Number(normalized.slice(0, 4));
  const month = Number(normalized.slice(4, 6));
  const day = Number(normalized.slice(6, 8));
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return new Date(fallback.getTime());
  }

  return date;
}

function parseSapTime(value, fallback = DEFAULT_TIME) {
  const normalized = String(value || "").trim();
  if (normalized.length !== 6 || normalized === "000000") {
    return fallback;
  }

  const hh = Number(normalized.slice(0, 2));
  const mm = Number(normalized.slice(2, 4));
  const ss = Number(normalized.slice(4, 6));

  if (!Number.isInteger(hh) || !Number.isInteger(mm) || !Number.isInteger(ss)) {
    return fallback;
  }

  const safeHh = Math.max(0, Math.min(23, hh));
  const safeMm = Math.max(0, Math.min(59, mm));
  const safeSs = Math.max(0, Math.min(59, ss));

  return `${String(safeHh).padStart(2, "0")}:${String(safeMm).padStart(2, "0")}:${String(
    safeSs
  ).padStart(2, "0")}`;
}

function parseSapDecimal(value, fallback = 0) {
  const normalized = String(value || "").trim().replace(",", ".");
  if (!normalized) {
    return fallback;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatDateToIso(date) {
  return date instanceof Date && !Number.isNaN(date.getTime())
    ? date.toISOString().slice(0, 10)
    : null;
}

function formatDateToSap(date) {
  const iso = formatDateToIso(date);
  return iso ? iso.replace(/-/g, "") : null;
}

function normalizeDestination(value, fallback) {
  const normalized = String(value || fallback || "").trim().toUpperCase();
  return /^[A-Z0-9_]+$/.test(normalized) ? normalized : null;
}

function normalizeVbeln(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (!normalized || normalized.length > 20 || !/^[A-Z0-9]+$/.test(normalized)) {
    return null;
  }

  if (/^\d+$/.test(normalized) && normalized.length < 10) {
    return normalized.padStart(10, "0");
  }

  return normalized;
}

function normalizeXblnr(value) {
  const normalized = String(value || "").trim().toUpperCase();
  if (!normalized || normalized.length > 16 || !/^[A-Z0-9]+$/.test(normalized)) {
    return null;
  }
  return normalized;
}

function inclusiveDateRangeDays(fromDate, toDate) {
  const diffMs = toDate.getTime() - fromDate.getTime();
  return Math.floor(diffMs / 86400000) + 1;
}

function buildSourceSystem(destination) {
  return `SAP_${destination}`;
}

function buildSourceName(job) {
  if (job.job_type === JOB_TYPE.VBELN) {
    const count = Array.isArray(job.vbelns) ? job.vbelns.length : 0;
    if (count === 1) {
      return `CFL OnDemand VBELN ${job.vbelns[0]} via SAP ETL`;
    }
    return `CFL OnDemand ${count} VBELN via SAP ETL`;
  }

  if (job.job_type === JOB_TYPE.XBLNR) {
    const count = Array.isArray(job.xblnrs) ? job.xblnrs.length : 0;
    if (count === 1) {
      return `CFL OnDemand XBLNR ${job.xblnrs[0]} via SAP ETL`;
    }
    return `CFL OnDemand ${count} XBLNR via SAP ETL`;
  }

  return `CFL OnDemand DateRange ${job.fecha_desde}..${job.fecha_hasta} via SAP ETL`;
}

function buildScopeKey(job) {
  if (job.job_type === JOB_TYPE.VBELN) {
    const normalizedVbelns = Array.isArray(job.vbelns) ? [...job.vbelns].sort() : [];
    return `${job.destination}|VBELN|${normalizedVbelns.join(",")}`;
  }

  if (job.job_type === JOB_TYPE.XBLNR) {
    const normalizedXblnrs = Array.isArray(job.xblnrs) ? [...job.xblnrs].sort() : [];
    return `${job.destination}|XBLNR|${normalizedXblnrs.join(",")}`;
  }

  return `${job.destination}|DATE_RANGE|${job.fecha_desde}|${job.fecha_hasta}`;
}

function serializeJobParams(job) {
  return JSON.stringify({
    job_type: job.job_type,
    destination: job.destination,
    source_system: job.destination,
    vbeln: Array.isArray(job.vbelns) && job.vbelns.length > 0 ? job.vbelns : null,
    xblnr: Array.isArray(job.xblnrs) && job.xblnrs.length > 0 ? job.xblnrs : null,
    fecha_desde: job.fecha_desde || null,
    fecha_hasta: job.fecha_hasta || null,
    requested_by: {
      id_usuario: Number(job.requested_by?.id_usuario || 0) || null,
      username: job.requested_by?.username || null,
      role: job.requested_by?.role || null,
    },
  });
}

function parseJsonObject(value) {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

module.exports = {
  DEFAULT_DATE,
  DEFAULT_TIME,
  padOrTrim,
  trunc,
  parseIsoDate,
  parseSapDate,
  parseSapTime,
  parseSapDecimal,
  formatDateToIso,
  formatDateToSap,
  normalizeDestination,
  normalizeVbeln,
  normalizeXblnr,
  inclusiveDateRangeDays,
  buildSourceSystem,
  buildSourceName,
  buildScopeKey,
  serializeJobParams,
  parseJsonObject,
};
