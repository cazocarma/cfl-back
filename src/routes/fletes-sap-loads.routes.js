const express = require("express");
const { resolveAuthzContext, hasAnyPermission } = require("../authz");
const { cflSapLoadService } = require("../modules/cfl-sap-load/service");

const router = express.Router();
const GUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

async function ensurePermission(req, res, permissionKeys, message) {
  const authzContext = await resolveAuthzContext(req);
  const allowed = hasAnyPermission(authzContext, [...permissionKeys, "mantenedores.admin"]);
  if (!allowed) {
    res.status(403).json({ error: message });
    return false;
  }
  return true;
}

router.post("/vbeln", async (req, res, next) => {
  if (
    !(await ensurePermission(
      req,
      res,
      ["fletes.sap.etl.ejecutar"],
      "No tienes permisos para ejecutar cargas SAP"
    ))
  ) {
    return;
  }

  req.auditContext = {
    entity: "fletes.cargas-sap",
    action: "ejecutar-vbeln",
  };

  try {
    const body = req.body || {};
    const job = await cflSapLoadService.createVbelnJob({
      sourceSystem: body.source_system,
      destination: body.destination,
      vbeln: body.vbeln,
      authnClaims: req.authnClaims,
    });

    res.status(202).json({ data: job });
  } catch (error) {
    if (error.statusCode === 409 && error.data) {
      res.status(409).json({
        error: error.message,
        data: error.data,
      });
      return;
    }
    next(error);
  }
});

router.post("/rango-fechas", async (req, res, next) => {
  if (
    !(await ensurePermission(
      req,
      res,
      ["fletes.sap.etl.ejecutar"],
      "No tienes permisos para ejecutar cargas SAP"
    ))
  ) {
    return;
  }

  req.auditContext = {
    entity: "fletes.cargas-sap",
    action: "ejecutar-rango-fechas",
  };

  try {
    const body = req.body || {};
    const job = await cflSapLoadService.createDateRangeJob({
      sourceSystem: body.source_system,
      destination: body.destination,
      fechaDesde: body.fecha_desde,
      fechaHasta: body.fecha_hasta,
      authnClaims: req.authnClaims,
    });

    res.status(202).json({ data: job });
  } catch (error) {
    if (error.statusCode === 409 && error.data) {
      res.status(409).json({
        error: error.message,
        data: error.data,
      });
      return;
    }
    next(error);
  }
});

router.get("/jobs", async (req, res, next) => {
  if (
    !(await ensurePermission(
      req,
      res,
      ["fletes.sap.etl.ver"],
      "No tienes permisos para consultar jobs de cargas SAP"
    ))
  ) {
    return;
  }

  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 50);
    const userId = req.authnClaims?.id_usuario || null;
    const jobs = await cflSapLoadService.getRecentJobs(limit, userId);
    res.json({ data: jobs });
  } catch (error) {
    next(error);
  }
});

router.get("/jobs/ultimo", async (req, res, next) => {
  if (
    !(await ensurePermission(
      req,
      res,
      ["fletes.sap.etl.ver"],
      "No tienes permisos para consultar jobs de cargas SAP"
    ))
  ) {
    return;
  }

  try {
    const userId = req.authnClaims?.id_usuario || null;
    const job = await cflSapLoadService.getLatestJob(userId);
    if (!job) {
      res.status(404).json({ error: "No existen jobs de cargas SAP" });
      return;
    }

    res.json({ data: job });
  } catch (error) {
    next(error);
  }
});

router.get("/jobs/:jobId", async (req, res, next) => {
  if (
    !(await ensurePermission(
      req,
      res,
      ["fletes.sap.etl.ver"],
      "No tienes permisos para consultar jobs de cargas SAP"
    ))
  ) {
    return;
  }

  if (!GUID_PATTERN.test(String(req.params.jobId || "").trim())) {
    res.status(400).json({ error: "job_id invalido" });
    return;
  }

  try {
    const job = await cflSapLoadService.getJob(req.params.jobId);
    if (!job) {
      res.status(404).json({ error: "Job no encontrado" });
      return;
    }

    res.json({ data: job });
  } catch (error) {
    next(error);
  }
});

module.exports = {
  fletesSapLoadsRouter: router,
};
