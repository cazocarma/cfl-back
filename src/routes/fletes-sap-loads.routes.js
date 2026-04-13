const express = require("express");
const { cflSapLoadService } = require("../modules/cfl-sap-load/service");
const { requirePermission } = require("../middleware/authz.middleware");

const router = express.Router();
const GUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

router.post("/vbeln", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
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

router.post("/xblnr", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
  req.auditContext = {
    entity: "fletes.cargas-sap",
    action: "ejecutar-xblnr",
  };

  try {
    const body = req.body || {};
    const job = await cflSapLoadService.createXblnrJob({
      sourceSystem: body.source_system,
      destination: body.destination,
      xblnr: body.xblnr,
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

router.post("/rango-fechas", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
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

router.get("/jobs", requirePermission("fletes.sap.etl.ver"), async (req, res, next) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 20, 1), 50);
    const userId = req.authnClaims?.id_usuario || null;
    const jobs = await cflSapLoadService.getRecentJobs(limit, userId);
    res.json({ data: jobs });
  } catch (error) {
    next(error);
  }
});

router.get("/jobs/ultimo", requirePermission("fletes.sap.etl.ver"), async (req, res, next) => {
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

router.get("/jobs/:jobId", requirePermission("fletes.sap.etl.ver"), async (req, res, next) => {
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
