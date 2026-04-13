const express = require("express");
const { cflRomanaLoadService } = require("../modules/cfl-romana-load/service");
const { requirePermission } = require("../middleware/authz.middleware");

const router = express.Router();

router.post("/rango-fechas", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
  try {
    const { centro, fecha_desde, fecha_hasta } = req.body || {};
    const result = await cflRomanaLoadService.createDateRangeJob({ centro, fechaDesde: fecha_desde, fechaHasta: fecha_hasta, authnClaims: req.authnClaims });
    res.status(202).json({ data: result });
  } catch (error) { next(error); }
});

router.post("/npartida", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
  try {
    const result = await cflRomanaLoadService.createNPartidaJob({ centro: req.body?.centro, nPartida: req.body?.n_partida, authnClaims: req.authnClaims });
    res.status(202).json({ data: result });
  } catch (error) { next(error); }
});

router.post("/guia", requirePermission("fletes.sap.etl.ejecutar"), async (req, res, next) => {
  try {
    const result = await cflRomanaLoadService.createGuiaJob({ centro: req.body?.centro, guia: req.body?.guia, authnClaims: req.authnClaims });
    res.status(202).json({ data: result });
  } catch (error) { next(error); }
});

module.exports = { fletesRomanaLoadsRouter: router };
