const express = require("express");
const { resolveAuthContext } = require("../authz");

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

module.exports = {
  authRouter: router,
};
