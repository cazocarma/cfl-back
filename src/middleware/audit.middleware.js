const { recordAuditForRequest, shouldAuditRequest } = require("../audit");
const { logger } = require("../logger");

function auditMiddleware(req, res, next) {
  if (!shouldAuditRequest(req)) {
    return next();
  }

  let responseBody = null;

  const originalJson = res.json.bind(res);
  res.json = (body) => {
    responseBody = body;
    return originalJson(body);
  };

  const originalSend = res.send.bind(res);
  res.send = (body) => {
    if (responseBody === null) {
      responseBody = body;
    }
    return originalSend(body);
  };

  res.once("finish", () => {
    recordAuditForRequest(req, res.statusCode, responseBody).catch(
      (err) => logger.error({ err: err.message }, "audit record failed")
    );
  });

  return next();
}

module.exports = {
  auditMiddleware,
};
