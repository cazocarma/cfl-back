const { recordAuditForRequest, shouldAuditRequest } = require("../audit");

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
    recordAuditForRequest(req, res.statusCode, responseBody).catch(() => {});
  });

  return next();
}

module.exports = {
  auditMiddleware,
};
