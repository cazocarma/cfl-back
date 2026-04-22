const { ZodError } = require("zod");

/**
 * Middleware factory que valida req.body, req.query y/o req.params contra schemas Zod.
 *
 * Uso:
 *   const { z } = require("zod");
 *   const { validate } = require("../middleware/validate.middleware");
 *
 *   router.post("/ruta",
 *     validate({ body: z.object({ email: z.string().email() }) }),
 *     handler
 *   );
 */
function validate(schemas) {
  return (req, res, next) => {
    const errors = [];

    for (const source of ["params", "query", "body"]) {
      const schema = schemas[source];
      if (!schema) continue;

      const result = schema.safeParse(req[source]);
      if (!result.success) {
        for (const issue of result.error.issues) {
          errors.push({
            source,
            path: issue.path.join("."),
            message: issue.message,
          });
        }
      } else {
        req[source] = result.data;
      }
    }

    if (errors.length > 0) {
      // Primer error como mensaje humano: "campo.anidado: mensaje específico".
      // Permite que la UI muestre la causa exacta sin forzar al usuario a inspeccionar
      // la consola. `details` se mantiene para casos avanzados.
      const first = errors[0];
      const human = first.path
        ? `${first.path}: ${first.message}`
        : first.message;
      return res.status(400).json({
        error: `Error de validación · ${human}`,
        details: errors,
      });
    }

    return next();
  };
}

module.exports = { validate };
