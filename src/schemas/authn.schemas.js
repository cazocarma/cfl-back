const { z } = require("zod");

const loginBody = z.object({
  email: z.string().trim().min(1, "Email es requerido").max(255).email("Formato de email invalido"),
  password: z.string().min(1, "Contraseña es requerida").max(128),
});

module.exports = { loginBody };
