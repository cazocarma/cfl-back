const { z } = require("zod");

const crearUsuarioBody = z.object({
  IdUsuario: z.coerce.number().int().positive().optional().nullable(),
  id_usuario: z.coerce.number().int().positive().optional().nullable(),
  username: z.string().trim().min(1).max(100),
  email: z.string().trim().min(1).max(255).email("Formato de email invalido"),
  password: z.string().min(8, "Minimo 8 caracteres").max(128),
  nombre: z.string().max(100).optional().nullable(),
  apellido: z.string().max(100).optional().nullable(),
  activo: z.union([z.boolean(), z.literal(0), z.literal(1)]).optional(),
  id_rol: z.coerce.number().int().positive().optional().nullable(),
});

const actualizarUsuarioBody = z.object({
  username: z.string().trim().min(1).max(100).optional(),
  email: z.string().trim().max(255).email("Formato de email invalido").optional(),
  password: z.string().min(8, "Minimo 8 caracteres").max(128).optional(),
  nombre: z.string().max(100).optional().nullable(),
  apellido: z.string().max(100).optional().nullable(),
  activo: z.union([z.boolean(), z.literal(0), z.literal(1)]).optional(),
  id_rol: z.coerce.number().int().positive().optional().nullable(),
});

module.exports = { crearUsuarioBody, actualizarUsuarioBody };
