const { z } = require("zod");

const criterioEnum = z.enum(["centro_costo", "tipo_flete"]);

const previewBody = z.object({
  id_empresa: z.coerce.number().int().positive(),
  ids_folio: z.array(z.coerce.number().int().positive()).min(1).max(500),
  criterio: criterioEnum,
});

const generarBody = previewBody;

const agregarFoliosBody = z.object({
  ids_folio: z.array(z.coerce.number().int().positive()).min(1).max(500),
});

const actualizarFacturaBody = z.object({
  observaciones: z.string().max(500).optional().nullable(),
  criterio_agrupacion: criterioEnum.optional().nullable(),
});

const cambiarEstadoBody = z.object({
  estado: z.enum(["emitida", "anulada"]),
});

const idParam = z.object({
  id: z.coerce.number().int().positive(),
});

module.exports = {
  previewBody,
  generarBody,
  agregarFoliosBody,
  actualizarFacturaBody,
  cambiarEstadoBody,
  idParam,
};
