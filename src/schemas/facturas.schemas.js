const { z } = require("zod");

const grupoMovimientos = z.object({
  ids_cabecera_flete: z.array(z.coerce.number().int().positive()).min(1),
});

const previewBody = z.object({
  id_empresa: z.coerce.number().int().positive(),
  grupos: z.array(grupoMovimientos).min(1).max(100),
});

const generarBody = previewBody;

const agregarMovimientosBody = z.object({
  ids_cabecera_flete: z.array(z.coerce.number().int().positive()).min(1).max(500),
});

const actualizarFacturaBody = z.object({
  observaciones: z.string().max(500).optional().nullable(),
  criterio_agrupacion: z.enum(["centro_costo", "tipo_flete"]).optional().nullable(),
});

const cambiarEstadoBody = z.object({
  estado: z.enum(["anulada", "recibida"]),
  numero_factura_recibida: z.string().max(60).optional().nullable(),
});

const idParam = z.object({
  id: z.coerce.number().int().positive(),
});

module.exports = {
  previewBody,
  generarBody,
  agregarMovimientosBody,
  actualizarFacturaBody,
  cambiarEstadoBody,
  idParam,
};
