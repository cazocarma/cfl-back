const { z } = require('zod');

const generarBody = z.object({
  id_factura: z.coerce.number().int().positive(),
  fecha_documento: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  fecha_contabilizacion: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  glosa_cabecera: z.string().min(1).max(100),
  temporada: z.string().max(20).optional().nullable(),
  codigo_cargo_abono: z.string().max(20).optional().nullable(),
  glosa_cargo_abono: z.string().max(100).optional().nullable(),
  indicador_impuesto: z.string().max(10).default('C0'),
  productores_oc: z.array(z.object({
    id_productor: z.coerce.number().int().positive(),
    orden_compra: z.string().max(30),
    posicion_oc: z.string().max(10).default('10'),
  })).optional(),
});

const cambiarEstadoBody = z.object({
  estado: z.enum(['descargada', 'contabilizada']),
});

const idParam = z.object({
  id: z.coerce.number().int().positive(),
});

module.exports = { generarBody, cambiarEstadoBody, idParam };
