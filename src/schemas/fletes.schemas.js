const { z } = require("zod");

const detalle = z.object({
  material: z.string().max(50).optional(),
  descripcion: z.string().max(200).optional(),
  unidad: z.string().max(3).optional(),
  cantidad: z.coerce.number().min(0).optional(),
  peso: z.coerce.number().min(0).optional(),
  id_especie: z.coerce.number().int().positive().optional().nullable(),
});

const fleteManualBody = z.object({
  cabecera: z.object({
    id_tipo_flete: z.coerce.number().int().positive("Tipo de flete es requerido"),
    id_centro_costo: z.coerce.number().int().positive().optional().nullable(),
    id_cuenta_mayor: z.coerce.number().int().positive().optional().nullable(),
    id_imputacion_flete: z.coerce.number().int().positive().optional().nullable(),
    tipo_movimiento: z.string().max(50).optional().nullable(),
    estado: z.string().max(30).optional().nullable(),
    fecha_salida: z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "Formato fecha: YYYY-MM-DD"),
    hora_salida: z.string().regex(/^\d{2}:\d{2}(:\d{2})?$/, "Formato hora: HH:MM o HH:MM:SS"),
    monto_aplicado: z.coerce.number().min(0, "Monto no puede ser negativo").optional().nullable(),
    monto_extra: z.coerce.number().min(0, "Monto extra no puede ser negativo").optional().nullable(),
    guia_remision: z.string().max(25).optional().nullable(),
    numero_entrega: z.string().max(20).optional().nullable(),
    id_detalle_viaje: z.coerce.number().int().positive().optional().nullable(),
    id_productor: z.coerce.number().int().positive().optional().nullable(),
    id_tarifa: z.coerce.number().int().positive().optional().nullable(),
    sentido_flete: z.string().max(20).optional().nullable(),
    sap_numero_entrega: z.string().max(20).optional().nullable(),
    sap_codigo_tipo_flete: z.string().max(20).optional().nullable(),
    sap_centro_costo: z.string().max(20).optional().nullable(),
    sap_cuenta_mayor: z.string().max(20).optional().nullable(),
    observaciones: z.string().max(200).optional().nullable(),
    id_usuario_creador: z.coerce.number().int().positive().optional().nullable(),
  }),
  detalles: z.array(detalle).max(100, "Maximo 100 detalles por flete").default([]),
});

const fleteIdParam = z.object({
  id_cabecera_flete: z.coerce.number().int().positive(),
});

module.exports = { fleteManualBody, fleteIdParam };
