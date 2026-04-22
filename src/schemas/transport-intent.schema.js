const { z } = require("zod");

// ────────────────────────────────────────────────────────────────────
// Seccion `transport` opcional en el body de POST/PUT de fletes.
// Representa la intencion transaccional del usuario sobre las
// entidades de transporte (empresa, chofer, camion).
//
// Modos por entidad:
//   matched          → ya se eligio un id existente; no tocar mantenedor.
//   pending_create   → hint SAP/Romana sin match; CREAR antes de asociar.
//   update           → id existente + diff de fields; UPDATE mantenedor.
//   empty            → nada que hacer.
//
// Ademas `update_tipo_camion` cambia el IdTipoCamion del camion
// existente (requiere confirmacion UX checkbox = true).
// ────────────────────────────────────────────────────────────────────

const entityMode = z.enum(["matched", "pending_create", "update", "empty"]);

const empresaPendingCreate = z.object({
  rut: z.string().trim().min(1).max(20),
  sap_codigo: z.string().trim().max(10).optional().nullable(),
  razon_social: z.string().trim().max(100).optional().nullable(),
  nombre_representante: z.string().trim().max(100).optional().nullable(),
  correo: z.string().trim().max(100).optional().nullable(),
  telefono: z.string().trim().max(20).optional().nullable(),
  activo: z.coerce.boolean().optional().default(true),
});

const empresaUpdate = z.object({
  id_empresa_transporte: z.coerce.number().int().positive(),
  fields: z.object({
    rut: z.string().trim().max(20).optional().nullable(),
    sap_codigo: z.string().trim().max(10).optional().nullable(),
    razon_social: z.string().trim().max(100).optional().nullable(),
    nombre_representante: z.string().trim().max(100).optional().nullable(),
    correo: z.string().trim().max(100).optional().nullable(),
    telefono: z.string().trim().max(20).optional().nullable(),
    activo: z.coerce.boolean().optional(),
  }).partial(),
});

const empresaIntent = z.object({
  mode: entityMode,
  pending_create: empresaPendingCreate.optional(),
  update: empresaUpdate.optional(),
});

const choferPendingCreate = z.object({
  sap_id_fiscal: z.string().trim().min(1).max(24),
  sap_nombre: z.string().trim().min(1).max(80),
  telefono: z.string().trim().max(30).optional().nullable(),
  activo: z.coerce.boolean().optional().default(true),
});

const choferUpdate = z.object({
  id_chofer: z.coerce.number().int().positive(),
  fields: z.object({
    sap_id_fiscal: z.string().trim().max(24).optional().nullable(),
    sap_nombre: z.string().trim().max(80).optional().nullable(),
    telefono: z.string().trim().max(30).optional().nullable(),
    activo: z.coerce.boolean().optional(),
  }).partial(),
});

const choferIntent = z.object({
  mode: entityMode,
  pending_create: choferPendingCreate.optional(),
  update: choferUpdate.optional(),
});

const camionPendingCreate = z.object({
  sap_patente: z.string().trim().min(1).max(20),
  sap_carro: z.string().trim().max(20).optional().nullable(),
  id_tipo_camion: z.coerce.number().int().positive(),
  activo: z.coerce.boolean().optional().default(true),
});

const camionUpdate = z.object({
  id_camion: z.coerce.number().int().positive(),
  fields: z.object({
    sap_patente: z.string().trim().max(20).optional().nullable(),
    sap_carro: z.string().trim().max(20).optional().nullable(),
    id_tipo_camion: z.coerce.number().int().positive().optional().nullable(),
    activo: z.coerce.boolean().optional(),
  }).partial(),
});

const camionTipoChange = z.object({
  id_camion: z.coerce.number().int().positive(),
  from_id_tipo_camion: z.coerce.number().int().positive().optional().nullable(),
  to_id_tipo_camion: z.coerce.number().int().positive(),
});

const camionIntent = z.object({
  mode: entityMode,
  pending_create: camionPendingCreate.optional(),
  update: camionUpdate.optional(),
  update_tipo_camion: camionTipoChange.optional(),
});

const routeContext = z.object({
  id_ruta: z.coerce.number().int().positive().optional().nullable(),
  fecha_salida: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional().nullable(),
});

const transportIntent = z.object({
  empresa: empresaIntent.optional(),
  chofer: choferIntent.optional(),
  camion: camionIntent.optional(),
  recalc_tarifa: z.coerce.boolean().optional().default(false),
  route_context: routeContext.optional(),
}).optional();

module.exports = {
  transportIntent,
};
