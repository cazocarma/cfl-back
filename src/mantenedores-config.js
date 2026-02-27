const EMPRESAS_TRANSPORTE_CONFIG = {
  title: "Empresas de Transporte",
  table: "[cfl].[CFL_empresa_transporte]",
  alias: "t",
  idColumn: "id_empresa",
  orderBy: "t.razon_social ASC",
  listColumns: [
    "t.id_empresa",
    "t.sap_codigo",
    "t.rut",
    "t.razon_social",
    "t.nombre_rep",
    "t.correo",
    "t.telefono",
    "t.activo",
    "t.created_at",
    "t.updated_at"
  ],
  create: {
    required: ["rut"],
    optional: ["sap_codigo", "razon_social", "nombre_rep", "correo", "telefono", "activo"]
  },
  update: {
    allowed: ["sap_codigo", "rut", "razon_social", "nombre_rep", "correo", "telefono", "activo"]
  },
  timestamps: {
    created: "created_at",
    updated: "updated_at"
  },
  softDeleteColumn: "activo"
};

const MAINTAINERS = {
  temporadas: {
    title: "Temporadas",
    table: "[cfl].[CFL_temporada]",
    alias: "t",
    idColumn: "id_temporada",
    orderBy: "t.fecha_inicio DESC",
    listColumns: [
      "t.id_temporada",
      "t.codigo",
      "t.nombre",
      "t.fecha_inicio",
      "t.fecha_fin",
      "t.activa",
      "t.cerrada",
      "t.fecha_cierre",
      "t.id_usuario_cierre",
      "t.observacion_cierre",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["codigo", "nombre", "fecha_inicio", "fecha_fin"],
      optional: ["activa", "cerrada", "fecha_cierre", "id_usuario_cierre", "observacion_cierre"]
    },
    update: {
      allowed: [
        "codigo",
        "nombre",
        "fecha_inicio",
        "fecha_fin",
        "activa",
        "cerrada",
        "fecha_cierre",
        "id_usuario_cierre",
        "observacion_cierre"
      ]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    },
    softDeleteColumn: "activa"
  },
  "centros-costo": {
    title: "Centros de Costo",
    table: "[cfl].[CFL_centro_costo]",
    alias: "t",
    idColumn: "id_centro_costo",
    orderBy: "t.nombre ASC",
    listColumns: ["t.id_centro_costo", "t.sap_codigo", "t.nombre", "t.activo"],
    create: {
      required: ["sap_codigo", "nombre"],
      optional: ["activo"]
    },
    update: {
      allowed: ["sap_codigo", "nombre", "activo"]
    },
    softDeleteColumn: "activo"
  },
  "tipos-flete": {
    title: "Tipos de Flete",
    table: "[cfl].[CFL_tipo_flete]",
    alias: "t",
    idColumn: "id_tipo_flete",
    from: "[cfl].[CFL_tipo_flete] t INNER JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = t.id_centro_costo",
    orderBy: "t.nombre ASC",
    listColumns: [
      "t.id_tipo_flete",
      "t.sap_codigo",
      "t.nombre",
      "t.activo",
      "t.id_centro_costo",
      "cc.sap_codigo AS centro_costo_sap_codigo",
      "cc.nombre AS centro_costo_nombre"
    ],
    create: {
      required: ["sap_codigo", "nombre", "id_centro_costo"],
      optional: ["activo"]
    },
    update: {
      allowed: ["sap_codigo", "nombre", "id_centro_costo", "activo"]
    },
    softDeleteColumn: "activo"
  },
  "detalles-viaje": {
    title: "Detalles de Viaje",
    table: "[cfl].[CFL_detalle_viaje]",
    alias: "t",
    idColumn: "id_detalle_viaje",
    orderBy: "t.descripcion ASC",
    listColumns: ["t.id_detalle_viaje", "t.descripcion", "t.observacion", "t.activo"],
    create: {
      required: ["descripcion"],
      optional: ["observacion", "activo"]
    },
    update: {
      allowed: ["descripcion", "observacion", "activo"]
    },
    softDeleteColumn: "activo"
  },
  especies: {
    title: "Especies",
    table: "[cfl].[CFL_especie]",
    alias: "t",
    idColumn: "id_especie",
    orderBy: "t.glosa ASC",
    listColumns: ["t.id_especie", "t.glosa"],
    create: {
      required: ["glosa"],
      optional: []
    },
    update: {
      allowed: ["glosa"]
    }
  },
  nodos: {
    title: "Nodos Logisticos",
    table: "[cfl].[CFL_nodo_logistico]",
    alias: "t",
    idColumn: "id_nodo",
    orderBy: "t.nombre ASC",
    listColumns: ["t.id_nodo", "t.nombre", "t.region", "t.comuna", "t.ciudad", "t.calle", "t.activo"],
    create: {
      required: ["nombre", "region", "comuna", "ciudad", "calle"],
      optional: ["activo"]
    },
    update: {
      allowed: ["nombre", "region", "comuna", "ciudad", "calle", "activo"]
    },
    softDeleteColumn: "activo"
  },
  rutas: {
    title: "Rutas",
    table: "[cfl].[CFL_ruta]",
    alias: "t",
    idColumn: "id_ruta",
    from: "[cfl].[CFL_ruta] t INNER JOIN [cfl].[CFL_nodo_logistico] no ON no.id_nodo = t.id_origen_nodo INNER JOIN [cfl].[CFL_nodo_logistico] nd ON nd.id_nodo = t.id_destino_nodo",
    orderBy: "t.nombre_ruta ASC",
    listColumns: [
      "t.id_ruta",
      "t.id_origen_nodo",
      "no.nombre AS origen_nombre",
      "t.id_destino_nodo",
      "nd.nombre AS destino_nombre",
      "t.nombre_ruta",
      "t.distancia_km",
      "t.activo",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["id_origen_nodo", "id_destino_nodo", "nombre_ruta"],
      optional: ["distancia_km", "activo"]
    },
    update: {
      allowed: ["id_origen_nodo", "id_destino_nodo", "nombre_ruta", "distancia_km", "activo"]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    },
    softDeleteColumn: "activo"
  },
  "tipos-camion": {
    title: "Tipos de Camion",
    table: "[cfl].[CFL_tipo_camion]",
    alias: "t",
    idColumn: "id_tipo_camion",
    orderBy: "t.nombre ASC",
    listColumns: [
      "t.id_tipo_camion",
      "t.nombre",
      "t.categoria",
      "t.capacidad_kg",
      "t.requiere_temperatura",
      "t.descripcion",
      "t.activo"
    ],
    create: {
      required: ["nombre", "categoria", "capacidad_kg", "requiere_temperatura"],
      optional: ["descripcion", "activo"]
    },
    update: {
      allowed: ["nombre", "categoria", "capacidad_kg", "requiere_temperatura", "descripcion", "activo"]
    },
    softDeleteColumn: "activo"
  },
  camiones: {
    title: "Camiones",
    table: "[cfl].[CFL_camion]",
    alias: "t",
    idColumn: "id_camion",
    from: "[cfl].[CFL_camion] t INNER JOIN [cfl].[CFL_tipo_camion] tc ON tc.id_tipo_camion = t.id_tipo_camion",
    orderBy: "t.sap_patente ASC, t.sap_carro ASC",
    listColumns: [
      "t.id_camion",
      "t.id_tipo_camion",
      "tc.nombre AS tipo_camion_nombre",
      "t.sap_patente",
      "t.sap_carro",
      "t.activo",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["id_tipo_camion", "sap_patente", "sap_carro"],
      optional: ["activo"]
    },
    update: {
      allowed: ["id_tipo_camion", "sap_patente", "sap_carro", "activo"]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    },
    softDeleteColumn: "activo"
  },
  "empresas-transporte": EMPRESAS_TRANSPORTE_CONFIG,
  choferes: {
    title: "Choferes",
    table: "[cfl].[CFL_chofer]",
    alias: "t",
    idColumn: "id_chofer",
    orderBy: "t.sap_nombre ASC",
    listColumns: ["t.id_chofer", "t.sap_id_fiscal", "t.sap_nombre", "t.telefono", "t.activo"],
    create: {
      required: ["sap_id_fiscal", "sap_nombre"],
      optional: ["telefono", "activo"]
    },
    update: {
      allowed: ["sap_id_fiscal", "sap_nombre", "telefono", "activo"]
    },
    softDeleteColumn: "activo"
  },
  tarifas: {
    title: "Tarifas",
    table: "[cfl].[CFL_tarifa]",
    alias: "t",
    idColumn: "id_tarifa",
    from: "[cfl].[CFL_tarifa] t INNER JOIN [cfl].[CFL_tipo_camion] tc ON tc.id_tipo_camion = t.id_tipo_camion INNER JOIN [cfl].[CFL_temporada] tp ON tp.id_temporada = t.id_temporada INNER JOIN [cfl].[CFL_ruta] r ON r.id_ruta = t.id_ruta INNER JOIN [cfl].[CFL_nodo_logistico] no ON no.id_nodo = r.id_origen_nodo INNER JOIN [cfl].[CFL_nodo_logistico] nd ON nd.id_nodo = r.id_destino_nodo",
    orderBy: "t.id_tarifa DESC",
    listColumns: [
      "t.id_tarifa",
      "t.id_tipo_camion",
      "tc.nombre AS tipo_camion_nombre",
      "t.id_temporada",
      "tp.codigo AS temporada_codigo",
      "t.id_ruta",
      "r.nombre_ruta",
      "no.nombre AS ruta_origen_nombre",
      "nd.nombre AS ruta_destino_nombre",
      "t.vigencia_desde",
      "t.vigencia_hasta",
      "t.prioridad",
      "t.regla",
      "t.moneda",
      "t.monto_fijo",
      "t.activo",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["id_tipo_camion", "id_temporada", "id_ruta", "vigencia_desde", "prioridad", "regla", "moneda", "monto_fijo"],
      optional: ["vigencia_hasta", "activo"]
    },
    update: {
      allowed: [
        "id_tipo_camion",
        "id_temporada",
        "id_ruta",
        "vigencia_desde",
        "vigencia_hasta",
        "prioridad",
        "regla",
        "moneda",
        "monto_fijo",
        "activo"
      ]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    },
    softDeleteColumn: "activo"
  },
  "cuentas-mayor": {
    title: "Cuentas Mayores",
    table: "[cfl].[CFL_cuenta_mayor]",
    alias: "t",
    idColumn: "id_cuenta_mayor",
    orderBy: "t.codigo ASC",
    listColumns: ["t.id_cuenta_mayor", "t.codigo", "t.glosa"],
    create: {
      required: ["codigo", "glosa"],
      optional: []
    },
    update: {
      allowed: ["codigo", "glosa"]
    }
  },
  folios: {
    title: "Folios",
    table: "[cfl].[CFL_folio]",
    alias: "t",
    idColumn: "id_folio",
    from: "[cfl].[CFL_folio] t INNER JOIN [cfl].[CFL_temporada] tp ON tp.id_temporada = t.id_temporada INNER JOIN [cfl].[CFL_centro_costo] cc ON cc.id_centro_costo = t.id_centro_costo",
    orderBy: "t.created_at DESC",
    listColumns: [
      "t.id_folio",
      "t.id_centro_costo",
      "cc.sap_codigo AS centro_costo_sap_codigo",
      "cc.nombre AS centro_costo_nombre",
      "t.id_temporada",
      "tp.codigo AS temporada_codigo",
      "t.folio_numero",
      "t.periodo_desde",
      "t.periodo_hasta",
      "t.estado",
      "t.bloqueado",
      "t.fecha_cierre",
      "t.resultado_cuadratura",
      "t.resumen_cuadratura",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["id_centro_costo", "id_temporada", "folio_numero", "estado"],
      optional: ["periodo_desde", "periodo_hasta", "bloqueado", "fecha_cierre", "resultado_cuadratura", "resumen_cuadratura"]
    },
    update: {
      allowed: [
        "id_centro_costo",
        "id_temporada",
        "folio_numero",
        "periodo_desde",
        "periodo_hasta",
        "estado",
        "bloqueado",
        "fecha_cierre",
        "resultado_cuadratura",
        "resumen_cuadratura"
      ]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    }
  },
  usuarios: {
    title: "Usuarios",
    table: "[cfl].[CFL_usuario]",
    alias: "t",
    idColumn: "id_usuario",
    orderBy: "t.username ASC",
    listColumns: [
      "t.id_usuario",
      "t.username",
      "t.email",
      "t.nombre",
      "t.apellido",
      "t.activo",
      "t.ultimo_login",
      "t.created_at",
      "t.updated_at"
    ],
    create: {
      required: ["username", "email", "password_hash"],
      optional: ["nombre", "apellido", "activo"]
    },
    update: {
      allowed: ["username", "email", "password_hash", "nombre", "apellido", "activo", "ultimo_login"]
    },
    timestamps: {
      created: "created_at",
      updated: "updated_at"
    },
    softDeleteColumn: "activo"
  }
};

module.exports = {
  MAINTAINERS
};
