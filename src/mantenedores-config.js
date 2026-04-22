const MAINTAINERS = {
  temporadas: {
    title: "Temporadas",
    table: "[cfl].[Temporada]",
    alias: "t",
    idColumn: "IdTemporada",
    orderBy: "t.FechaInicio DESC",
    listColumns: [
      "t.IdTemporada",
      "t.Codigo",
      "t.Nombre",
      "t.FechaInicio",
      "t.FechaFin",
      "t.Activa",
      "t.Cerrada",
      "t.FechaCierre",
      "t.IdUsuarioCierre",
      "t.ObservacionCierre",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["Codigo", "Nombre", "FechaInicio", "FechaFin"],
      optional: ["Activa", "Cerrada", "FechaCierre", "IdUsuarioCierre", "ObservacionCierre"]
    },
    update: {
      allowed: [
        "Codigo",
        "Nombre",
        "FechaInicio",
        "FechaFin",
        "Activa",
        "Cerrada",
        "FechaCierre",
        "IdUsuarioCierre",
        "ObservacionCierre"
      ]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activa"
  },
  "centros-costo": {
    title: "Centros de Costo",
    table: "[cfl].[CentroCosto]",
    alias: "t",
    idColumn: "IdCentroCosto",
    orderBy: "t.Nombre ASC",
    listColumns: ["t.IdCentroCosto", "t.SapCodigo", "t.Nombre", "t.Activo"],
    create: {
      required: ["SapCodigo", "Nombre"],
      optional: ["Activo"]
    },
    update: {
      allowed: ["SapCodigo", "Nombre", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  "tipos-flete": {
    title: "Tipos de Flete",
    table: "[cfl].[TipoFlete]",
    alias: "t",
    idColumn: "IdTipoFlete",
    orderBy: "t.Nombre ASC",
    listColumns: [
      "t.IdTipoFlete",
      "t.SapCodigo",
      "t.Nombre",
      "t.Activo"
    ],
    create: {
      required: ["SapCodigo", "Nombre"],
      optional: ["Activo"]
    },
    update: {
      allowed: ["SapCodigo", "Nombre", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  "imputaciones-flete": {
    title: "Imputaciones de Flete",
    table: "[cfl].[ImputacionFlete]",
    alias: "t",
    idColumn: "IdImputacionFlete",
    from: "[cfl].[ImputacionFlete] t INNER JOIN [cfl].[TipoFlete] tf ON tf.IdTipoFlete = t.IdTipoFlete INNER JOIN [cfl].[CentroCosto] cc ON cc.IdCentroCosto = t.IdCentroCosto INNER JOIN [cfl].[CuentaMayor] cm ON cm.IdCuentaMayor = t.IdCuentaMayor",
    orderBy: "tf.Nombre ASC, cc.SapCodigo ASC, cm.Codigo ASC",
    listColumns: [
      "t.IdImputacionFlete",
      "t.IdTipoFlete",
      "tf.SapCodigo AS TipoFleteSapCodigo",
      "tf.Nombre AS TipoFleteNombre",
      "t.IdCentroCosto",
      "cc.SapCodigo AS CentroCostoSapCodigo",
      "cc.Nombre AS CentroCostoNombre",
      "t.IdCuentaMayor",
      "cm.Codigo AS CuentaMayorCodigo",
      "cm.Glosa AS CuentaMayorGlosa",
      "t.Activo",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["IdTipoFlete", "IdCentroCosto", "IdCuentaMayor"],
      optional: ["Activo"]
    },
    update: {
      allowed: ["IdTipoFlete", "IdCentroCosto", "IdCuentaMayor", "Activo"]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  "detalles-viaje": {
    title: "Detalles de Viaje",
    table: "[cfl].[DetalleViaje]",
    alias: "t",
    idColumn: "IdDetalleViaje",
    orderBy: "t.Descripcion ASC",
    listColumns: ["t.IdDetalleViaje", "t.Descripcion", "t.Observacion", "t.Activo"],
    create: {
      required: ["Descripcion"],
      optional: ["Observacion", "Activo"]
    },
    update: {
      allowed: ["Descripcion", "Observacion", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  especies: {
    title: "Especies",
    table: "[cfl].[Especie]",
    alias: "t",
    idColumn: "IdEspecie",
    orderBy: "t.Glosa ASC",
    listColumns: ["t.IdEspecie", "t.Glosa"],
    create: {
      required: ["Glosa"],
      optional: []
    },
    update: {
      allowed: ["Glosa"]
    }
  },
  nodos: {
    title: "Nodos Logisticos",
    table: "[cfl].[NodoLogistico]",
    alias: "t",
    idColumn: "IdNodo",
    orderBy: "t.Nombre ASC",
    listColumns: ["t.IdNodo", "t.Nombre", "t.Region", "t.Comuna", "t.Ciudad", "t.Calle", "t.Activo"],
    create: {
      required: ["Nombre", "Region", "Comuna", "Ciudad", "Calle"],
      optional: ["Activo"]
    },
    update: {
      allowed: ["Nombre", "Region", "Comuna", "Ciudad", "Calle", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  rutas: {
    title: "Rutas",
    table: "[cfl].[Ruta]",
    alias: "t",
    idColumn: "IdRuta",
    from: "[cfl].[Ruta] t INNER JOIN [cfl].[NodoLogistico] no ON no.IdNodo = t.IdOrigenNodo INNER JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = t.IdDestinoNodo",
    orderBy: "t.NombreRuta ASC",
    listColumns: [
      "t.IdRuta",
      "t.IdOrigenNodo",
      "no.Nombre AS OrigenNombre",
      "t.IdDestinoNodo",
      "nd.Nombre AS DestinoNombre",
      "t.NombreRuta",
      "t.DistanciaKm",
      "t.Activo",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["IdOrigenNodo", "IdDestinoNodo", "NombreRuta"],
      optional: ["DistanciaKm", "Activo"]
    },
    update: {
      allowed: ["IdOrigenNodo", "IdDestinoNodo", "NombreRuta", "DistanciaKm", "Activo"]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  "tipos-camion": {
    title: "Tipos de Camion",
    table: "[cfl].[TipoCamion]",
    alias: "t",
    idColumn: "IdTipoCamion",
    orderBy: "t.Nombre ASC",
    listColumns: [
      "t.IdTipoCamion",
      "t.Nombre",
      "t.Categoria",
      "t.CapacidadKg",
      "t.RequiereTemperatura",
      "t.Descripcion",
      "t.Activo"
    ],
    create: {
      required: ["Nombre", "Categoria", "CapacidadKg", "RequiereTemperatura"],
      optional: ["Descripcion", "Activo"]
    },
    update: {
      allowed: ["Nombre", "Categoria", "CapacidadKg", "RequiereTemperatura", "Descripcion", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  camiones: {
    title: "Camiones",
    table: "[cfl].[Camion]",
    alias: "t",
    idColumn: "IdCamion",
    from: "[cfl].[Camion] t INNER JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = t.IdTipoCamion",
    orderBy: "t.SapPatente ASC, t.SapCarro ASC",
    listColumns: [
      "t.IdCamion",
      "t.IdTipoCamion",
      "tc.Nombre AS TipoCamionNombre",
      "t.SapPatente",
      "t.SapCarro",
      "t.Activo",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["IdTipoCamion", "SapPatente", "SapCarro"],
      optional: ["Activo"]
    },
    update: {
      allowed: ["IdTipoCamion", "SapPatente", "SapCarro", "Activo"]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  "empresas-transporte": {
    title: "Empresas de Transporte",
    table: "[cfl].[EmpresaTransporte]",
    alias: "t",
    idColumn: "IdEmpresa",
    orderBy: "t.RazonSocial ASC",
    listColumns: [
      "t.IdEmpresa",
      "t.SapCodigo",
      "t.Rut",
      "t.RazonSocial",
      "t.NombreRepresentante",
      "t.Correo",
      "t.Telefono",
      "t.Activo",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["Rut"],
      optional: ["SapCodigo", "RazonSocial", "NombreRepresentante", "Correo", "Telefono", "Activo"]
    },
    update: {
      allowed: ["SapCodigo", "Rut", "RazonSocial", "NombreRepresentante", "Correo", "Telefono", "Activo"]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  productores: {
    title: "Productores",
    table: "[cfl].[Productor]",
    alias: "t",
    idColumn: "IdProductor",
    orderBy: "t.Nombre ASC",
    unlimited: true,
    listColumns: [
      "t.IdProductor",
      "t.CodigoProveedor",
      "t.Rut",
      "t.Nombre",
      "t.Pais",
      "t.Region",
      "t.Comuna",
      "t.Distrito",
      "t.Calle",
      "t.Email",
      "t.OrganizacionCompra",
      "t.MonedaPedido",
      "t.CondicionPago",
      "t.Incoterm",
      "t.Sociedad",
      "t.CuentaAsociada",
      "t.Activo",
      "t.FechaActualizacionSap",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["CodigoProveedor", "Nombre"],
      optional: [
        "Rut",
        "Pais",
        "Region",
        "Comuna",
        "Distrito",
        "Calle",
        "Email",
        "OrganizacionCompra",
        "MonedaPedido",
        "CondicionPago",
        "Incoterm",
        "Sociedad",
        "CuentaAsociada",
        "Activo",
        "FechaActualizacionSap"
      ]
    },
    update: {
      allowed: [
        "CodigoProveedor",
        "Rut",
        "Nombre",
        "Pais",
        "Region",
        "Comuna",
        "Distrito",
        "Calle",
        "Email",
        "OrganizacionCompra",
        "MonedaPedido",
        "CondicionPago",
        "Incoterm",
        "Sociedad",
        "CuentaAsociada",
        "Activo",
        "FechaActualizacionSap"
      ]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  choferes: {
    title: "Choferes",
    table: "[cfl].[Chofer]",
    alias: "t",
    idColumn: "IdChofer",
    orderBy: "t.SapNombre ASC",
    // SapIdFiscalNorm se incluye para habilitar busqueda tolerante
    // en el frontend (no se renderiza como columna visible).
    listColumns: ["t.IdChofer", "t.SapIdFiscal", "t.SapIdFiscalNorm", "t.SapNombre", "t.Telefono", "t.Activo"],
    create: {
      required: ["SapIdFiscal", "SapNombre"],
      optional: ["Telefono", "Activo"]
    },
    update: {
      allowed: ["SapIdFiscal", "SapNombre", "Telefono", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  tarifas: {
    title: "Tarifas",
    table: "[cfl].[Tarifa]",
    alias: "t",
    idColumn: "IdTarifa",
    from: "[cfl].[Tarifa] t INNER JOIN [cfl].[TipoCamion] tc ON tc.IdTipoCamion = t.IdTipoCamion INNER JOIN [cfl].[Temporada] tp ON tp.IdTemporada = t.IdTemporada INNER JOIN [cfl].[Ruta] r ON r.IdRuta = t.IdRuta INNER JOIN [cfl].[NodoLogistico] no ON no.IdNodo = r.IdOrigenNodo INNER JOIN [cfl].[NodoLogistico] nd ON nd.IdNodo = r.IdDestinoNodo",
    orderBy: "t.IdTarifa DESC",
    listColumns: [
      "t.IdTarifa",
      "t.IdTipoCamion",
      "tc.Nombre AS TipoCamionNombre",
      "t.IdTemporada",
      "tp.Codigo AS TemporadaCodigo",
      "tp.Nombre AS TemporadaNombre",
      "t.IdRuta",
      "r.NombreRuta",
      "no.Nombre AS RutaOrigenNombre",
      "nd.Nombre AS RutaDestinoNombre",
      "t.VigenciaDesde",
      "t.VigenciaHasta",
      "t.Prioridad",
      "t.Regla",
      "t.Moneda",
      "t.MontoFijo",
      "t.Activo",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["IdTipoCamion", "IdTemporada", "IdRuta", "VigenciaDesde", "Prioridad", "Regla", "Moneda", "MontoFijo"],
      optional: ["VigenciaHasta", "Activo"]
    },
    update: {
      allowed: [
        "IdTipoCamion",
        "IdTemporada",
        "IdRuta",
        "VigenciaDesde",
        "VigenciaHasta",
        "Prioridad",
        "Regla",
        "Moneda",
        "MontoFijo",
        "Activo"
      ]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  "cuentas-mayor": {
    title: "Cuentas Mayores",
    table: "[cfl].[CuentaMayor]",
    alias: "t",
    idColumn: "IdCuentaMayor",
    orderBy: "t.Codigo ASC",
    listColumns: ["t.IdCuentaMayor", "t.Codigo", "t.Glosa"],
    create: {
      required: ["Codigo", "Glosa"],
      optional: []
    },
    update: {
      allowed: ["Codigo", "Glosa"]
    }
  },
  usuarios: {
    title: "Usuarios",
    table: "[cfl].[Usuario]",
    alias: "t",
    idColumn: "IdUsuario",
    orderBy: "t.Username ASC",
    listColumns: [
      "t.IdUsuario",
      "t.Username",
      "t.Email",
      "t.Nombre",
      "t.Apellido",
      "t.Activo",
      "t.UltimoLogin",
      "t.FechaCreacion",
      "t.FechaActualizacion"
    ],
    create: {
      required: ["Username", "Email", "PasswordHash"],
      optional: ["Nombre", "Apellido", "Activo"]
    },
    update: {
      allowed: ["Username", "Email", "PasswordHash", "Nombre", "Apellido", "Activo", "UltimoLogin"]
    },
    timestamps: {
      created: "FechaCreacion",
      updated: "FechaActualizacion"
    },
    softDeleteColumn: "Activo"
  },
  roles: {
    title: "Roles",
    table: "[cfl].[Rol]",
    alias: "t",
    idColumn: "IdRol",
    orderBy: "t.Nombre ASC",
    listColumns: ["t.IdRol", "t.Nombre", "t.Descripcion", "t.Activo"],
    create: {
      required: ["Nombre"],
      optional: ["Descripcion", "Activo"]
    },
    update: {
      allowed: ["Nombre", "Descripcion", "Activo"]
    },
    softDeleteColumn: "Activo"
  },
  permisos: {
    title: "Permisos",
    table: "[cfl].[Permiso]",
    alias: "t",
    idColumn: "IdPermiso",
    orderBy: "t.Clave ASC",
    listColumns: ["t.IdPermiso", "t.Clave", "t.Recurso", "t.Accion", "t.Descripcion", "t.Activo"],
    create: {
      required: ["Clave", "Recurso", "Accion"],
      optional: ["Descripcion", "Activo"]
    },
    update: {
      allowed: ["Clave", "Recurso", "Accion", "Descripcion", "Activo"]
    },
    softDeleteColumn: "Activo"
  }
};

module.exports = {
  MAINTAINERS
};
