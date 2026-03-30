'use strict';

/**
 * Generador de archivo Excel para planilla SAP FI.
 *
 * Formato basado en la plantilla real de carga masiva SAP:
 * - 38 columnas con encabezados exactos
 * - Texto cabecera, referencia, fechas, temporada y cargo/abono en TODAS las líneas
 * - Centro de costo SOLO en línea de débito (cabecera X)
 * - Línea débito: glosa como texto por línea, sin NroAsignacion
 * - Líneas crédito: "FLETES FOLIO xxx" como texto, con NroAsignacion (especie)
 * - Líneas X resaltadas en amarillo
 */

const HEADERS = [
  'Indicador nuevo doc \'X\'',
  'N° de asiento',
  'VACÍO',
  'Soc FI',
  'Fecha doc',
  'Fecha contabilizacion',
  'Clase de doc',
  'Moneda',
  'Texto cabecera',
  'Referencia *',
  'clave contabilizacion',
  'Cuenta Mayor',
  'Cliente',
  'Proveedor',
  'Indicador CME',
  'Importe',
  'Orden CO',
  'centro de costo',
  'OC',
  'Posición',
  'Centro de beneficio',
  'Centro Gestor',
  'Area funcional',
  'División',
  'Condición de Pago *',
  'Fecha  Tesoreria/Base',
  'NroAsignacion',
  'Texto por linea',
  'Indicador de Impuesto',
  'Oficina de Ventas',
  'Clave de ref 1 (Temporada)',
  'Clave de ref 2 (Tipo Cargo/Abono)',
  'Segmento',
  'Indicador de retención',
  'POSPRE',
  'Indicador Impuesto Automático',
  'Descripción error de salida',
  'Fecha conversión',
];

/** Format date as DDMMYYYY string */
function fmtDate(d) {
  if (!d) return '';
  const dt = new Date(d);
  if (isNaN(dt.getTime())) return '';
  const dd = String(dt.getDate()).padStart(2, '0');
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const yyyy = dt.getFullYear();
  return `${dd}${mm}${yyyy}`;
}

/** Format amount with Chilean thousand separators, no decimals */
function fmtAmount(n) {
  const val = Number(n || 0);
  return val.toLocaleString('es-CL', { maximumFractionDigits: 0 });
}

/**
 * Genera un workbook Excel con la planilla SAP.
 * @param {object} planilla - Cabecera con .documentos[].lineas[]
 * @returns {import('exceljs').Workbook}
 */
function generateSapExcel(planilla) {
  const ExcelJS = require('exceljs');
  const wb = new ExcelJS.Workbook();
  wb.creator = 'CFL - Control de Fletes';
  wb.created = new Date();

  const ws = wb.addWorksheet('Planilla SAP', {
    views: [{ state: 'frozen', ySplit: 1 }],
  });

  // --- Encabezados ---
  const headerRow = ws.addRow(HEADERS);
  headerRow.font = { bold: true, size: 9 };
  headerRow.alignment = { vertical: 'middle', wrapText: true };
  headerRow.height = 30;
  for (let i = 1; i <= HEADERS.length; i++) {
    headerRow.getCell(i).fill = {
      type: 'pattern', pattern: 'solid',
      fgColor: { argb: 'FFE8F0FE' },
    };
    headerRow.getCell(i).border = {
      bottom: { style: 'thin', color: { argb: 'FF999999' } },
    };
  }

  // --- Valores comunes ---
  const fechaDoc    = fmtDate(planilla.fecha_documento);
  const fechaContab = fmtDate(planilla.fecha_contabilizacion);
  const glosa       = planilla.glosa_cabecera || '';
  const socFI       = planilla.sociedad_fi || '1000';
  const clasDoc     = planilla.clase_documento || 'KA';
  const moneda      = planilla.moneda || 'CLP';
  const temporada   = planilla.temporada || '';
  const cargoAbono  = planilla.codigo_cargo_abono || '';
  const indImpuesto = planilla.indicador_impuesto || 'C0';

  // Estilos
  const yellowFill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFFF00' } };
  const fontNormal = { size: 9 };
  const fontBold   = { size: 9, bold: true };

  // --- Filas de datos ---
  for (const doc of planilla.documentos || []) {
    // Referencia y texto por documento (cada doc pertenece a una factura)
    const referencia = doc.referencia || '';
    const textoCredito = doc.numero_pre_factura
      ? `PREFACTURA ${doc.numero_pre_factura}`
      : glosa;

    for (const linea of doc.lineas || []) {
      const isDebit  = linea.clave_contabilizacion === '50';
      const isHeader = !!linea.es_doc_nuevo;

      const values = [
        /* 01 */ isHeader ? 'X' : '',
        /* 02 */ isHeader ? '1' : '',
        /* 03 */ '',
        /* 04 */ socFI,
        /* 05 */ fechaDoc,
        /* 06 */ fechaContab,
        /* 07 */ clasDoc,
        /* 08 */ moneda,
        /* 09 */ glosa,
        /* 10 */ referencia,
        /* 11 */ linea.clave_contabilizacion || '',
        /* 12 */ isDebit ? (linea.cuenta_mayor || '') : '',
        /* 13 */ '',
        /* 14 */ !isDebit ? (linea.codigo_proveedor || '') : '',
        /* 15 */ !isDebit ? (linea.indicador_cme || 'A') : '',
        /* 16 */ fmtAmount(linea.importe),
        /* 17 */ '',
        /* 18 */ isDebit ? (linea.centro_costo || '') : '',
        /* 19 */ !isDebit ? (linea.orden_compra || '') : '',
        /* 20 */ !isDebit ? (linea.posicion_oc || '10') : '',
        /* 21 */ '', /* 22 */ '', /* 23 */ '', /* 24 */ '',
        /* 25 */ '',
        /* 26 */ '',
        /* 27 */ linea.nro_asignacion || '',
        /* 28 */ isDebit ? glosa : textoCredito,
        /* 29 */ indImpuesto,
        /* 30 */ '',
        /* 31 */ temporada,
        /* 32 */ cargoAbono,
        /* 33 */ '', /* 34 */ '', /* 35 */ '', /* 36 */ '', /* 37 */ '',
        /* 38 */ fechaDoc,
      ];

      const row = ws.addRow(values);
      row.font = isHeader ? fontBold : fontNormal;
      row.alignment = { vertical: 'middle' };

      // Resaltar líneas X (cabecera de asiento) en amarillo
      if (isHeader) {
        for (let i = 1; i <= HEADERS.length; i++) {
          row.getCell(i).fill = yellowFill;
        }
      }

      // Columna Importe (16): alinear derecha, color rojo si negativo
      const importeCell = row.getCell(16);
      importeCell.alignment = { horizontal: 'right', vertical: 'middle' };
      if (Number(linea.importe) < 0) {
        importeCell.font = { ...row.font, color: { argb: 'FFCC0000' } };
      }

      // Columnas OC (19) y Posición (20): resaltar en amarillo si tienen valor
      if (!isDebit && linea.orden_compra) {
        row.getCell(19).fill = yellowFill;
        row.getCell(20).fill = yellowFill;
      }
    }
  }

  // --- Anchos de columna ---
  const widths = [
    14, 10, 7, 7, 12, 12, 10, 8,
    28, 10, 12, 12, 8, 14, 8, 14,
    8, 14, 14, 8,
    10, 10, 10, 8,
    12, 12, 22, 24, 10, 10,
    14, 14,
    8, 10, 8, 10, 16, 12,
  ];
  ws.columns.forEach((col, i) => {
    if (widths[i]) col.width = widths[i];
  });

  return wb;
}

module.exports = { generateSapExcel, HEADERS };
