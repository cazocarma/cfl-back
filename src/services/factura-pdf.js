'use strict';

/**
 * Generador de PDF para pre facturas.
 * Responsabilidad única: renderizar un PDFDocument a partir de los datos de una factura.
 *
 * Uso: const stream = generatePreFacturaPdf(factura);
 *      stream.pipe(res);
 */

const GREENVIC_RUT = '78.335.990-1';
const BRAND_COLOR = '#1e4424';

// ── Formatters ──────────────────────────────────────────────────────────────

function formatCLP(n) {
  return `$${Number(n || 0).toLocaleString('es-CL', { maximumFractionDigits: 0 })}`;
}

function formatDate(d) {
  return d ? new Date(d).toLocaleDateString('es-CL') : '-';
}

function tipoMovLabel(tm) {
  const v = String(tm || '').trim().toLowerCase();
  if (v === 'push') return 'Despacho';
  if (v === 'pull') return 'Retorno';
  return tm || '-';
}

// ── Layout constants ────────────────────────────────────────────────────────

const PAGE_W = 792;   // Letter landscape
const PAGE_H = 612;
const MARGIN_L = 36;
const MARGIN_R = 36;
const CONTENT_W = PAGE_W - MARGIN_L - MARGIN_R;
const FOOTER_Y = PAGE_H - 30;
const TABLE_BOTTOM = FOOTER_Y - 10;
const HEADER_BAR_H = 50;
const TABLE_HEADER_H = 15;
const FONT_SIZE = 6.2;
const DETAIL_FONT_SIZE = 5.5;
const DETAIL_ROW_H = 7;
const CELL_PAD = 2;

// ── Column definitions ──────────────────────────────────────────────────────

function buildColumns() {
  const defs = [
    { label: 'Fecha',       w: 46, align: 'left'  },
    { label: 'Tipo Mov.',   w: 46, align: 'left'  },
    { label: 'N° Guía',     w: 52, align: 'left'  },
    { label: 'Tipo Cam.',   w: 50, align: 'left'  },
    { label: 'Patente',     w: 46, align: 'left'  },
    { label: 'Chofer',      w: 72, align: 'left'  },
    { label: 'Ruta',        w: 100, align: 'left' },
    { label: 'Det. Viaje',  w: 62, align: 'left'  },
    { label: 'Productor',   w: 72, align: 'left'  },
    { label: 'C. Costo',    w: 46, align: 'left'  },
    { label: 'Tipo Flete',  w: 58, align: 'left'  },
    { label: 'Valor',       w: 58, align: 'right' },
  ];

  // Scale proportionally to fit CONTENT_W
  const totalW = defs.reduce((s, c) => s + c.w, 0);
  const factor = CONTENT_W / totalW;
  for (const c of defs) c.w = Math.round(c.w * factor);

  // Absorb rounding error in Ruta column (index 6)
  const diff = CONTENT_W - defs.reduce((s, c) => s + c.w, 0);
  if (diff !== 0) defs[6].w += diff;

  // Compute x positions
  let x = MARGIN_L;
  for (const c of defs) { c.x = x; x += c.w; }

  return defs;
}

// ── PDF rendering ───────────────────────────────────────────────────────────

/**
 * Genera un PDFDocument con la pre factura y lo pipea al stream de salida.
 * @param {object} factura - Datos completos de la factura (de fetchFactura).
 * @param {import('stream').Writable} outputStream - Stream de salida (e.g., res de Express).
 */
function generatePreFacturaPdf(factura, outputStream) {
  const PDFDocument = require('pdfkit');
  const doc = new PDFDocument({ size: 'LETTER', layout: 'landscape', margin: MARGIN_L });
  doc.pipe(outputStream);
  const cols = buildColumns();

  // ── Helpers ligados al doc ────────────────────────────────────────────────

  function drawTableHeader(y) {
    doc.rect(MARGIN_L, y, CONTENT_W, TABLE_HEADER_H).fill('#f0fdf4');
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor(BRAND_COLOR);
    for (const c of cols) {
      doc.text(c.label, c.x + CELL_PAD, y + 4, {
        width: c.w - CELL_PAD * 2, lineBreak: false, align: c.align,
      });
    }
    doc.fillColor('#000000');
    return y + TABLE_HEADER_H;
  }

  function drawPageFooter() {
    doc.fontSize(6).font('Helvetica').fillColor('#999999').text(
      `Pre Factura ${factura.numero_factura} — pág. ${doc.bufferedPageRange().count}`,
      MARGIN_L, FOOTER_Y, { width: CONTENT_W, align: 'center' },
    );
  }

  function newPage() {
    drawPageFooter();
    doc.addPage();
    return drawTableHeader(30);
  }

  function measureCellH(text, colW) {
    return doc.font('Helvetica').fontSize(FONT_SIZE)
      .heightOfString(String(text || ''), { width: colW - CELL_PAD * 2 });
  }

  // ── Encabezado (barra verde) ──────────────────────────────────────────────

  doc.rect(0, 0, PAGE_W, HEADER_BAR_H).fill(BRAND_COLOR);

  // Izquierda: PRE FACTURA (grande) + número
  doc.fontSize(20).font('Helvetica-Bold').fillColor('#ffffff')
    .text('PRE FACTURA', MARGIN_L, 6, { width: 300 });
  doc.fontSize(10).font('Helvetica').fillColor('#a7f3d0')
    .text(factura.numero_factura, MARGIN_L, 30, { width: 300 });

  // Derecha: Greenvic SPA + dirección + RUT
  doc.fontSize(10).font('Helvetica-Bold').fillColor('#ffffff')
    .text('Greenvic SPA', PAGE_W - MARGIN_R - 240, 6, { width: 232, align: 'right' });
  doc.fontSize(6.5).font('Helvetica').fillColor('#d1fae5')
    .text('Apoquindo 4700 Of. 901, Las Condes, Santiago', PAGE_W - MARGIN_R - 240, 19, { width: 232, align: 'right' });
  doc.fontSize(6.5).fillColor('#a7f3d0')
    .text(`RUT: ${GREENVIC_RUT}`, PAGE_W - MARGIN_R - 240, 30, { width: 232, align: 'right' });

  // ── Metadata (una sola columna) ─────────────────────────────────────────

  doc.fillColor('#000000');
  let infoY = 58;
  doc.font('Helvetica-Bold').fontSize(8)
    .text(factura.empresa_nombre, MARGIN_L, infoY);
  doc.font('Helvetica').fontSize(7);
  doc.text(`RUT: ${factura.empresa_rut || '-'}`, MARGIN_L, infoY + 11);
  doc.text(`Fecha emisión: ${formatDate(factura.fecha_emision)}`, MARGIN_L, infoY + 22);
  doc.text(`Moneda: ${factura.moneda}`, MARGIN_L, infoY + 33);

  if (factura.observaciones) {
    doc.text(`Obs: ${factura.observaciones}`, MARGIN_L, infoY + 44, { width: CONTENT_W });
    infoY += 12;
  }

  infoY += 40;
  doc.moveTo(MARGIN_L, infoY).lineTo(PAGE_W - MARGIN_R, infoY)
    .lineWidth(0.5).strokeColor('#c4c4c4').stroke();
  infoY += 5;

  // ── Tabla de movimientos ──────────────────────────────────────────────────

  let tableY = drawTableHeader(infoY);
  let rowIdx = 0;
  const detailIndent = Math.round(CONTENT_W * 0.25);

  for (const m of factura.movimientos) {
    const detalles = m.detalles || [];

    // Datos formateados
    const choferRut = m.chofer_rut || '';
    const choferNombre = m.chofer_nombre || '-';
    const prodCodigo = m.productor_codigo || '';
    const prodNombre = m.productor_nombre || '-';
    const rutaText = m.ruta || '-';
    const ccText = `${m.centro_costo_codigo || '-'}\n${m.centro_costo || ''}`.trim();

    // Altura dinámica basada en celdas con wrap
    const wrapCells = [
      { text: rutaText, w: cols[6].w },
      { text: `${choferRut}\n${choferNombre}`, w: cols[5].w },
      { text: `${prodCodigo}\n${prodNombre}`, w: cols[8].w },
      { text: m.detalle_viaje || '-', w: cols[7].w },
      { text: ccText, w: cols[9].w },
    ];
    let maxCellH = 12;
    for (const ct of wrapCells) {
      const h = measureCellH(ct.text, ct.w);
      if (h > maxCellH) maxCellH = h;
    }
    const mainRowH = maxCellH + 6;
    const detailBlockH = detalles.length > 0 ? (detalles.length * DETAIL_ROW_H + 14) : 0;
    const rowH = mainRowH + detailBlockH;

    // Salto de página
    if (tableY + rowH > TABLE_BOTTOM) {
      tableY = newPage();
      rowIdx = 0;
    }

    // Fondo alterno + separador
    if (rowIdx % 2 === 0) {
      doc.rect(MARGIN_L, tableY, CONTENT_W, rowH).fill('#fafafa');
    }
    doc.moveTo(MARGIN_L, tableY).lineTo(PAGE_W - MARGIN_R, tableY)
      .lineWidth(0.3).strokeColor('#e0e0e0').stroke();

    const guia = m.GuiaRemision || m.NumeroEntrega || m.SapNumeroEntrega || '-';
    const ly = tableY + 3;

    // Celdas principales
    doc.font('Helvetica').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(formatDate(m.FechaSalida), cols[0].x + CELL_PAD, ly, { width: cols[0].w - CELL_PAD * 2 });
    doc.text(tipoMovLabel(m.TipoMovimiento), cols[1].x + CELL_PAD, ly, { width: cols[1].w - CELL_PAD * 2 });
    doc.text(String(guia), cols[2].x + CELL_PAD, ly, { width: cols[2].w - CELL_PAD * 2 });
    doc.text(String(m.tipo_camion || '-'), cols[3].x + CELL_PAD, ly, { width: cols[3].w - CELL_PAD * 2 });
    doc.text(String(m.camion_patente || '-'), cols[4].x + CELL_PAD, ly, { width: cols[4].w - CELL_PAD * 2 });

    // Chofer: RUT + nombre
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(choferRut || '-', cols[5].x + CELL_PAD, ly, { width: cols[5].w - CELL_PAD * 2, lineBreak: false });
    doc.font('Helvetica').fontSize(FONT_SIZE);
    doc.text(choferNombre, cols[5].x + CELL_PAD, ly + 8, { width: cols[5].w - CELL_PAD * 2 });

    // Ruta (con wrap)
    doc.text(rutaText, cols[6].x + CELL_PAD, ly, { width: cols[6].w - CELL_PAD * 2 });

    // Detalle viaje
    doc.text(String(m.detalle_viaje || '-'), cols[7].x + CELL_PAD, ly, { width: cols[7].w - CELL_PAD * 2 });

    // Productor: código + nombre
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(prodCodigo || '-', cols[8].x + CELL_PAD, ly, { width: cols[8].w - CELL_PAD * 2, lineBreak: false });
    doc.font('Helvetica').fontSize(FONT_SIZE);
    doc.text(prodNombre, cols[8].x + CELL_PAD, ly + 8, { width: cols[8].w - CELL_PAD * 2 });

    // Centro costo (código + nombre)
    doc.text(ccText, cols[9].x + CELL_PAD, ly, { width: cols[9].w - CELL_PAD * 2 });

    // Tipo flete
    doc.text(String(m.tipo_flete_nombre || '-'), cols[10].x + CELL_PAD, ly, { width: cols[10].w - CELL_PAD * 2 });

    // Valor
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(formatCLP(m.MontoAplicado), cols[11].x + CELL_PAD, ly, { width: cols[11].w - CELL_PAD * 2, align: 'right' });

    // Sub-filas de detalle (material / especie / cantidad)
    if (detalles.length > 0) {
      const detStartY = tableY + mainRowH;
      const detailX = MARGIN_L + detailIndent;
      const detailW = CONTENT_W - detailIndent - 10;

      // Encabezado de detalle
      doc.font('Helvetica-Bold').fontSize(DETAIL_FONT_SIZE).fillColor(BRAND_COLOR);
      doc.text('Material', detailX, detStartY, { width: detailW * 0.45, lineBreak: false });
      doc.text('Especie', detailX + detailW * 0.45, detStartY, { width: detailW * 0.35, lineBreak: false });
      doc.text('Cantidad', detailX + detailW * 0.80, detStartY, { width: detailW * 0.20, align: 'right', lineBreak: false });

      doc.font('Helvetica').fontSize(DETAIL_FONT_SIZE).fillColor('#444444');
      let subY = detStartY + DETAIL_ROW_H;
      for (const det of detalles) {
        doc.text(det.Material || det.Descripcion || '-', detailX, subY, { width: detailW * 0.45, lineBreak: false });
        doc.text(det.especie_glosa || '-', detailX + detailW * 0.45, subY, { width: detailW * 0.35, lineBreak: false });
        doc.text(det.Cantidad != null ? String(det.Cantidad) : '-', detailX + detailW * 0.80, subY, { width: detailW * 0.20, align: 'right', lineBreak: false });
        subY += DETAIL_ROW_H;
      }
    }

    doc.fillColor('#000000');
    tableY += rowH;
    rowIdx++;
  }

  // ── Línea final + Totales ─────────────────────────────────────────────────

  doc.moveTo(MARGIN_L, tableY).lineTo(PAGE_W - MARGIN_R, tableY)
    .lineWidth(0.5).strokeColor('#c4c4c4').stroke();
  tableY += 14;

  const totalsBlockH = 70;
  if (tableY + totalsBlockH > TABLE_BOTTOM) {
    tableY = newPage();
  }

  const totalsX = PAGE_W - MARGIN_R - 220;
  const totalsW = 220;
  const lblW = 130;
  const valW = 80;

  doc.rect(totalsX - 6, tableY - 4, totalsW + 12, totalsBlockH).fill('#f9fafb');

  doc.font('Helvetica').fontSize(10).fillColor('#333333');
  doc.text('Neto:', totalsX, tableY, { width: lblW });
  doc.text(formatCLP(factura.monto_neto), totalsX + lblW, tableY, { width: valW, align: 'right' });
  tableY += 16;

  doc.text('IVA (19%):', totalsX, tableY, { width: lblW });
  doc.text(formatCLP(factura.monto_iva), totalsX + lblW, tableY, { width: valW, align: 'right' });
  tableY += 14;

  doc.moveTo(totalsX, tableY).lineTo(totalsX + totalsW, tableY)
    .lineWidth(0.5).strokeColor(BRAND_COLOR).stroke();
  tableY += 6;

  doc.font('Helvetica-Bold').fontSize(13).fillColor(BRAND_COLOR);
  doc.text('Total:', totalsX, tableY, { width: lblW });
  doc.text(formatCLP(factura.monto_total), totalsX + lblW, tableY, { width: valW, align: 'right' });

  // ── Pie de página ─────────────────────────────────────────────────────────
  drawPageFooter();

  doc.end();
  return doc;
}

module.exports = { generatePreFacturaPdf };
