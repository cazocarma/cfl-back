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
const CELL_PAD = 2;

// ── Column definitions ──────────────────────────────────────────────────────

function buildColumns() {
  const defs = [
    { label: 'Fecha',       w: 44, align: 'left'  },
    { label: 'Guía Ida',    w: 48, align: 'left'  },
    { label: 'Guía Ret.',   w: 48, align: 'left'  },
    { label: 'Tipo Cam.',   w: 46, align: 'left'  },
    { label: 'Patente',     w: 44, align: 'left'  },
    { label: 'Chofer',      w: 68, align: 'left'  },
    { label: 'Ruta',        w: 90, align: 'left'  },
    { label: 'Especie',     w: 50, align: 'left'  },
    { label: 'Det. Viaje',  w: 56, align: 'left'  },
    { label: 'Productor',   w: 66, align: 'left'  },
    { label: 'C. Costo',    w: 46, align: 'left'  },
    { label: 'Cta. Mayor',  w: 52, align: 'left'  },
    { label: 'Valor',       w: 54, align: 'right' },
  ];

  const totalW = defs.reduce((s, c) => s + c.w, 0);
  const factor = CONTENT_W / totalW;
  for (const c of defs) c.w = Math.round(c.w * factor);

  const diff = CONTENT_W - defs.reduce((s, c) => s + c.w, 0);
  if (diff !== 0) defs[6].w += diff;

  let x = MARGIN_L;
  for (const c of defs) { c.x = x; x += c.w; }

  return defs;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/** Construye texto de ruta, invirtiendo si sentido es VUELTA. */
function buildRutaText(m) {
  const sentido = (m.SentidoFlete || '').toUpperCase();
  const isVuelta = sentido === 'VUELTA';

  if (m.ruta_nombre) {
    // Named route — still invert the display if VUELTA
    if (isVuelta && m.origen_nombre && m.destino_nombre) {
      return `${m.destino_nombre} -> ${m.origen_nombre}`;
    }
    return m.ruta_nombre;
  }
  if (m.origen_nombre || m.destino_nombre) {
    const orig = m.origen_nombre || 'Origen';
    const dest = m.destino_nombre || 'Destino';
    return isVuelta ? `${dest} -> ${orig}` : `${orig} -> ${dest}`;
  }
  return '-';
}

/** Extrae la especie principal (más frecuente) de los detalles de un movimiento. */
function especiePrincipal(detalles) {
  if (!detalles || detalles.length === 0) return '-';
  const counts = {};
  for (const d of detalles) {
    const esp = (d.especie_glosa || '').trim();
    if (esp) counts[esp] = (counts[esp] || 0) + 1;
  }
  const entries = Object.entries(counts);
  if (entries.length === 0) return '-';
  entries.sort((a, b) => b[1] - a[1]);
  return entries[0][0];
}

// ── PDF rendering ───────────────────────────────────────────────────────────

function generatePreFacturaPdf(factura, outputStream) {
  const PDFDocument = require('pdfkit');
  const doc = new PDFDocument({ size: 'LETTER', layout: 'landscape', margin: MARGIN_L });
  doc.pipe(outputStream);
  const cols = buildColumns();

  // ── Helpers ──────────────────────────────────────────────────────────────

  function drawDocHeader() {
    doc.rect(0, 0, PAGE_W, HEADER_BAR_H).fill(BRAND_COLOR);
    doc.fontSize(20).font('Helvetica-Bold').fillColor('#ffffff')
      .text('PRE FACTURA', MARGIN_L, 10, { width: 300 });
    doc.fontSize(10).font('Helvetica').fillColor('#a7f3d0')
      .text(factura.numero_factura, MARGIN_L, 32, { width: 300 });
    doc.fontSize(10).font('Helvetica-Bold').fillColor('#ffffff')
      .text('Greenvic SPA', PAGE_W - MARGIN_R - 240, 6, { width: 232, align: 'right' });
    doc.fontSize(6.5).font('Helvetica').fillColor('#d1fae5')
      .text('Apoquindo 4700 Of. 901, Las Condes, Santiago', PAGE_W - MARGIN_R - 240, 19, { width: 232, align: 'right' });
    doc.fontSize(6.5).fillColor('#a7f3d0')
      .text(`RUT: ${GREENVIC_RUT}`, PAGE_W - MARGIN_R - 240, 30, { width: 232, align: 'right' });
    doc.fillColor('#000000');
  }

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

  function newTablePage() {
    doc.addPage();
    drawDocHeader();
    return drawTableHeader(HEADER_BAR_H + 8);
  }

  function ensureSpace(tableY, neededH) {
    if (tableY + neededH > TABLE_BOTTOM) {
      return newTablePage();
    }
    return tableY;
  }

  function measureCellH(text, colW) {
    return doc.font('Helvetica').fontSize(FONT_SIZE)
      .heightOfString(String(text || ''), { width: colW - CELL_PAD * 2 });
  }

  // ── Page 1: Header bar ────────────────────────────────────────────────────
  drawDocHeader();

  // ── Metadata ──────────────────────────────────────────────────────────────

  doc.fillColor('#000000');
  let infoY = 58;
  doc.font('Helvetica-Bold').fontSize(8)
    .text(factura.empresa_nombre, MARGIN_L, infoY);
  doc.font('Helvetica').fontSize(7);
  doc.text(`RUT: ${factura.empresa_rut || '-'}`, MARGIN_L, infoY + 11);
  doc.text(`Fecha emision: ${formatDate(factura.fecha_emision)}`, MARGIN_L, infoY + 22);
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

  for (const m of factura.movimientos) {
    const choferRut = m.chofer_rut || '';
    const choferNombre = m.chofer_nombre || '-';
    const prodNombre = m.productor_nombre || '-';
    const rutaText = buildRutaText(m);
    const ccText = `${m.centro_costo_codigo || '-'}\n${m.centro_costo || ''}`.trim();
    const cmText = `${m.cuenta_mayor_codigo || '-'}\n${m.cuenta_mayor_nombre || ''}`.trim();
    const especieText = especiePrincipal(m.detalles);

    // Guía Ida / Retorno based on TipoMovimiento
    const guia = m.GuiaRemision || m.NumeroEntrega || m.SapNumeroEntrega || '-';
    const tipoMov = String(m.TipoMovimiento || '').trim().toLowerCase();
    const guiaIda = tipoMov === 'push' ? String(guia) : '';
    const guiaRetorno = tipoMov === 'pull' ? String(guia) : '';

    // Dynamic row height
    const wrapCells = [
      { text: rutaText, w: cols[6].w },
      { text: `${choferRut}\n${choferNombre}`, w: cols[5].w },
      { text: prodNombre, w: cols[9].w },
      { text: m.detalle_viaje || '-', w: cols[8].w },
      { text: ccText, w: cols[10].w },
      { text: cmText, w: cols[11].w },
    ];
    let maxCellH = 12;
    for (const ct of wrapCells) {
      const h = measureCellH(ct.text, ct.w);
      if (h > maxCellH) maxCellH = h;
    }
    const rowH = maxCellH + 6;

    // Page break if needed
    tableY = ensureSpace(tableY, rowH);

    // Alternating row background + separator
    if (rowIdx % 2 === 0) {
      doc.rect(MARGIN_L, tableY, CONTENT_W, rowH).fill('#fafafa');
    }
    doc.moveTo(MARGIN_L, tableY).lineTo(PAGE_W - MARGIN_R, tableY)
      .lineWidth(0.3).strokeColor('#e0e0e0').stroke();

    const ly = tableY + 3;

    // Main cells
    doc.font('Helvetica').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(formatDate(m.FechaSalida), cols[0].x + CELL_PAD, ly, { width: cols[0].w - CELL_PAD * 2 });
    doc.text(guiaIda, cols[1].x + CELL_PAD, ly, { width: cols[1].w - CELL_PAD * 2 });
    doc.text(guiaRetorno, cols[2].x + CELL_PAD, ly, { width: cols[2].w - CELL_PAD * 2 });
    doc.text(String(m.tipo_camion || '-'), cols[3].x + CELL_PAD, ly, { width: cols[3].w - CELL_PAD * 2 });
    doc.text(String(m.camion_patente || '-'), cols[4].x + CELL_PAD, ly, { width: cols[4].w - CELL_PAD * 2 });

    // Chofer
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(choferRut || '-', cols[5].x + CELL_PAD, ly, { width: cols[5].w - CELL_PAD * 2, lineBreak: false });
    doc.font('Helvetica').fontSize(FONT_SIZE);
    doc.text(choferNombre, cols[5].x + CELL_PAD, ly + 8, { width: cols[5].w - CELL_PAD * 2 });

    // Ruta
    doc.text(rutaText, cols[6].x + CELL_PAD, ly, { width: cols[6].w - CELL_PAD * 2 });

    // Especie
    doc.text(especieText, cols[7].x + CELL_PAD, ly, { width: cols[7].w - CELL_PAD * 2 });

    // Detalle viaje
    doc.text(String(m.detalle_viaje || '-'), cols[8].x + CELL_PAD, ly, { width: cols[8].w - CELL_PAD * 2 });

    // Productor
    doc.font('Helvetica').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(prodNombre, cols[9].x + CELL_PAD, ly, { width: cols[9].w - CELL_PAD * 2 });

    // Centro costo
    doc.text(ccText, cols[10].x + CELL_PAD, ly, { width: cols[10].w - CELL_PAD * 2 });

    // Cuenta mayor
    doc.text(cmText, cols[11].x + CELL_PAD, ly, { width: cols[11].w - CELL_PAD * 2 });

    // Valor
    doc.font('Helvetica-Bold').fontSize(FONT_SIZE).fillColor('#1a1a1a');
    doc.text(formatCLP(m.MontoAplicado), cols[12].x + CELL_PAD, ly, { width: cols[12].w - CELL_PAD * 2, align: 'right' });

    doc.fillColor('#000000');
    tableY += rowH;
    rowIdx++;
  }

  // ── Totals ────────────────────────────────────────────────────────────────

  doc.moveTo(MARGIN_L, tableY).lineTo(PAGE_W - MARGIN_R, tableY)
    .lineWidth(0.5).strokeColor('#c4c4c4').stroke();
  tableY += 14;

  const totalsBlockH = 70;
  if (tableY + totalsBlockH > TABLE_BOTTOM) {
    doc.addPage();
    drawDocHeader();
    tableY = HEADER_BAR_H + 20;
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

  doc.end();
  return doc;
}

module.exports = { generatePreFacturaPdf };
