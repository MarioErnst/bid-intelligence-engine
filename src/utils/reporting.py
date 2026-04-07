import os
import logging
from datetime import datetime
from typing import List, Dict
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.enums import TA_CENTER

from src.core.models import ResumenAuditoria, LicitacionPerdida

logger = logging.getLogger(__name__)

class GeneradorInformePDF:
    """Genera el informe PDF profesional con todas las secciones"""
    
    def __init__(self, output_path: str = "Informe_Auditoria_Licitaciones.pdf"):
        self.output_path = output_path
        self.doc = SimpleDocTemplate(output_path, pagesize=A4)
        self.styles = getSampleStyleSheet()
        self.story = []
        
        # Estilos personalizados
        self._crear_estilos()
    
    def _crear_estilos(self):
        """Define estilos personalizados para el PDF"""
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#2E86AB'),
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#A23B72'),
            spaceAfter=12,
            spaceBefore=12,
            fontName='Helvetica-Bold'
        ))
        
        self.styles.add(ParagraphStyle(
            name='HighlightBox',
            parent=self.styles['Normal'],
            fontSize=14,
            textColor=colors.white,
            backColor=colors.HexColor('#2E86AB'),
            borderPadding=10,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
    
    def agregar_portada(self, resumen: ResumenAuditoria):
        """Genera la portada del informe"""
        # Título principal
        titulo = Paragraph(
            "AUDITORÍA DE LICITACIONES PERDIDAS",
            self.styles['CustomTitle']
        )
        self.story.append(titulo)
        self.story.append(Spacer(1, 0.3*inch))
        
        # Subtítulo
        subtitulo = Paragraph(
            "Contract Defense System - Diagnóstico de Dolor B2B",
            self.styles['Heading3']
        )
        self.story.append(subtitulo)
        self.story.append(Spacer(1, 0.5*inch))
        
        # El "Número de Impacto"
        impacto_data = [
            ['MÉTRICA CLAVE', 'VALOR'],
            ['💰 Dinero Perdido por Precio', f"${resumen.total_dinero_perdido_precio:,.0f} CLP"],
            ['📊 Total Licitaciones Analizadas', f"{resumen.total_licitaciones_perdidas}"],
            ['🔴 Pérdidas por Precio', f"{resumen.perdidas_por_precio}"],
            ['🔧 Pérdidas Técnicas', f"{resumen.perdidas_tecnicas}"],
            ['⚠️ Licitaciones Desiertas', f"{resumen.licitaciones_desiertas}"],
        ]
        
        tabla_impacto = Table(impacto_data, colWidths=[3.5*inch, 2*inch])
        tabla_impacto.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
        ]))
        
        self.story.append(tabla_impacto)
        self.story.append(Spacer(1, 0.3*inch))
        
        # Fecha del informe
        fecha = Paragraph(
            f"<i>Generado el: {datetime.now().strftime('%d/%m/%Y %H:%M')}</i>",
            self.styles['Normal']
        )
        self.story.append(fecha)
        self.story.append(PageBreak())
    
    def agregar_graficos(self, rutas_graficos: Dict[str, str]):
        """Agrega los gráficos al informe"""
        self.story.append(Paragraph("ANÁLISIS VISUAL", self.styles['SectionHeader']))
        self.story.append(Spacer(1, 0.2*inch))
        
        for nombre, ruta in rutas_graficos.items():
            if ruta and os.path.exists(ruta):
                try:
                    img = Image(ruta, width=6*inch, height=4*inch)
                    self.story.append(img)
                    self.story.append(Spacer(1, 0.3*inch))
                except Exception as e:
                    logger.error(f"   ❌ Error agregando gráfico {nombre}: {e}")
    
    def agregar_oportunidades(self, oportunidades: List[Dict]):
        """Agrega la tabla de oportunidades inmediatas"""
        self.story.append(PageBreak())
        self.story.append(Paragraph("🎯 OPORTUNIDADES INMEDIATAS", self.styles['SectionHeader']))
        self.story.append(Spacer(1, 0.2*inch))
        
        descripcion = Paragraph(
            "Estas licitaciones las perdiste por una diferencia de precio menor al 5%. "
            "¡Estuviste muy cerca de ganar!",
            self.styles['Normal']
        )
        self.story.append(descripcion)
        self.story.append(Spacer(1, 0.2*inch))
        
        if not oportunidades:
            self.story.append(Paragraph(
                "✅ No hay oportunidades en este rango. Tus pérdidas fueron más amplias.",
                self.styles['Normal']
            ))
            return
        
        # Crear tabla
        data = [['Licitación', 'Producto', 'Gap $', 'Gap %', 'Insight']]
        
        for op in oportunidades[:10]:  # Top 10
            data.append([
                op['id_licitacion'][:15] + '...',
                op['producto'][:20] + '...',
                f"${op['gap_monetario']:,.0f}",
                f"{op['gap_porcentual']:.1f}%",
                op['insight'][:40] + '...'
            ])
        
        tabla = Table(data, colWidths=[1.2*inch, 1.5*inch, 1*inch, 0.8*inch, 2*inch])
        tabla.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F18F01')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey]),
        ]))
        
        self.story.append(tabla)
    
    def agregar_analisis_detallado(self, licitaciones: List[LicitacionPerdida]):
        """Agrega el análisis detallado de IA para cada licitación"""
        self.story.append(PageBreak())
        self.story.append(Paragraph("🧠 ANÁLISIS DETALLADO (GENERADO POR GEMINI)", self.styles['SectionHeader']))
        self.story.append(Spacer(1, 0.2*inch))
        
        count = 0
        for licit in licitaciones:
            if licit.analisis_ai:
                count += 1
                # Título de la licitación
                self.story.append(Paragraph(f"#{count} - {licit.id_licitacion}: {licit.producto_cliente}", self.styles['Heading3']))
                
                # Tabla resumen
                data = [
                    ['Mi Oferta', 'Ganador', 'Diferencia'],
                    [
                        f"${licit.precio_oferta_cliente:,.0f}\n({licit.rut_cliente})", 
                        f"${licit.precio_ganador:,.0f}\n({licit.proveedor_ganador})",
                        f"${licit.gap_monetario:,.0f}\n({licit.gap_porcentual:.1f}%)"
                    ]
                ]
                t = Table(data, colWidths=[2*inch, 2*inch, 2*inch])
                t.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
                    ('GRID', (0,0), (-1,-1), 1, colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ]))
                self.story.append(t)
                self.story.append(Spacer(1, 0.1*inch))
                
                # Análisis de IA (formateado)
                # Reemplazar saltos de línea con <br/> para HTML en Paragraph
                analisis_fmt = licit.analisis_ai.replace('\n', '<br/>')
                self.story.append(Paragraph(analisis_fmt, self.styles['Normal']))
                self.story.append(Spacer(1, 0.3*inch))
                self.story.append(Paragraph("_"*50, self.styles['Normal']))
                self.story.append(Spacer(1, 0.2*inch))
                
                # Salto de página cada 2 análisis para no saturar
                if count % 2 == 0:
                    self.story.append(PageBreak())

    def generar(self):
        """Construye y genera el PDF final"""
        self.doc.build(self.story)
        logger.info(f"✅ Informe PDF generado: {self.output_path}")
