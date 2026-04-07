import json
import logging
import os
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("MOD5_Reporter")

def run_module_5():
    """
    Módulo 5:
    1. Lee el output final del Módulo 4.
    2. Genera estadísticas agregadas (% pérdida por precio vs otros).
    3. Crea un reporte consolidado en Excel y Markdown.
    """
    input_file = "data/step4_ai_analysis.json"
    report_md = "Reporte_Final_Auditoria.md"
    report_xlsx = "Reporte_Final_Auditoria.xlsx"
    
    if not os.path.exists(input_file):
        logger.error(f"Archivo de entrada no encontrado: {input_file}. Ejecute Módulo 4 primero.")
        return

    logger.info("--- 📊 INICIANDO MÓDULO 5: GENERACIÓN DE REPORTES ---")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    df = pd.DataFrame(data)
    
    # --- 1. Estadísticas Generales y Financieras ---
    # Calcular monto total de la oferta perdida (Precio Unitario * Cantidad)
    df['monto_total_oferta'] = df['precio_oferta_cliente'] * df['cantidad']
    
    total_licitaciones = len(df)
    total_dinero_perdido = df['monto_total_oferta'].sum()
    
    # Pérdidas por PRECIO
    df_price = df[df['motivo_perdida_preliminar'] == 'PRECIO']
    count_price = df_price.shape[0]
    money_price = df_price['monto_total_oferta'].sum()
    
    # Pérdidas por OTROS (Técnico/Admin)
    df_other = df[df['motivo_perdida_preliminar'] == 'OTRO']
    count_other = df_other.shape[0]
    money_other = df_other['monto_total_oferta'].sum()
    
    # Desconocido
    df_unknown = df[df['motivo_perdida_preliminar'] == 'DESCONOCIDO']
    count_unknown = df_unknown.shape[0]
    money_unknown = df_unknown['monto_total_oferta'].sum()
    
    # Porcentajes (Cantidad)
    pct_count_price = (count_price / total_licitaciones * 100) if total_licitaciones > 0 else 0
    pct_count_other = (count_other / total_licitaciones * 100) if total_licitaciones > 0 else 0
    
    # Porcentajes (Dinero)
    pct_money_price = (money_price / total_dinero_perdido * 100) if total_dinero_perdido > 0 else 0
    pct_money_other = (money_other / total_dinero_perdido * 100) if total_dinero_perdido > 0 else 0
    
    # Promedios
    avg_price = (money_price / count_price) if count_price > 0 else 0
    avg_other = (money_other / count_other) if count_other > 0 else 0

    # --- 2. Generar Markdown ---
    md_content = f"""# 📊 Reporte de Auditoría de Licitaciones Perdidas
**Fecha:** {datetime.now().strftime('%Y-%m-%d %H:%M')}
**Total Licitaciones Analizadas:** {total_licitaciones}
**💰 Monto Total de Ventas Perdidas:** ${total_dinero_perdido:,.0f}

## 1. Resumen Ejecutivo (Impacto Financiero)

| Motivo de Pérdida | Cantidad | % Cantidad | Dinero Perdido (Venta Potencial) | % Dinero | Promedio por Licitación |
|-------------------|----------|------------|----------------------------------|----------|-------------------------|
| 📉 Precio (Éramos más caros) | {count_price} | {pct_count_price:.1f}% | **${money_price:,.0f}** | **{pct_money_price:.1f}%** | ${avg_price:,.0f} |
| 🔍 Otros (Técnico/Admin) | {count_other} | {pct_count_other:.1f}% | **${money_other:,.0f}** | **{pct_money_other:.1f}%** | ${avg_other:,.0f} |
| ❓ Desconocido | {count_unknown} | - | ${money_unknown:,.0f} | - | - |

> 💡 **Insight:** Aunque perdemos más veces por **Precio** ({pct_count_price:.1f}% de los casos), las pérdidas por **Razones Técnicas/Otras** representan el mayor impacto económico ({pct_money_other:.1f}% del dinero).
> Esto indica que **perdemos licitaciones de mayor valor por errores técnicos**, mientras que por precio perdemos licitaciones más pequeñas (Promedio: ${avg_price:,.0f} vs ${avg_other:,.0f}).

---

## 2. Detalle: Pérdidas por Precio (Winner < Nosotros)
Estas son licitaciones donde nuestra oferta económica fue superior a la ganadora.

| ID Licitación | Producto | Cant. | Nuestro Precio | Precio Ganador | GAP ($) | Venta Perdida Total |
|---------------|----------|-------|----------------|----------------|---------|---------------------|
"""
    
    # Tabla Precio
    for _, row in df_price.iterrows():
        p_cli = row.get('precio_oferta_cliente', 0)
        p_gan = row.get('precio_ganador', 0)
        gap = row.get('gap_monetario', 0)
        cant = row.get('cantidad', 1)
        total_row = row.get('monto_total_oferta', 0)
        
        md_content += f"| {row['id_licitacion']} | {str(row['producto_cliente'])[:25]} | {cant:,.0f} | ${p_cli:,.0f} | ${p_gan:,.0f} | ${gap:,.0f} | **${total_row:,.0f}** |\n"

    md_content += """
---

## 3. Detalle: Pérdidas No Económicas (Análisis IA)
En estos casos, nuestro precio era competitivo (menor o igual al ganador), pero perdimos por otras razones.

"""

    # Sección IA
    for _, row in df_other.iterrows():
        analisis = row.get('analisis_ai')
        if pd.isna(analisis):
            analisis = 'Pendiente de análisis.'
        else:
            analisis = str(analisis)
            
        # Limpiar un poco el texto del análisis para que no rompa el markdown
        analisis_excerpt = analisis.replace('\n', '\n> ')
        
        total_row = row.get('monto_total_oferta', 0)
        cant = row.get('cantidad', 1)
        
        md_content += f"### 📄 Licitación: {row['id_licitacion']}\n"
        md_content += f"- **Producto:** {row['producto_cliente']} (Cant: {cant:,.0f})\n"
        md_content += f"- **Venta Total Perdida:** ${total_row:,.0f}\n"
        md_content += f"- **Ganador:** {row['proveedor_ganador']} (${row.get('precio_ganador', 0):,.0f})\n"
        md_content += f"- **Nuestro Precio:** ${row.get('precio_oferta_cliente', 0):,.0f}\n"
        md_content += f"- **Link Acta:** [Ver Acta]({row['url_acta']})\n\n"
        md_content += f"**🧠 Análisis de Causa Raíz:**\n> {analisis_excerpt}\n\n"
        md_content += "---\n"

    with open(report_md, 'w', encoding='utf-8') as f:
        f.write(md_content)

    # --- 3. Generar Excel ---
    # Seleccionar columnas relevantes para el reporte final
    cols = [
        'id_licitacion', 'producto_cliente', 'fecha_licitacion', 'cantidad',
        'monto_total_oferta',  # Nueva columna
        'rut_cliente', 'precio_oferta_cliente',
        'rut_ganador', 'proveedor_ganador', 'precio_ganador',
        'motivo_perdida_preliminar', 'gap_monetario', 'gap_porcentual',
        'analisis_ai', 'url_acta'
    ]
    # Filtrar solo columnas que existen
    cols = [c for c in cols if c in df.columns]
    
    df[cols].to_excel(report_xlsx, index=False)
    
    logger.info(f"📝 Reporte Markdown generado: {report_md}")
    logger.info(f"📊 Reporte Excel generado: {report_xlsx}")
    logger.info("--- ✅ MÓDULO 5 FINALIZADO ---")

if __name__ == "__main__":
    run_module_5()
