import os
import logging
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
from typing import List
from collections import Counter
from src.core.models import LicitacionPerdida

matplotlib.use('Agg')  # Backend sin interfaz gráfica
logger = logging.getLogger(__name__)

class GeneradorGraficos:
    """Genera gráficos profesionales para el informe"""
    
    def __init__(self, output_dir: str = "graficos"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Estilo profesional
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colores = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']
    
    def pie_causas_derrota(self, licitaciones: List[LicitacionPerdida]) -> str:
        """
        Gráfico de torta: Causas de Derrota
        
        Returns:
            Ruta al archivo de imagen generado
        """
        causas = [l.causa_derrota for l in licitaciones]
        contador = Counter(causas)
        
        fig, ax = plt.subplots(figsize=(10, 7))
        
        labels = list(contador.keys())
        sizes = list(contador.values())
        colors = self.colores[:len(labels)]
        
        wedges, texts, autotexts = ax.pie(
            sizes, 
            labels=labels, 
            colors=colors,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 11, 'weight': 'bold'}
        )
        
        ax.set_title('Distribución de Causas de Derrota', fontsize=16, weight='bold', pad=20)
        
        # Mejorar legibilidad
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(12)
        
        plt.tight_layout()
        ruta = os.path.join(self.output_dir, 'pie_causas.png')
        plt.savefig(ruta, dpi=150, bbox_inches='tight')
        plt.close()
        
        logger.info(f"   📊 Gráfico generado: pie_causas.png")
        return ruta
    
    def bar_top_competidores(self, licitaciones: List[LicitacionPerdida], top_n: int = 10) -> str:
        """
        Gráfico de barras: Top Competidores que más nos ganan
        
        Returns:
            Ruta al archivo de imagen generado
        """
        competidores = [l.proveedor_ganador for l in licitaciones if l.proveedor_ganador]
        contador = Counter(competidores)
        top_competidores = contador.most_common(top_n)
        
        if not top_competidores:
            logger.warning("   ⚠️  No hay datos de competidores para graficar")
            return None
        
        nombres = [nombre[:30] + '...' if len(nombre) > 30 else nombre for nombre, _ in top_competidores]
        victorias = [count for _, count in top_competidores]
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        bars = ax.barh(nombres, victorias, color=self.colores[0])
        
        # Añadir valores en las barras
        for i, (bar, valor) in enumerate(zip(bars, victorias)):
            ax.text(valor + 0.1, i, str(valor), va='center', fontsize=10, weight='bold')
        
        ax.set_xlabel('Número de Victorias sobre Ti', fontsize=12, weight='bold')
        ax.set_title(f'Top {top_n} Competidores que Más Te Ganan', fontsize=16, weight='bold', pad=20)
        ax.invert_yaxis()
        
        plt.tight_layout()
        ruta = os.path.join(self.output_dir, 'bar_competidores.png')
        plt.savefig(ruta, dpi=150, bbox_inches='tight')
        plt.close()
        
        logger.info(f"   📊 Gráfico generado: bar_competidores.png")
        return ruta
    
    def scatter_dispersion_precios(self, licitaciones: List[LicitacionPerdida]) -> str:
        """
        Gráfico de dispersión: Evolución temporal de diferencias de precio
        
        Returns:
            Ruta al archivo de imagen generado
        """
        # Filtrar solo pérdidas por precio con fechas válidas
        datos = []
        for l in licitaciones:
            if l.causa_derrota == "PÉRDIDA POR PRECIO" and l.fecha_licitacion:
                try:
                    fecha = pd.to_datetime(l.fecha_licitacion)
                    datos.append({
                        'fecha': fecha,
                        'gap_porcentual': l.gap_porcentual
                    })
                except:
                    pass
        
        if not datos:
            logger.warning("   ⚠️  No hay datos temporales para graficar dispersión")
            return None
        
        df = pd.DataFrame(datos).sort_values('fecha')
        
        fig, ax = plt.subplots(figsize=(12, 7))
        
        scatter = ax.scatter(
            df['fecha'], 
            df['gap_porcentual'],
            c=df['gap_porcentual'],
            cmap='RdYlGn_r',
            s=100,
            alpha=0.6,
            edgecolors='black',
            linewidth=0.5
        )
        
        # Línea de tendencia
        if len(df) > 1:
            z = np.polyfit(range(len(df)), df['gap_porcentual'], 1)
            p = np.poly1d(z)
            ax.plot(df['fecha'], p(range(len(df))), "r--", alpha=0.8, linewidth=2, label='Tendencia')
            ax.legend()
        
        ax.set_xlabel('Fecha de Adjudicación', fontsize=12, weight='bold')
        ax.set_ylabel('Diferencia de Precio (%)', fontsize=12, weight='bold')
        ax.set_title('Dispersión Temporal: ¿Te Estás Volviendo Más Caro?', fontsize=16, weight='bold', pad=20)
        ax.grid(True, alpha=0.3)
        
        # Añadir colorbar
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('Gap %', rotation=270, labelpad=20)
        
        plt.tight_layout()
        ruta = os.path.join(self.output_dir, 'scatter_precios.png')
        plt.savefig(ruta, dpi=150, bbox_inches='tight')
        plt.close()
        
        logger.info(f"   📊 Gráfico generado: scatter_precios.png")
        return ruta
