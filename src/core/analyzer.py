from typing import Dict, Optional
from src.core.models import LicitacionPerdida

class AnalizadorDerrotas:
    """Analiza las causas de cada derrota según la lógica de negocio"""
    
    @staticmethod
    def analizar(licitacion: LicitacionPerdida, datos_ganador: Optional[Dict]) -> LicitacionPerdida:
        """
        Aplica la lógica de decisión para determinar causa de derrota
        
        ESCENARIO 1: Licitación Desierta
        ESCENARIO 2: Derrota por Precio
        ESCENARIO 3: Derrota Técnica/Administrativa
        
        Args:
            licitacion: Objeto LicitacionPerdida
            datos_ganador: Datos del ganador obtenidos de la API
            
        Returns:
            Objeto LicitacionPerdida actualizado con análisis
        """
        
        # ESCENARIO 1: ERROR API O SIN DATOS
        if datos_ganador is None:
            licitacion.causa_derrota = "ERROR CONSULTA API"
            licitacion.gap_monetario = 0
            licitacion.gap_porcentual = 0
            licitacion.insight = "⚠️ No se pudo obtener información oficial de la API (posible error de conexión o licitación no encontrada)."
            return licitacion

        estado = datos_ganador.get('estado_licitacion', 'Desconocido')
        
        # ESCENARIO 2: DESIERTA / REVOCADA / CERRADA SIN ADJUDICAR
        if estado in ['Desierta', 'Revocada', 'Suspendida', 'Caducada']:
            licitacion.causa_derrota = f"LICITACIÓN {estado.upper()}"
            licitacion.gap_monetario = 0
            licitacion.gap_porcentual = 0
            licitacion.insight = f"⚠️ La licitación fue declarada {estado}. Nadie ganó."
            return licitacion
            
        if estado in ['Publicada', 'Cerrada']:
             licitacion.causa_derrota = "EN PROCESO (NO ADJUDICADA)"
             licitacion.gap_monetario = 0
             licitacion.gap_porcentual = 0
             licitacion.insight = "⏳ La licitación aún está en proceso de evaluación o cerrada sin adjudicar todavía."
             return licitacion

        # ESCENARIO 3: ADJUDICADA PERO SIN PRECIO (GANADOR NO ENCONTRADO O PARCIAL)
        if datos_ganador.get('precio_ganador', 0) == 0:
            licitacion.causa_derrota = "ADJUDICADA (ITEM NO GANADO)"
            licitacion.gap_monetario = 0
            licitacion.gap_porcentual = 0
            licitacion.insight = "⚠️ La licitación fue adjudicada, pero no se encontró un ganador para tu ítem específico (posible adjudicación parcial o desierta para este ítem)."
            return licitacion
        
        # Extraer datos del ganador
        licitacion.precio_ganador = datos_ganador['precio_ganador']
        licitacion.proveedor_ganador = datos_ganador['proveedor_ganador']
        licitacion.rut_ganador = datos_ganador['rut_ganador']
        cantidad = datos_ganador.get('cantidad', licitacion.cantidad)
        
        # Calcular diferencias
        diferencia_unitaria = licitacion.precio_oferta_cliente - licitacion.precio_ganador
        diferencia_total = diferencia_unitaria * cantidad
        
        if licitacion.precio_ganador > 0:
            porcentaje = (diferencia_unitaria / licitacion.precio_ganador) * 100
        else:
            porcentaje = 0
        
        # ESCENARIO 2: DERROTA POR PRECIO
        if licitacion.precio_oferta_cliente > licitacion.precio_ganador:
            licitacion.causa_derrota = "PÉRDIDA POR PRECIO"
            licitacion.gap_monetario = abs(diferencia_total)
            licitacion.gap_porcentual = abs(porcentaje)
            
            licitacion.insight = (
                f"💰 Tu precio fue {licitacion.gap_porcentual:.1f}% más alto. "
                f"Perdiste ${licitacion.gap_monetario:,.0f} CLP por sobrepricing. "
                f"Ganador: {licitacion.proveedor_ganador}"
            )
            return licitacion
        
        # ESCENARIO 3: DERROTA TÉCNICA / ADMINISTRATIVA
        else:
            licitacion.causa_derrota = "PÉRDIDA TÉCNICA (NO PRECIO)"
            licitacion.gap_monetario = 0  # No es pérdida por precio
            licitacion.gap_porcentual = abs(porcentaje)
            
            licitacion.insight = (
                f"🔧 Ofertaste {licitacion.gap_porcentual:.1f}% MÁS BARATO pero perdiste igual. "
                f"Revisar: (1) Plazos de entrega, (2) Marca/Especificaciones técnicas, "
                f"(3) Boletas de garantía, (4) Documentación administrativa. "
                f"Ganador: {licitacion.proveedor_ganador}"
            )
            return licitacion
