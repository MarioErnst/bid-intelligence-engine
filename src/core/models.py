from dataclasses import dataclass
from typing import Dict, List

@dataclass
class LicitacionPerdida:
    """Modelo de datos para una licitación perdida"""
    id_licitacion: str
    producto_cliente: str
    precio_oferta_cliente: float
    rut_cliente: str
    fecha_licitacion: str = ""
    cantidad: float = 0
    
    # Datos del ganador (enriquecidos vía API)
    precio_ganador: float = 0
    proveedor_ganador: str = ""
    rut_ganador: str = ""
    
    # Análisis
    causa_derrota: str = ""  # DESIERTA / PRECIO / TÉCNICA
    gap_monetario: float = 0
    gap_porcentual: float = 0
    insight: str = ""
    analisis_ai: str = "" # Nuevo campo para el análisis detallado de Gemini


@dataclass
class ResumenAuditoria:
    """Resumen ejecutivo de la auditoría"""
    total_licitaciones_perdidas: int = 0
    total_dinero_perdido_precio: float = 0
    
    perdidas_por_precio: int = 0
    perdidas_tecnicas: int = 0
    licitaciones_desiertas: int = 0
    
    top_competidores: Dict[str, int] = None
    oportunidades_inmediatas: List[Dict] = None
    
    def __post_init__(self):
        if self.top_competidores is None:
            self.top_competidores = {}
        if self.oportunidades_inmediatas is None:
            self.oportunidades_inmediatas = []
