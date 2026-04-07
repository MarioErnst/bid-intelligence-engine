from dataclasses import dataclass
from typing import Optional, List

@dataclass
class LicitacionPerdida:
    id_licitacion: str
    producto_cliente: str
    precio_oferta_cliente: float
    rut_cliente: str
    fecha_licitacion: str
    cantidad: float
    codigo_producto_onu: Optional[int] = None  # Added field
    precio_ganador: Optional[float] = None
    proveedor_ganador: Optional[str] = None
    rut_ganador: Optional[str] = None
    causa_derrota: Optional[str] = None
    gap_monetario: Optional[float] = None
    gap_porcentual: Optional[float] = None
    insight: Optional[str] = None
    analisis_ai: Optional[str] = None
    estado_licitacion: Optional[str] = None
    url_acta: Optional[str] = None
    path_pdf_informe: Optional[str] = None
    evidencia_pdf: Optional[str] = None  # Texto extraído del PDF
