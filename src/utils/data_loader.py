import pandas as pd
import logging
from typing import List
from ..models.data_models import LicitacionPerdida

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    @staticmethod
    def cargar_datos_excel(filepath: str) -> List[LicitacionPerdida]:
        """Carga datos desde Excel y los convierte a objetos LicitacionPerdida"""
        try:
            df = pd.read_excel(filepath)
            
            # Normalizar columnas
            columnas_requeridas = ['NroLicitacion', 'NombreItem', 'MontoNetoOferta', 'ProveedorRUT']
            if not all(col in df.columns for col in columnas_requeridas):
                raise ValueError(f"El Excel debe contener las columnas: {columnas_requeridas}")
            
            licitaciones = []
            for _, row in df.iterrows():
                # Extract Code (handle potential NaN/missing)
                codigo_onu = row.get('CodigoProductoONU')
                if pd.isna(codigo_onu):
                    codigo_onu = None
                else:
                    try:
                        codigo_onu = int(codigo_onu)
                    except:
                        codigo_onu = None

                licit = LicitacionPerdida(
                    id_licitacion=str(row['NroLicitacion']),
                    producto_cliente=str(row['NombreItem']),
                    precio_oferta_cliente=float(row['MontoNetoOferta']),
                    rut_cliente=str(row['ProveedorRUT']),
                    fecha_licitacion=str(row.get('FechaAdjudicacion', '')),
                    cantidad=float(row.get('CantidadOferta', 1)),
                    codigo_producto_onu=codigo_onu
                )
                licitaciones.append(licit)
            
            logger.info(f"✅ Cargadas {len(licitaciones)} licitaciones desde {filepath}")
            return licitaciones
            
        except Exception as e:
            logger.error(f"❌ Error cargando Excel: {e}")
            return []
