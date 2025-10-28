"""
Módulo de extracción de datos desde APIs externas.
Contiene funciones para extraer datos de las 3 fuentes principales.
"""

import pandas as pd
import logging
from typing import Optional
from datetime import datetime
from src.utils import hacer_request_api, validar_dataframe_basico

logger = logging.getLogger(__name__)


def extraer_api_productos(
    url: str = "https://api.exchangerate-api.com/v4/latest/USD",
    api_key: Optional[str] = None
) -> pd.DataFrame:
    """
    Extrae datos de productos financieros desde una API.
    
    Usamos ExchangeRate API como ejemplo. En producción, 
    reemplazar con tu API real de productos.
    
    Args:
        url: URL de la API de productos
        api_key: API key opcional
        
    Returns:
        DataFrame con columnas: producto_id, nombre, precio_usd, categoria, fecha
        
    Example:
        >>> df = extraer_api_productos()
        >>> print(df.head())
    """
    try:
        logger.info("📥 Extrayendo productos...")
        
        # Headers con API key si existe
        headers = {'Authorization': f'Bearer {api_key}'} if api_key else None
        
        # Hacer request
        data = hacer_request_api(url, headers=headers)
        
        # Transformar a formato de productos
        productos = []
        if 'rates' in data:
            for moneda, tasa in data['rates'].items():
                producto = {
                    'producto_id': f'CURR_{moneda}',
                    'nombre': f'Moneda {moneda}',
                    'precio_usd': round(1 / tasa, 4) if tasa > 0 else 0,
                    'categoria': 'Forex',
                    'fecha': datetime.now().strftime('%Y-%m-%d')
                }
                productos.append(producto)
        
        df = pd.DataFrame(productos)
        
        # Validar
        validar_dataframe_basico(df, "Productos")
        
        logger.info(f"✅ {len(df)} productos extraídos")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo productos: {str(e)}")
        raise


def extraer_api_tipos_cambio(
    url: str = "https://api.exchangerate-api.com/v4/latest/USD",
    moneda_base: str = "USD"
) -> pd.DataFrame:
    """
    Extrae tipos de cambio desde una API.
    
    Args:
        url: URL de la API
        moneda_base: Moneda base (default: USD)
        
    Returns:
        DataFrame con columnas: fecha, moneda_origen, moneda_destino, tipo_cambio
        
    Example:
        >>> df = extraer_api_tipos_cambio()
        >>> print(df[df['moneda_destino'] == 'ARS'])
    """
    try:
        logger.info("📥 Extrayendo tipos de cambio...")
        
        # Hacer request
        data = hacer_request_api(url)
        
        # Extraer fecha y rates
        fecha = data.get('date', datetime.now().strftime('%Y-%m-%d'))
        rates = data.get('rates', {})
        
        # Crear registros
        tipos_cambio = []
        for moneda, tasa in rates.items():
            registro = {
                'fecha': fecha,
                'moneda_origen': moneda_base,
                'moneda_destino': moneda,
                'tipo_cambio': float(tasa)
            }
            tipos_cambio.append(registro)
        
        df = pd.DataFrame(tipos_cambio)
        
        # Validar
        validar_dataframe_basico(df, "Tipos de Cambio")
        
        logger.info(f"✅ {len(df)} tipos de cambio extraídos")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo tipos de cambio: {str(e)}")
        raise


def extraer_api_datos_adicionales(
    url: str = "https://api.coinbase.com/v2/exchange-rates?currency=USD"
) -> pd.DataFrame:
    """
    Extrae datos adicionales desde una API complementaria.
    
    Usa Coinbase API como ejemplo. En producción, reemplazar con tu API.
    
    Args:
        url: URL de la API
        
    Returns:
        DataFrame con columnas: producto_id, rating, volumen, fecha
        
    Example:
        >>> df = extraer_api_datos_adicionales()
        >>> print(df.head())
    """
    try:
        logger.info("📥 Extrayendo datos adicionales...")
        
        # Hacer request
        data = hacer_request_api(url)
        
        # Extraer rates
        adicionales = []
        if 'data' in data and 'rates' in data['data']:
            rates = data['data']['rates']
            
            # Tomar primeros 10 para ejemplo
            for crypto, rate in list(rates.items())[:10]:
                registro = {
                    'producto_id': f'CRYPTO_{crypto}',
                    'rating': 'A',
                    'volumen': round(float(rate) * 1000000, 2),
                    'fecha': datetime.now().strftime('%Y-%m-%d')
                }
                adicionales.append(registro)
        
        df = pd.DataFrame(adicionales)
        
        # Validar
        validar_dataframe_basico(df, "Datos Adicionales")
        
        logger.info(f"✅ {len(df)} registros adicionales extraídos")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo datos adicionales: {str(e)}")
        raise


def extraer_todas_las_fuentes():
    """
    Extrae datos de todas las APIs.
    
    Returns:
        dict: Diccionario con 3 DataFrames
            - 'productos': DataFrame de productos
            - 'tipos_cambio': DataFrame de tipos de cambio
            - 'adicionales': DataFrame de datos adicionales
        
    Example:
        >>> datos = extraer_todas_las_fuentes()
        >>> print(datos['productos'].head())
    """
    logger.info("🚀 Extrayendo todas las fuentes...")
    
    try:
        resultados = {
            'productos': extraer_api_productos(),
            'tipos_cambio': extraer_api_tipos_cambio(),
            'adicionales': extraer_api_datos_adicionales()
        }
        
        logger.info("✅ Extracción completa exitosa")
        return resultados
        
    except Exception as e:
        logger.error(f"❌ Error en extracción completa: {str(e)}")
        raise


# Para testing rápido
if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBA DE EXTRACCIÓN")
    print("=" * 60)
    
    try:
        datos = extraer_todas_las_fuentes()
        
        print(f"\n📊 RESUMEN:")
        print(f"  • Productos: {len(datos['productos'])} registros")
        print(f"  • Tipos de cambio: {len(datos['tipos_cambio'])} registros")
        print(f"  • Adicionales: {len(datos['adicionales'])} registros")
        
        print("\n✅ Extracción exitosa")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")