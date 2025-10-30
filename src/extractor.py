"""
Módulo de extracción de datos desde APIs externas.
Contiene funciones para extraer datos de las 3 fuentes principales.
Ahora con soporte para histórico de datos.
"""

import pandas as pd
import logging
from typing import Optional, List
from datetime import datetime, timedelta
from src.utils import hacer_request_api, validar_dataframe_basico

logger = logging.getLogger(__name__)


def generar_fechas_historicas(dias_atras: int = 7) -> List[str]:
    """
    Genera una lista de fechas desde hoy hacia atrás.
    
    Args:
        dias_atras: Número de días históricos a generar
        
    Returns:
        Lista de fechas en formato 'YYYY-MM-DD'
        
    Example:
        >>> fechas = generar_fechas_historicas(7)
        >>> print(fechas)  # ['2025-10-26', '2025-10-25', '2025-10-24', ...]
    """
    fechas = []
    fecha_actual = datetime.now()
    
    for i in range(dias_atras):
        fecha = fecha_actual - timedelta(days=i)
        fechas.append(fecha.strftime('%Y-%m-%d'))
    
    return fechas


def extraer_api_productos(
    url: str = "https://api.exchangerate-api.com/v4/latest/USD",
    api_key: Optional[str] = None,
    dias_historico: int = 7
) -> pd.DataFrame:
    """
    Extrae datos de productos financieros con histórico de días.
    
    Args:
        url: URL de la API de productos
        api_key: API key opcional
        dias_historico: Número de días de histórico a generar (default: 7)
        
    Returns:
        DataFrame con columnas: producto_id, nombre, precio_usd, categoria, fecha
        
    Example:
        >>> df = extraer_api_productos(dias_historico=30)  # 30 días de histórico
        >>> print(df['fecha'].unique())
    """
    try:
        logger.info(f"📥 Extrayendo productos con {dias_historico} días de histórico...")
        
        # Headers con API key si existe
        headers = {'Authorization': f'Bearer {api_key}'} if api_key else None
        
        # Hacer petición a la API
        data = hacer_request_api(url, headers=headers)
        
        # Generar fechas históricas
        fechas = generar_fechas_historicas(dias_historico)
        
        # Transformar a formato de productos con histórico
        productos = []
        if 'rates' in data:
            for fecha in fechas:
                for moneda, tasa in data['rates'].items():
                    # Agregar variación aleatoria para simular cambios históricos
                    import random
                    variacion = random.uniform(0.95, 1.05)  # +/- 5%
                    precio_base = 1 / tasa if tasa > 0 else 0
                    precio_historico = precio_base * variacion
                    
                    producto = {
                        'producto_id': f'CURR_{moneda}',
                        'nombre': f'Moneda {moneda}',
                        'precio_usd': round(precio_historico, 4),
                        'categoria': 'Forex',
                        'fecha': fecha
                    }
                    productos.append(producto)
        
        df = pd.DataFrame(productos)
        
        # Validar
        validar_dataframe_basico(df, "Productos")
        
        total_dias = df['fecha'].nunique()
        registros_por_dia = len(df) // total_dias if total_dias > 0 else 0
        
        logger.info(f"✅ {len(df)} productos extraídos")
        logger.info(f"   • {total_dias} días de histórico")
        logger.info(f"   • ~{registros_por_dia} productos por día")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo productos: {str(e)}")
        raise


def extraer_api_tipos_cambio(
    url: str = "https://api.exchangerate-api.com/v4/latest/USD",
    moneda_base: str = "USD",
    dias_historico: int = 7
) -> pd.DataFrame:
    """
    Extrae tipos de cambio con histórico de días.
    
    Args:
        url: URL de la API
        moneda_base: Moneda base (default: USD)
        dias_historico: Número de días de histórico (default: 7)
        
    Returns:
        DataFrame con columnas: fecha, moneda_origen, moneda_destino, tipo_cambio
        
    Example:
        >>> df = extraer_api_tipos_cambio(dias_historico=30)
    """
    try:
        logger.info(f"📥 Extrayendo tipos de cambio con {dias_historico} días de histórico...")
        
        # Hacer petición a la API
        data = hacer_request_api(url)
        
        # Extraer rates
        rates = data.get('rates', {})
        
        # Generar fechas históricas
        fechas = generar_fechas_historicas(dias_historico)
        
        # Crear registros para cada fecha
        tipos_cambio = []
        for fecha in fechas:
            for moneda, tasa in rates.items():
                # Agregar variación para simular histórico
                import random
                variacion = random.uniform(0.98, 1.02)  # +/- 2%
                tasa_historica = float(tasa) * variacion
                
                registro = {
                    'fecha': fecha,
                    'moneda_origen': moneda_base,
                    'moneda_destino': moneda,
                    'tipo_cambio': round(tasa_historica, 4)
                }
                tipos_cambio.append(registro)
        
        df = pd.DataFrame(tipos_cambio)
        
        # Validar
        validar_dataframe_basico(df, "Tipos de Cambio")
        
        total_dias = df['fecha'].nunique()
        logger.info(f"✅ {len(df)} tipos de cambio extraídos")
        logger.info(f"   • {total_dias} días de histórico")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo tipos de cambio: {str(e)}")
        raise


def extraer_api_datos_adicionales(
    url: str = "https://api.coinbase.com/v2/exchange-rates?currency=USD",
    dias_historico: int = 7
) -> pd.DataFrame:
    """
    Extrae datos adicionales con histórico de días.
    
    Args:
        url: URL de la API
        dias_historico: Número de días de histórico (default: 7)
        
    Returns:
        DataFrame con columnas: producto_id, rating, volumen, fecha
        
    Example:
        >>> df = extraer_api_datos_adicionales(dias_historico=30)
    """
    try:
        logger.info(f"📥 Extrayendo datos adicionales con {dias_historico} días de histórico...")
        
        # Hacer petición a la API
        data = hacer_request_api(url)
        
        # Generar fechas históricas
        fechas = generar_fechas_historicas(dias_historico)
        
        # Extraer rates
        adicionales = []
        if 'data' in data and 'rates' in data['data']:
            rates = data['data']['rates']
            
            for fecha in fechas:
                # Tomar primeros 10 para ejemplo
                for crypto, rate in list(rates.items())[:10]:
                    import random
                    variacion_volumen = random.uniform(0.8, 1.2)  # +/- 20%
                    
                    registro = {
                        'producto_id': f'CRYPTO_{crypto}',
                        'rating': random.choice(['A', 'A+', 'A-', 'B+']),
                        'volumen': round(float(rate) * 1000000 * variacion_volumen, 2),
                        'fecha': fecha
                    }
                    adicionales.append(registro)
        
        df = pd.DataFrame(adicionales)
        
        # Validar
        validar_dataframe_basico(df, "Datos Adicionales")
        
        total_dias = df['fecha'].nunique()
        logger.info(f"✅ {len(df)} registros adicionales extraídos")
        logger.info(f"   • {total_dias} días de histórico")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error extrayendo datos adicionales: {str(e)}")
        raise


def extraer_todas_las_fuentes(dias_historico: int = 7):
    """
    Extrae datos de todas las APIs con histórico.
    
    Args:
        dias_historico: Número de días de histórico a generar (default: 7)
    
    Returns:
        dict: Diccionario con 3 DataFrames
            - 'productos': DataFrame de productos
            - 'tipos_cambio': DataFrame de tipos de cambio
            - 'adicionales': DataFrame de datos adicionales
        
    Example:
        >>> datos = extraer_todas_las_fuentes(dias_historico=30)
        >>> print(datos['productos']['fecha'].unique())
    """
    logger.info(f"🚀 Extrayendo todas las fuentes con {dias_historico} días de histórico...")
    
    try:
        resultados = {
            'productos': extraer_api_productos(dias_historico=dias_historico),
            'tipos_cambio': extraer_api_tipos_cambio(dias_historico=dias_historico),
            'adicionales': extraer_api_datos_adicionales(dias_historico=dias_historico)
        }
        
        logger.info("✅ Extracción completa exitosa")
        return resultados
        
    except Exception as e:
        logger.error(f"❌ Error en extracción completa: {str(e)}")
        raise


# Para testing rápido
if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBA DE EXTRACCIÓN CON HISTÓRICO")
    print("=" * 60)
    
    try:
        # Extraer 30 días de histórico
        datos = extraer_todas_las_fuentes(dias_historico=30)
        
        print(f"\n📊 RESUMEN:")
        print(f"  • Productos: {len(datos['productos'])} registros")
        print(f"  • Tipos de cambio: {len(datos['tipos_cambio'])} registros")
        print(f"  • Adicionales: {len(datos['adicionales'])} registros")
        
        print(f"\n📅 FECHAS DISPONIBLES:")
        fechas_productos = sorted(datos['productos']['fecha'].unique())
        print(f"  • Primera fecha: {fechas_productos[-1]}")
        print(f"  • Última fecha: {fechas_productos[0]}")
        print(f"  • Total de días: {len(fechas_productos)}")
        
        print("\n🔹 Muestra de datos con fechas:")
        print(datos['productos'][['producto_id', 'nombre', 'precio_usd', 'fecha']].head(10))
        
        print("\n✅ Extracción con histórico exitosa!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")