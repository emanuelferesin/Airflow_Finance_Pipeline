"""
Módulo de utilidades para el pipeline ETL.
Funciones auxiliares esenciales.
"""

import logging
import requests
from typing import Dict, Any, Optional

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def hacer_request_api(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Realiza una petición GET a una API con manejo básico de errores.
    
    Args:
        url: URL completa de la API
        headers: Headers HTTP opcionales
        timeout: Timeout en segundos (default: 30)
        
    Returns:
        Diccionario con la respuesta JSON
        
    Raises:
        requests.RequestException: Si falla la petición
        
    Example:
        >>> data = hacer_request_api('https://api.example.com/data')
    """
    try:
        logger.info(f"📡 Petición a: {url}")
        
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout
        )
        
        # Verificar si fue exitoso (status 200-299)
        response.raise_for_status()
        
        # Convertir a JSON
        data = response.json()
        
        # Log de éxito
        if isinstance(data, list):
            logger.info(f"✅ Respuesta exitosa: {len(data)} registros")
        elif isinstance(data, dict) and 'rates' in data:
            logger.info(f"✅ Respuesta exitosa: {len(data['rates'])} rates")
        else:
            logger.info(f"✅ Respuesta exitosa")
        
        return data
        
    except requests.exceptions.Timeout:
        logger.error(f"❌ Timeout al conectar con {url}")
        raise
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ Error HTTP {e.response.status_code}: {url}")
        raise
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error en la petición: {str(e)}")
        raise
        
    except ValueError:
        logger.error(f"❌ La respuesta no es JSON válido")
        raise


def validar_dataframe_basico(df, nombre: str = "DataFrame") -> bool:
    """
    Validación básica de DataFrame.
    
    Args:
        df: DataFrame a validar
        nombre: Nombre descriptivo para logs
        
    Returns:
        True si es válido
        
    Raises:
        ValueError: Si el DataFrame está vacío
    """
    if df is None or len(df) == 0:
        error_msg = f"{nombre} está vacío"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    logger.info(f"✅ {nombre} válido: {len(df)} registros, {len(df.columns)} columnas")
    return True