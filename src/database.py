"""
Módulo de conexión a base de datos.
Soporta PostgreSQL local y Redshift según configuración.
"""

import os
import logging
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_database_engine() -> Engine:
    """
    Retorna engine de SQLAlchemy según configuración.
    
    Usa la variable USE_REDSHIFT para determinar qué BD usar:
    - true: Redshift (Producción)
    - false: PostgreSQL Local (Desarrollo)
    
    Returns:
        Engine de SQLAlchemy configurado
        
    Raises:
        ValueError: Si faltan variables de entorno necesarias
        
    Example:
        >>> engine = get_database_engine()
        >>> df.to_sql('tabla', engine, if_exists='append')
    """
    use_redshift = os.getenv('USE_REDSHIFT', 'false').lower() == 'true'
    
    if use_redshift:
        # ==========================================
        # REDSHIFT (Producción)
        # ==========================================
        host = os.getenv('REDSHIFT_HOST')
        port = os.getenv('REDSHIFT_PORT', '5439')
        database = os.getenv('REDSHIFT_DB')
        user = os.getenv('REDSHIFT_USER')
        password = os.getenv('REDSHIFT_PASSWORD')
        
        if not all([host, database, user, password]):
            raise ValueError("Faltan variables de entorno de Redshift")
        
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        logger.info("🌐 Conectando a Redshift (Producción)")
        logger.info(f"   Host: {host}")
        logger.info(f"   Database: {database}")
        
    else:
        # ==========================================
        # POSTGRESQL LOCAL (Desarrollo)
        # ==========================================
        host = os.getenv('POSTGRES_HOST', 'postgres')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'airflow')
        user = os.getenv('POSTGRES_USER', 'airflow')
        password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        logger.info("💻 Conectando a PostgreSQL Local (Desarrollo)")
        logger.info(f"   Host: {host}")
        logger.info(f"   Database: {database}")
    
    # Crear engine
    engine = create_engine(
        conn_string,
        pool_pre_ping=True,  # Verifica conexión antes de usar
        echo=False  # No mostrar queries SQL en logs
    )
    
    return engine


def test_connection() -> bool:
    """
    Prueba la conexión a la base de datos.
    
    Returns:
        True si la conexión es exitosa, False si falla
    """
    try:
        engine = get_database_engine()
        
        # Intentar ejecutar query simple
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            result.fetchone()
        
        logger.info("✅ Conexión exitosa a la base de datos")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error de conexión: {str(e)}")
        return False


def get_db_info() -> dict:
    """
    Retorna información sobre la base de datos configurada.
    
    Returns:
        Diccionario con información de la BD
    """
    use_redshift = os.getenv('USE_REDSHIFT', 'false').lower() == 'true'
    
    if use_redshift:
        return {
            'type': 'Redshift',
            'environment': 'Production',
            'host': os.getenv('REDSHIFT_HOST'),
            'port': os.getenv('REDSHIFT_PORT', '5439'),
            'database': os.getenv('REDSHIFT_DB')
        }
    else:
        return {
            'type': 'PostgreSQL',
            'environment': 'Development',
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'airflow')
        }


# Para testing rápido
if __name__ == "__main__":
    print("=" * 60)
    print("TEST DE CONEXIÓN A BASE DE DATOS")
    print("=" * 60)
    
    # Mostrar configuración
    info = get_db_info()
    print(f"\nConfiguración:")
    print(f"  • Tipo: {info['type']}")
    print(f"  • Ambiente: {info['environment']}")
    print(f"  • Host: {info['host']}")
    print(f"  • Puerto: {info['port']}")
    print(f"  • Database: {info['database']}")
    
    # Probar conexión
    print("\nProbando conexión...")
    if test_connection():
        print("\n✅ Todo funcionando correctamente")
    else:
        print("\n❌ Error en la conexión")
