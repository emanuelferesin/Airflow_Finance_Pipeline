"""
Módulo de conexión a base de datos.
Soporta PostgreSQL DWH local y Redshift según configuración.
IMPORTANTE: Nunca usa la base de datos de metadatos de Airflow.
"""

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_database_engine() -> Engine:
    """
    Retorna engine de SQLAlchemy según configuración.
    
    USA ESTAS BASES:
    - false: postgres_dwh (Base de datos SEPARADA para DWH)
    - true:  Redshift (Producción)
    
    NUNCA USA: postgres:airflow (reservado para Airflow)
    """
    use_redshift = os.getenv('USE_REDSHIFT', 'false').lower() == 'true'
    logger.info(f" use_redshift: {use_redshift}")

    if use_redshift:
        # Redshift (Producción)
        host = os.getenv('REDSHIFT_HOST')
        port = os.getenv('REDSHIFT_PORT')
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
        # PostgreSQL DWH (Desarrollo) - SEPARADO de Airflow
        host = os.getenv('POSTGRES_DWH_HOST')
        port = os.getenv('POSTGRES_DWH_PORT')
        database = os.getenv('POSTGRES_DWH_DB')
        user = os.getenv('POSTGRES_DWH_USER')
        password = os.getenv('POSTGRES_DWH_PASSWORD')
        
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        logger.info("💻 Conectando a PostgreSQL DWH (Desarrollo)")
        logger.info(f"   Host: {host} (SEPARADO de Airflow)")
        logger.info(f"   Database: {database}")
        logger.info("   ✅ Metadatos de Airflow protegidos")
    
    engine = create_engine(
        conn_string,
        pool_pre_ping=True,
        echo=False
    )
    
    return engine


def test_connection() -> bool:
    """Prueba la conexión a la base de datos."""
    try:
        engine = get_database_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        logger.info("✅ Conexión exitosa")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error de conexión: {str(e)}")
        return False


def get_db_info() -> dict:
    """Retorna información sobre la BD configurada."""
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
            'type': 'PostgreSQL DWH',
            'environment': 'Development',
            'host': os.getenv('POSTGRES_DWH_HOST'),
            'port': os.getenv('POSTGRES_DWH_PORT'),
            'database': os.getenv('POSTGRES_DWH_DB')
        }


if __name__ == "__main__":
    print("=" * 60)
    print("TEST DE CONEXIÓN - DATA WAREHOUSE")
    print("=" * 60)
    
    info = get_db_info()
    print(f"\n📊 Configuración:")
    print(f"  • Tipo: {info['type']}")
    print(f"  • Ambiente: {info['environment']}")
    print(f"  • Host: {info['host']}")
    print(f"  • Puerto: {info['port']}")
    print(f"  • Database: {info['database']}")
    
    print("\n🔌 Probando conexión...")
    if test_connection():
        print("\n✅ Todo funcionando correctamente")
        
    else:
        print("\n❌ Error en la conexión")
