"""
DAG Completo del Pipeline ETL Financiero
Extrae datos de 3 APIs, transforma y carga a Data Warehouse con SCD Type 2
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging

# Importar nuestros módulos
import sys
sys.path.insert(0, '/opt/airflow')

from src.extractor import extraer_todas_las_fuentes
from src.transformer import transformar_datos_completo

# Configurar logging
logger = logging.getLogger(__name__)


# ============================================
# FUNCIONES PARA LAS TAREAS
# ============================================

def tarea_extraer_datos(**context):
    """
    Tarea 1: Extrae datos de las 3 APIs
    """
    logger.info("Iniciando extracción de datos...")
    
    try:
        # Extraer datos (1 día para ejecución diaria)
        datos = extraer_todas_las_fuentes(dias_historico=1)
        
        # Log de resumen
        logger.info(f"✅ Extracción completada:")
        logger.info(f"  • Productos: {len(datos['productos'])} registros")
        logger.info(f"  • Tipos de cambio: {len(datos['tipos_cambio'])} registros")
        logger.info(f"  • Adicionales: {len(datos['adicionales'])} registros")
        
        # Guardar en XCom para la siguiente tarea
        context['ti'].xcom_push(key='datos_extraidos', value={
            'productos_count': len(datos['productos']),
            'tipos_cambio_count': len(datos['tipos_cambio']),
            'adicionales_count': len(datos['adicionales'])
        })
        
        # Retornar los DataFrames para la siguiente tarea
        return datos
        
    except Exception as e:
        logger.error(f"❌ Error en extracción: {str(e)}")
        raise


def tarea_transformar_datos(**context):
    """
    Tarea 2: Transforma y consolida los datos
    """
    logger.info("🔄 Iniciando transformación de datos...")
    
    try:
        # Obtener datos de la tarea anterior
        datos = context['ti'].xcom_pull(task_ids='extraer_datos')
        
        if not datos:
            raise ValueError("No se recibieron datos de la tarea de extracción")
        
        # Transformar datos
        df_final, resumen = transformar_datos_completo(
            datos['productos'],
            datos['tipos_cambio'],
            datos['adicionales'],
            moneda_local='ARS'
        )
        
        # Log de resumen
        logger.info(f"✅ Transformación completada:")
        logger.info(f"  • Total registros: {len(df_final)}")
        logger.info(f"  • Columnas: {len(df_final.columns)}")
        logger.info(f"COLUMNAS_DF = {df_final.columns.tolist()}")
        
        # Guardar resumen en XCom
        context['ti'].xcom_push(key='datos_transformados', value={
            'registros_totales': len(df_final),
            'columnas_totales': len(df_final.columns),
            'resumen': resumen
        })
        
        logger.info("📊 Datos listos para carga")
        
        # Retornar DataFrame para la siguiente tarea
        return df_final
        
    except Exception as e:
        logger.error(f"❌ Error en transformación: {str(e)}")
        raise


def tarea_cargar_staging(**context):
    """
    Tarea 3: Carga datos consolidados a staging
    """
    logger.info("💾 Iniciando carga a staging...")
    
    try:
        # Obtener datos transformados de la tarea anterior
        df_consolidado = context['ti'].xcom_pull(task_ids='transformar_datos')
        
        if df_consolidado is None or len(df_consolidado) == 0:
            raise ValueError("No hay datos transformados para cargar")
        
        # Importar función de conexión a base de datos
        from src.database import get_database_engine
        from sqlalchemy import text
        
        # Obtener engine (usa USE_REDSHIFT de .env automáticamente)
        engine = get_database_engine()
        
        # Obtener configuración de ambiente
        import os
        use_redshift = os.getenv('USE_REDSHIFT', 'false').lower() == 'true'
        
        # Definir schema según ambiente
        if use_redshift:
            schema = "2025_emanuel_alcides_feresin_schema"
        else:
            schema = "public"  # PostgreSQL usa public por defecto
        
        tabla = "staging_consolidado"
        tabla_completa = f'"{schema}".{tabla}' if use_redshift else tabla
        
        # Limpiar staging antes de cargar
        logger.info(f"🧹 Limpiando tabla {tabla_completa}...")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {tabla_completa}"))
        
        # Cargar datos a staging
        logger.info(f"📤 Cargando {len(df_consolidado)} registros a {tabla_completa}...")
        df_consolidado.to_sql(
            tabla,
            engine,
            schema=schema,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"✅ Carga completada: {len(df_consolidado)} registros en staging")
        
        # Guardar conteo en XCom
        context['ti'].xcom_push(key='registros_cargados', value=len(df_consolidado))
        
        return len(df_consolidado)
        
    except Exception as e:
        logger.error(f"❌ Error en carga a staging: {str(e)}")
        raise


def tarea_resumen_final(**context):
    """
    Tarea final: Muestra resumen completo de la ejecución
    """
    logger.info("📋 Generando resumen final...")
    
    try:
        # Obtener datos de XCom
        datos_extraidos = context['ti'].xcom_pull(
            task_ids='extraer_datos',
            key='datos_extraidos'
        )
        
        datos_transformados = context['ti'].xcom_pull(
            task_ids='transformar_datos',
            key='datos_transformados'
        )
        
        registros_cargados = context['ti'].xcom_pull(
            task_ids='cargar_staging',
            key='registros_cargados'
        )
        
        # Mostrar resumen
        logger.info("=" * 60)
        logger.info("✅ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 60)
        
        if datos_extraidos:
            logger.info("📥 EXTRACCIÓN:")
            logger.info(f"  • Productos: {datos_extraidos['productos_count']}")
            logger.info(f"  • Tipos de cambio: {datos_extraidos['tipos_cambio_count']}")
            logger.info(f"  • Adicionales: {datos_extraidos['adicionales_count']}")
        
        if datos_transformados:
            logger.info("\n🔄 TRANSFORMACIÓN:")
            logger.info(f"  • Registros consolidados: {datos_transformados['registros_totales']}")
            logger.info(f"  • Columnas: {datos_transformados['columnas_totales']}")
        
        if registros_cargados:
            logger.info("\n💾 CARGA:")
            logger.info(f"  • Registros en staging: {registros_cargados}")
            logger.info(f"  • Tabla: staging_consolidado")
        
        logger.info("\n🎯 TRANSFORMACIÓN DIMENSIONAL:")
        logger.info("  ✅ SCD Type 2 ejecutado")
        logger.info("  ✅ Datos en dim_producto")
        logger.info("  ✅ Datos en fact_ventas")
        
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en resumen: {str(e)}")
        return False


# ============================================
# CONFIGURACIÓN DEL DAG
# ============================================

# Argumentos por defecto
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG
with DAG(
    dag_id='pipeline_financiero_etl',
    default_args=default_args,
    description='Pipeline ETL Completo - Extracción, Transformación y Carga con SCD Type 2',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['etl', 'finance', 'production'],
    doc_md=__doc__,
) as dag:
    
    # ============================================
    # DEFINICIÓN DE TAREAS
    # ============================================
    
    # Tarea inicial (dummy)
    inicio = EmptyOperator(
        task_id='inicio',
        doc_md="Marca el inicio del pipeline"
    )
    
    # Tarea 1: Extracción
    extraer_datos = PythonOperator(
        task_id='extraer_datos',
        python_callable=tarea_extraer_datos,
        doc_md="""
        # Extracción de Datos
        
        Extrae datos de 3 APIs:
        - API 1: Productos financieros
        - API 2: Tipos de cambio
        - API 3: Datos adicionales
        
        Retorna DataFrames de Pandas con los datos extraídos.
        """
    )
    
    # Tarea 2: Transformación
    transformar_datos = PythonOperator(
        task_id='transformar_datos',
        python_callable=tarea_transformar_datos,
        doc_md="""
        # Transformación de Datos
        
        - Limpia datos (duplicados, nulos)
        - Consolida 3 fuentes
        - Calcula precio local (USD → ARS)
        - Genera resumen estadístico
        """
    )
    
    # Tarea 3: Carga a Staging
    cargar_staging = PythonOperator(
        task_id='cargar_staging',
        python_callable=tarea_cargar_staging,
        doc_md="""
        # Carga a Staging
        
        - Limpia tabla staging_consolidado (TRUNCATE)
        - Carga datos consolidados
        - Usa configuración de USE_REDSHIFT para elegir BD
        - Soporta PostgreSQL DWH (dev) y Redshift (prod)
        """
    )
    
    # Tarea 4: Transformación SCD2
    transform_scd2 = PostgresOperator(
        task_id='transform_scd2',
        postgres_conn_id='postgres_dwh',
        sql='sql/04_transform_scd2.sql',
        doc_md="""
        # Transformación SCD Type 2
        
        - Cierra registros que cambiaron (es_actual=0)
        - Inserta nuevos registros en dim_producto
        - Carga datos a fact_ventas
        - Mantiene historial completo de cambios
        """
    )
    
    # Tarea 5: Resumen
    resumen_final = PythonOperator(
        task_id='resumen_final',
        python_callable=tarea_resumen_final,
        doc_md="Genera resumen completo de la ejecución del pipeline"
    )
    
    # Tarea final (dummy)
    fin = EmptyOperator(
        task_id='fin',
        doc_md="Marca el fin del pipeline"
    )
    
    # ============================================
    # DEPENDENCIAS (FLUJO DE EJECUCIÓN)
    # ============================================
    
    inicio >> extraer_datos >> transformar_datos >> cargar_staging >> transform_scd2 >> resumen_final >> fin


# ============================================
# DOCUMENTACIÓN DEL DAG
# ============================================

"""
## Pipeline ETL Financiero Completo

### Descripción
Pipeline diario que extrae datos de APIs financieras, transforma, consolida
y carga a Data Warehouse con modelado dimensional SCD Type 2.

### Flujo de Ejecución
1. **Inicio**: Marca inicio del pipeline
2. **Extracción**: Extrae de 3 APIs (productos, tipos de cambio, adicionales)
3. **Transformación**: Consolida y calcula precios locales
4. **Carga a Staging**: Inserta datos en staging_consolidado
5. **Transformación SCD2**: Ejecuta SQL para modelado dimensional
6. **Resumen**: Genera log con estadísticas completas
7. **Fin**: Marca fin del pipeline

### Arquitectura de Datos
```
APIs → Extracción → Transformación → Staging → SCD2 → Dimensional
                                        ↓
                                   PostgreSQL DWH
                                        o
                                    Redshift
```

### Bases de Datos Soportadas
- **Development**: PostgreSQL DWH (postgres_dwh:5433) - (configurar USE_REDSHIFT=false en .env)
- **Production**: Redshift (configurar USE_REDSHIFT=true en .env)
- **Switch**: Cambiar variable USE_REDSHIFT sin modificar código

### Tablas Creadas
- **Staging**: staging_consolidado (datos temporales)
- **Dimensión**: dim_producto (SCD Type 2 con historial)
- **Hechos**: fact_ventas (métricas de precios)

### Schedule
- **Frecuencia**: Diaria (@daily)
- **Hora**: 00:00 UTC
- **Catchup**: Desactivado

### Configuración
- **Retries**: 2
- **Retry Delay**: 5 minutos
- **Owner**: data_team
- **Conexión BD**: postgres_dwh (configurar en Airflow UI)

### Monitoreo
Verificar tablas en DBeaver:

```
"""
