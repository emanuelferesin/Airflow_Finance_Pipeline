"""
DAG Principal del Pipeline ETL Financiero
Extrae datos de 3 APIs, transforma y prepara para carga
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    logger.info("🚀 Iniciando extracción de datos...")
    
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
        
        # Guardar resumen en XCom
        context['ti'].xcom_push(key='datos_transformados', value={
            'registros_totales': len(df_final),
            'columnas_totales': len(df_final.columns),
            'resumen': resumen
        })
        
        logger.info("📊 Datos listos para carga (próximamente)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en transformación: {str(e)}")
        raise


def tarea_resumen_final(**context):
    """
    Tarea final: Muestra resumen de la ejecución
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
        
        # Mostrar resumen
        logger.info("=" * 60)
        logger.info("✅ PIPELINE EJECUTADO EXITOSAMENTE")
        logger.info("=" * 60)
        
        if datos_extraidos:
            logger.info("📥 EXTRACCIÓN:")
            logger.info(f"  • Productos: {datos_extraidos['productos_count']}")
            logger.info(f"  • Tipos de cambio: {datos_extraidos['tipos_cambio_count']}")
            logger.info(f"  • Adicionales: {datos_extraidos['adicionales_count']}")
        
        if datos_transformados:
            logger.info("\n🔄 TRANSFORMACIÓN:")
            logger.info(f"  • Registros finales: {datos_transformados['registros_totales']}")
            logger.info(f"  • Columnas: {datos_transformados['columnas_totales']}")
        
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
    'start_date': datetime(2025, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG
with DAG(
    dag_id='pipeline_financiero_etl',
    default_args=default_args,
    description='Pipeline ETL de datos financieros - Extracción y Transformación',
    schedule_interval='@daily',  # Ejecuta una vez al día
    start_date=datetime(2025, 10, 26),
    catchup=False,  # No ejecutar fechas pasadas
    tags=['etl', 'finance', 'daily'],
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
    
    # Tarea 3: Resumen
    resumen_final = PythonOperator(
        task_id='resumen_final',
        python_callable=tarea_resumen_final,
        doc_md="Genera resumen final de la ejecución"
    )
    
    # Tarea final (dummy)
    fin = EmptyOperator(
        task_id='fin',
        doc_md="Marca el fin del pipeline"
    )
    
    # ============================================
    # DEPENDENCIAS (FLUJO DE EJECUCIÓN)
    # ============================================
    
    inicio >> extraer_datos >> transformar_datos >> resumen_final >> fin

# ============================================
# DOCUMENTACIÓN DEL DAG
# ============================================

"""
## Pipeline ETL Financiero

### Descripción
Pipeline diario que extrae datos de APIs financieras, transforma y consolida
la información calculando precios en moneda local.

### Flujo de Ejecución
1. **Inicio**: Marca inicio del pipeline
2. **Extracción**: Extrae de 3 APIs (productos, tipos de cambio, adicionales)
3. **Transformación**: Consolida y calcula precios locales
4. **Resumen**: Genera log con estadísticas
5. **Fin**: Marca fin del pipeline

### Próximas Mejoras
- Día 8: Agregar carga a staging (PostgreSQL)
- Día 9: Agregar modelado dimensional (SCD Type 2)

### Schedule
- **Frecuencia**: Diaria (@daily)
- **Hora**: 00:00 UTC
- **Catchup**: Desactivado

### Configuración
- **Retries**: 2
- **Retry Delay**: 5 minutos
- **Owner**: data_team
"""
