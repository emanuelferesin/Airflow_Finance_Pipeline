"""
Módulo de transformación de datos.
Consolida y transforma datos extraídos de las APIs.
"""

import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def limpiar_dataframe(df: pd.DataFrame, nombre: str = "DataFrame") -> pd.DataFrame:
    """
    Limpia un DataFrame eliminando duplicados y valores nulos.
    
    Args:
        df: DataFrame a limpiar
        nombre: Nombre descriptivo para logs
        
    Returns:
        DataFrame limpio
        
    Example:
        >>> df_limpio = limpiar_dataframe(df, "Productos")
    """
    logger.info(f"🧹 Limpiando {nombre}...")
    
    filas_iniciales = len(df)
    
    # Eliminar duplicados
    df = df.drop_duplicates()
    duplicados = filas_iniciales - len(df)
    
    # Eliminar filas con todos los valores nulos
    df = df.dropna(how='all')
    
    # Rellenar valores nulos en columnas numéricas con 0
    columnas_numericas = df.select_dtypes(include=['float64', 'int64']).columns
    df[columnas_numericas] = df[columnas_numericas].fillna(0)
    
    # Rellenar valores nulos en columnas de texto con 'N/A'
    columnas_texto = df.select_dtypes(include=['object']).columns
    df[columnas_texto] = df[columnas_texto].fillna('N/A')
    
    filas_finales = len(df)
    
    logger.info(f"✅ {nombre} limpio: {filas_iniciales} → {filas_finales} filas")
    if duplicados > 0:
        logger.info(f"   • Duplicados eliminados: {duplicados}")
    
    return df


def calcular_precio_local(
    df: pd.DataFrame,
    moneda_local: str = 'ARS',
    columna_precio: str = 'precio_usd',
    columna_tipo_cambio: str = 'tipo_cambio'
) -> pd.DataFrame:
    """
    Calcula el precio en moneda local.
    
    Args:
        df: DataFrame con precios y tipos de cambio
        moneda_local: Código de moneda local (default: ARS)
        columna_precio: Nombre de la columna con precio en USD
        columna_tipo_cambio: Nombre de la columna con tipo de cambio
        
    Returns:
        DataFrame con nueva columna 'precio_local'
        
    Example:
        >>> df = calcular_precio_local(df, moneda_local='ARS')
    """
    logger.info(f"💱 Calculando precios en {moneda_local}...")
    
    try:
        # Verificar que existan las columnas necesarias
        if columna_precio not in df.columns:
            logger.warning(f"⚠️ Columna {columna_precio} no existe. Omitiendo cálculo.")
            return df
        
        if columna_tipo_cambio not in df.columns:
            logger.warning(f"⚠️ Columna {columna_tipo_cambio} no existe. Omitiendo cálculo.")
            return df
        
        # Calcular precio local
        df['precio_local'] = df[columna_precio] * df[columna_tipo_cambio]
        
        # Redondear a 2 decimales
        df['precio_local'] = df['precio_local'].round(2)
        
        # Agregar columna de moneda
        df['moneda_local'] = moneda_local
        
        # Calcular estadísticas
        precio_min = df['precio_local'].min()
        precio_max = df['precio_local'].max()
        precio_promedio = df['precio_local'].mean()
        
        logger.info(f"✅ Precios calculados en {moneda_local}:")
        logger.info(f"   • Mínimo: {precio_min:.2f}")
        logger.info(f"   • Máximo: {precio_max:.2f}")
        logger.info(f"   • Promedio: {precio_promedio:.2f}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error calculando precio local: {str(e)}")
        raise


def consolidar_datos(
    df_productos: pd.DataFrame,
    df_tipos_cambio: pd.DataFrame,
    df_adicionales: pd.DataFrame,
    moneda_local: str = 'ARS'
) -> pd.DataFrame:
    """
    Consolida datos de las 3 fuentes en un único DataFrame.
    
    Proceso:
    1. Limpia cada DataFrame
    2. Hace merge de productos con tipos de cambio
    3. Hace merge con datos adicionales
    4. Calcula precio en moneda local
    5. Agrega columnas calculadas
    
    Args:
        df_productos: DataFrame de productos
        df_tipos_cambio: DataFrame de tipos de cambio
        df_adicionales: DataFrame de datos adicionales
        moneda_local: Código de moneda local (default: ARS)
        
    Returns:
        DataFrame consolidado con todas las columnas
        
    Example:
        >>> df_final = consolidar_datos(df_prod, df_tipos, df_adic)
    """
    logger.info("🔄 Iniciando consolidación de datos...")
    
    try:
        # Paso 1: Limpiar cada DataFrame
        df_productos = limpiar_dataframe(df_productos, "Productos")
        df_tipos_cambio = limpiar_dataframe(df_tipos_cambio, "Tipos de Cambio")
        df_adicionales = limpiar_dataframe(df_adicionales, "Datos Adicionales")
        
        # Paso 2: Filtrar tipo de cambio para la moneda local
        logger.info(f"💱 Filtrando tipo de cambio para {moneda_local}...")
        df_tipo_cambio_local = df_tipos_cambio[
            df_tipos_cambio['moneda_destino'] == moneda_local
        ].copy()
        
        if len(df_tipo_cambio_local) == 0:
            logger.warning(f"⚠️ No se encontró tipo de cambio para {moneda_local}")
            # Usar valor por defecto
            df_tipo_cambio_local = pd.DataFrame([{
                'fecha': datetime.now().strftime('%Y-%m-%d'),
                'moneda_origen': 'USD',
                'moneda_destino': moneda_local,
                'tipo_cambio': 1.0
            }])
        
        logger.info(f"✅ Tipo de cambio {moneda_local}: {df_tipo_cambio_local['tipo_cambio'].values[0]:.2f}")
        
        # Paso 3: Agregar fecha a productos si no existe
        if 'fecha' not in df_productos.columns:
            df_productos['fecha'] = datetime.now().strftime('%Y-%m-%d')
        
        # Paso 4: Merge de productos con tipo de cambio
        logger.info("🔗 Haciendo merge: productos + tipo de cambio...")
        df_consolidado = pd.merge(
            df_productos,
            df_tipo_cambio_local[['fecha', 'tipo_cambio']],
            on='fecha',
            how='left'
        )
        
        logger.info(f"✅ Merge 1 completado: {len(df_consolidado)} registros")
        
        # Paso 5: Merge con datos adicionales
        logger.info("🔗 Haciendo merge: resultado + datos adicionales...")
        df_consolidado = pd.merge(
            df_consolidado,
            df_adicionales,
            on='producto_id',
            how='left',
            suffixes=('', '_adicional')
        )
        
        logger.info(f"✅ Merge 2 completado: {len(df_consolidado)} registros")
        
        # Paso 6: Calcular precio en moneda local
        df_consolidado = calcular_precio_local(
            df_consolidado,
            moneda_local=moneda_local
        )
        
        # Paso 7: Agregar columnas de metadata
        df_consolidado['fecha_procesamiento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_consolidado['pipeline_version'] = '1.0'
        
        # Paso 8: Ordenar columnas
        columnas_principales = [
            'producto_id', 'nombre', 'categoria',
            'precio_usd', 'tipo_cambio', 'precio_local', 'moneda_local',
            'fecha', 'fecha_procesamiento'
        ]
        
        # Agregar columnas adicionales que existan
        otras_columnas = [col for col in df_consolidado.columns 
                         if col not in columnas_principales]
        
        columnas_ordenadas = columnas_principales + otras_columnas
        columnas_disponibles = [col for col in columnas_ordenadas 
                               if col in df_consolidado.columns]
        
        df_consolidado = df_consolidado[columnas_disponibles]
        
        # Resumen final
        logger.info("=" * 60)
        logger.info("✅ CONSOLIDACIÓN COMPLETADA")
        logger.info("=" * 60)
        logger.info(f"📊 Total de registros: {len(df_consolidado)}")
        logger.info(f"📋 Total de columnas: {len(df_consolidado.columns)}")
        logger.info(f"🏷️  Columnas: {', '.join(df_consolidado.columns.tolist())}")
        logger.info("=" * 60)
        
        return df_consolidado
        
    except Exception as e:
        logger.error(f"❌ Error en consolidación: {str(e)}")
        raise


def generar_resumen_estadistico(df: pd.DataFrame) -> Dict:
    """
    Genera un resumen estadístico del DataFrame consolidado.
    
    Args:
        df: DataFrame consolidado
        
    Returns:
        Diccionario con estadísticas
        
    Example:
        >>> resumen = generar_resumen_estadistico(df)
        >>> print(resumen['total_registros'])
    """
    logger.info("📊 Generando resumen estadístico...")
    
    resumen = {
        'total_registros': len(df),
        'total_columnas': len(df.columns),
        'columnas': df.columns.tolist(),
        'fecha_procesamiento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Estadísticas de precios si existen
    if 'precio_usd' in df.columns:
        resumen['precio_usd'] = {
            'min': float(df['precio_usd'].min()),
            'max': float(df['precio_usd'].max()),
            'promedio': float(df['precio_usd'].mean()),
            'mediana': float(df['precio_usd'].median())
        }
    
    if 'precio_local' in df.columns:
        resumen['precio_local'] = {
            'min': float(df['precio_local'].min()),
            'max': float(df['precio_local'].max()),
            'promedio': float(df['precio_local'].mean()),
            'mediana': float(df['precio_local'].median())
        }
    
    # Conteo por categoría si existe
    if 'categoria' in df.columns:
        resumen['por_categoria'] = df['categoria'].value_counts().to_dict()
    
    logger.info("✅ Resumen generado")
    
    return resumen


def transformar_datos_completo(
    df_productos: pd.DataFrame,
    df_tipos_cambio: pd.DataFrame,
    df_adicionales: pd.DataFrame,
    moneda_local: str = 'ARS'
) -> tuple:
    """
    Ejecuta el proceso completo de transformación.
    
    Args:
        df_productos: DataFrame de productos
        df_tipos_cambio: DataFrame de tipos de cambio
        df_adicionales: DataFrame de datos adicionales
        moneda_local: Código de moneda local
        
    Returns:
        Tupla (DataFrame consolidado, Dict resumen)
        
    Example:
        >>> df_final, resumen = transformar_datos_completo(df1, df2, df3)
    """
    logger.info("🚀 Iniciando transformación completa...")
    
    # Consolidar datos
    df_consolidado = consolidar_datos(
        df_productos,
        df_tipos_cambio,
        df_adicionales,
        moneda_local
    )
    
    # Generar resumen
    resumen = generar_resumen_estadistico(df_consolidado)
    
    logger.info("✅ Transformación completa finalizada")
    
    return df_consolidado, resumen


# Para testing rápido
if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBA DE TRANSFORMACIÓN")
    print("=" * 60)
    
    # Importar extractor
    from src.extractor import extraer_todas_las_fuentes
    
    try:
        # Extraer datos
        print("\n1️⃣ Extrayendo datos...")
        datos = extraer_todas_las_fuentes()
        
        # Transformar datos
        print("\n2️⃣ Transformando datos...")
        df_final, resumen = transformar_datos_completo(
            datos['productos'],
            datos['tipos_cambio'],
            datos['adicionales'],
            moneda_local='ARS'
        )
        
        # Mostrar resultados
        print("\n" + "=" * 60)
        print("📊 RESULTADO FINAL")
        print("=" * 60)
        print(f"\nTotal de registros: {len(df_final)}")
        print(f"Total de columnas: {len(df_final.columns)}")
        print(f"\nColumnas: {df_final.columns.tolist()}")
        
        print("\n🔹 Primeras 5 filas:")
        print(df_final.head())
        
        print("\n📈 Resumen estadístico:")
        if 'precio_local' in resumen:
            print(f"  • Precio local mínimo: {resumen['precio_local']['min']:.2f}")
            print(f"  • Precio local máximo: {resumen['precio_local']['max']:.2f}")
            print(f"  • Precio local promedio: {resumen['precio_local']['promedio']:.2f}")
        
        print("\n✅ Transformación exitosa!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()