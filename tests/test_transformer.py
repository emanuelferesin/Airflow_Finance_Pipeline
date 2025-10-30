"""
Tests simples para transformer
"""

import pandas as pd
from src.transformer import limpiar_dataframe, calcular_precio_local


def test_limpieza_funciona():
    """Test básico: verifica que la limpieza funciona"""
    # Crear datos con duplicados
    df = pd.DataFrame({
        'id': [1, 2, 2, 3],
        'nombre': ['A', 'B', 'B', 'C']
    })
    
    resultado = limpiar_dataframe(df)
    
    # Debe tener menos filas (eliminó duplicados)
    assert len(resultado) < len(df)


def test_calculo_precio_local_funciona():
    """Test básico: verifica que calcula precio local"""
    # Crear datos simples
    df = pd.DataFrame({
        'precio_usd': [100],
        'tipo_cambio': [1000]
    })
    
    resultado = calcular_precio_local(df)
    
    # Debe crear la columna precio_local
    assert 'precio_local' in resultado.columns
    
    # Debe calcular correctamente (100 * 1000 = 100000)
    assert resultado['precio_local'].iloc[0] == 100000.0


def test_calculo_agrega_moneda():
    """Test: verifica que agrega columna de moneda"""
    df = pd.DataFrame({
        'precio_usd': [100],
        'tipo_cambio': [1000]
    })
    
    resultado = calcular_precio_local(df, moneda_local='ARS')
    
    assert 'moneda_local' in resultado.columns
    assert resultado['moneda_local'].iloc[0] == 'ARS'
