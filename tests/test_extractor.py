"""
Tests simples para extractor
"""

import pandas as pd
from src.extractor import extraer_api_productos, extraer_api_tipos_cambio


def test_extractor_productos_funciona():
    """Test básico: verifica que extrae productos"""
    resultado = extraer_api_productos(dias_historico=1)
    
    # Solo verificar que retorna algo
    assert resultado is not None
    assert len(resultado) > 0


def test_extractor_productos_tiene_columnas():
    """Test: verifica que tiene las columnas básicas"""
    resultado = extraer_api_productos(dias_historico=1)
    
    # Verificar columnas importantes
    assert 'producto_id' in resultado.columns
    assert 'precio_usd' in resultado.columns


def test_extractor_tipos_cambio_funciona():
    """Test básico: verifica que extrae tipos de cambio"""
    resultado = extraer_api_tipos_cambio(dias_historico=1)
    
    assert resultado is not None
    assert len(resultado) > 0


def test_extractor_tipos_cambio_tiene_ars():
    """Test: verifica que hay datos de ARS"""
    resultado = extraer_api_tipos_cambio(dias_historico=1)
    
    # Buscar ARS
    tiene_ars = 'ARS' in resultado['moneda_destino'].values
    assert tiene_ars
