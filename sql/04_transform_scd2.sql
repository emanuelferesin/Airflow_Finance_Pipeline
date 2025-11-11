-- ============================================
-- TRANSFORMACIÓN SCD TYPE 2
-- ============================================
-- Este script actualiza dim_producto manteniendo historial completo

-- PASO 1: Cerrar registros que cambiaron
-- ============================================
-- Si un producto cambió algún atributo, cerrar el registro actual

UPDATE dim_producto
SET 
    fecha_fin = CURRENT_DATE - INTERVAL '1 day',
    es_actual = 0,
    fecha_actualizacion = CURRENT_TIMESTAMP
WHERE es_actual = 1
  AND producto_id IN (
    -- Detectar productos que cambiaron
    SELECT DISTINCT s.producto_id
    FROM staging_consolidado s
    INNER JOIN dim_producto d 
        ON s.producto_id = d.producto_id 
        AND d.es_actual = 1
    WHERE 
        -- Cambió el nombre
        s.nombre != d.nombre
        -- O cambió el precio base
        OR ABS(s.precio_usd - d.precio_base) > 0.01
        -- O cambió la categoría
        OR s.categoria != d.categoria
  );


-- PASO 2: Insertar nuevos registros (productos que cambiaron o son nuevos)
-- ============================================

INSERT INTO dim_producto (
    producto_id,
    nombre,
    categoria,
    precio_base,
    fecha_inicio,
    fecha_fin,
    es_actual
)
SELECT DISTINCT
    s.producto_id,
    s.nombre,
    s.categoria,
    s.precio_usd,
    CURRENT_DATE,
    NULL,
    1
FROM staging_consolidado s
WHERE NOT EXISTS (
    -- No existe un registro actual idéntico
    SELECT 1
    FROM dim_producto d
    WHERE d.producto_id = s.producto_id
      AND d.es_actual = 1
      AND d.nombre = s.nombre
      AND ABS(d.precio_base - s.precio_usd) <= 0.01
      AND d.categoria = s.categoria
);


-- PASO 3: Cargar tabla de hechos
-- ============================================
-- Insertar métricas relacionando con la dimensión actual

INSERT INTO fact_ventas (
    producto_key,
    fecha_key,
    precio_usd,
    tipo_cambio,
    precio_local,
    volumen,
    moneda_local,
    rating,
    pipeline_version
)
SELECT 
    d.producto_key,
    TO_CHAR(s.fecha, 'YYYYMMDD')::INTEGER as fecha_key,
    s.precio_usd,
    s.tipo_cambio,
    s.precio_local,
    s.volumen,
    s.moneda_local,
    s.rating,
    s.pipeline_version
FROM staging_consolidado s
INNER JOIN dim_producto d 
    ON s.producto_id = d.producto_id 
    AND d.es_actual = 1;


-- PASO 4: Log de resultados
-- ============================================

SELECT 
    'Transformación SCD2 completada' as mensaje,
    (SELECT COUNT(*) FROM dim_producto WHERE es_actual = 1) as productos_actuales,
    (SELECT COUNT(*) FROM dim_producto WHERE es_actual = 0) as productos_historicos,
    (SELECT COUNT(*) FROM fact_ventas) as registros_hechos,
    CURRENT_TIMESTAMP as fecha_ejecucion;
