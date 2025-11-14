-- ==========================================================
--   SCD TYPE 2: Transformación completa de productos
-- ==========================================================

-- ==========================================
-- 1) Cerrar registros actuales que cambiaron
-- ==========================================

UPDATE dim_producto d
SET 
    fecha_fin = CURRENT_DATE - INTERVAL '1 day',
    es_actual = 0,
    fecha_actualizacion = CURRENT_TIMESTAMP
FROM staging_consolidado s
WHERE d.es_actual = 1
  AND d.producto_id = s.producto_id
  AND (
        d.nombre != s.nombre
     OR d.categoria != s.categoria
     OR ABS(d.precio_base - s.precio_usd) > 0.01
  );

-- ==========================================
-- 2) Insertar nuevas versiones (SCD2)
-- ==========================================

INSERT INTO dim_producto (
    producto_id,
    nombre,
    categoria,
    precio_base,
    fecha_inicio,
    fecha_fin,
    es_actual,
    fecha_creacion,
    fecha_actualizacion
)
SELECT
    s.producto_id,
    s.nombre,
    s.categoria,
    s.precio_usd,
    CURRENT_DATE,
    NULL,
    1,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM staging_consolidado s
LEFT JOIN dim_producto d
    ON d.producto_id = s.producto_id
   AND d.es_actual = 1
WHERE 
    d.producto_id IS NULL
 OR  d.nombre != s.nombre
 OR  d.categoria != s.categoria
 OR  ABS(d.precio_base - s.precio_usd) > 0.01;

-- ==========================================
-- 3) Insertar hechos con la versión actual
-- ==========================================

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
    TO_CHAR(s.fecha, 'YYYYMMDD')::INTEGER,
    s.precio_usd,
    s.tipo_cambio,
    s.precio_local,
    s.volumen,
    s.moneda_local,
    s.rating,
    s.pipeline_version
FROM staging_consolidado s
JOIN dim_producto d
  ON d.producto_id = s.producto_id
 AND d.es_actual = 1;

-- ==========================================
-- 4) Resultado
-- ==========================================
SELECT 
    'Transformación SCD2 completada' AS mensaje,
    (SELECT COUNT(*) FROM dim_producto WHERE es_actual = 1) AS productos_actuales,
    (SELECT COUNT(*) FROM dim_producto WHERE es_actual = 0) AS productos_historicos,
    (SELECT COUNT(*) FROM fact_ventas) AS registros_hechos,
    CURRENT_TIMESTAMP AS fecha_ejecucion;
