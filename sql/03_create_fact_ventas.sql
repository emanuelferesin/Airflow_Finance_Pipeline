-- ============================================
-- TABLA DE HECHOS: fact_ventas
-- ============================================
-- Almacena las métricas del negocio (precios, volúmenes)

DROP TABLE IF EXISTS fact_ventas CASCADE;

CREATE TABLE fact_ventas (
    -- Clave primaria
    venta_id SERIAL PRIMARY KEY,
    
    -- Foreign Keys (relaciones con dimensiones)
    producto_key INTEGER NOT NULL,
    fecha_key INTEGER NOT NULL,
    
    -- Métricas (measures)
    precio_usd DECIMAL(18,4),
    tipo_cambio DECIMAL(18,6),
    precio_local DECIMAL(18,2),
    volumen DECIMAL(18,2),
    
    -- Atributos degenerados (no justifican una dimensión)
    moneda_local VARCHAR(10),
    rating VARCHAR(10),
    
    -- Metadata
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_version VARCHAR(10)
);

-- Comentarios
COMMENT ON TABLE fact_ventas IS 'Tabla de hechos - Métricas de precios y volúmenes';
COMMENT ON COLUMN fact_ventas.producto_key IS 'FK a dim_producto';
COMMENT ON COLUMN fact_ventas.fecha_key IS 'Fecha en formato YYYYMMDD';
COMMENT ON COLUMN fact_ventas.precio_usd IS 'Precio en dólares';
COMMENT ON COLUMN fact_ventas.precio_local IS 'Precio en moneda local';
COMMENT ON COLUMN fact_ventas.volumen IS 'Volumen de operaciones';

-- Índices para optimizar consultas
CREATE INDEX idx_fact_ventas_producto ON fact_ventas(producto_key);
CREATE INDEX idx_fact_ventas_fecha ON fact_ventas(fecha_key);
CREATE INDEX idx_fact_ventas_fecha_producto ON fact_ventas(fecha_key, producto_key);

-- Foreign Key Constraints (comentados, activar si es necesario)
-- ALTER TABLE fact_ventas 
--     ADD CONSTRAINT fk_fact_producto 
--     FOREIGN KEY (producto_key) REFERENCES dim_producto(producto_key);

-- Log de ejecución
SELECT 
    'Tabla de hechos fact_ventas creada exitosamente' as mensaje,
    CURRENT_TIMESTAMP as fecha_creacion;
