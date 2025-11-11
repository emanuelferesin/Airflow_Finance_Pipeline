-- ============================================
-- DIMENSIÓN: dim_producto (SCD Type 2)
-- ============================================
-- Mantiene historial completo de cambios en productos

DROP TABLE IF EXISTS dim_producto CASCADE;

CREATE TABLE dim_producto (
    -- Surrogate Key (clave artificial autoincremental)
    producto_key SERIAL PRIMARY KEY,
    
    -- Business Key (clave de negocio)
    producto_id VARCHAR(50) NOT NULL,
    
    -- Atributos del producto
    nombre VARCHAR(200),
    categoria VARCHAR(100),
    precio_base DECIMAL(18,4),
    
    -- Campos de SCD Type 2
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE,
    es_actual SMALLINT DEFAULT 1,
    
    -- Metadata
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comentarios
COMMENT ON TABLE dim_producto IS 'Dimensión de Productos con SCD Type 2';
COMMENT ON COLUMN dim_producto.producto_key IS 'Clave surrogate (artificial)';
COMMENT ON COLUMN dim_producto.producto_id IS 'Clave de negocio (natural)';
COMMENT ON COLUMN dim_producto.fecha_inicio IS 'Fecha de inicio de vigencia del registro';
COMMENT ON COLUMN dim_producto.fecha_fin IS 'Fecha de fin de vigencia (NULL = actual)';
COMMENT ON COLUMN dim_producto.es_actual IS '1 = registro actual, 0 = histórico';

-- Índices para optimizar consultas
CREATE INDEX idx_dim_producto_id ON dim_producto(producto_id);
CREATE INDEX idx_dim_producto_actual ON dim_producto(producto_id, es_actual);
CREATE INDEX idx_dim_producto_fechas ON dim_producto(fecha_inicio, fecha_fin);
CREATE UNIQUE INDEX idx_dim_producto_unique_actual ON dim_producto(producto_id) 
    WHERE es_actual = 1;

-- Log de ejecución
SELECT 
    'Dimensión dim_producto creada exitosamente' as mensaje,
    CURRENT_TIMESTAMP as fecha_creacion;
