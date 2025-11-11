-- ============================================
-- TABLAS DE STAGING (Área de Aterrizaje)
-- ============================================
-- Estas tablas reciben los datos crudos desde las APIs
-- Son temporales y se truncan en cada ejecución

-- Staging: Productos
DROP TABLE IF EXISTS staging_productos CASCADE;

CREATE TABLE staging_productos (
    producto_id VARCHAR(50),
    nombre VARCHAR(200),
    precio_usd DECIMAL(18,4),
    categoria VARCHAR(100),
    fecha DATE,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging_productos IS 'Staging - Datos crudos de productos desde API';


-- Staging: Tipos de Cambio
DROP TABLE IF EXISTS staging_tipos_cambio CASCADE;

CREATE TABLE staging_tipos_cambio (
    fecha DATE,
    moneda_origen VARCHAR(10),
    moneda_destino VARCHAR(10),
    tipo_cambio DECIMAL(18,6),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging_tipos_cambio IS 'Staging - Tipos de cambio desde API';


-- Staging: Datos Adicionales
DROP TABLE IF EXISTS staging_datos_adicionales CASCADE;

CREATE TABLE staging_datos_adicionales (
    producto_id VARCHAR(50),
    rating VARCHAR(10),
    volumen DECIMAL(18,2),
    fecha DATE,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging_datos_adicionales IS 'Staging - Datos adicionales desde API';


-- Staging: Datos Consolidados (resultado de transformación)
DROP TABLE IF EXISTS staging_consolidado CASCADE;

CREATE TABLE staging_consolidado (
    producto_id VARCHAR(50),
    nombre VARCHAR(200),
    precio_usd DECIMAL(18,4),
    tipo_cambio DECIMAL(18,6),
    precio_local DECIMAL(18,2),
    moneda_local VARCHAR(10),
    categoria VARCHAR(100),
    rating VARCHAR(10),
    volumen DECIMAL(18,2),
    fecha DATE,
    fecha_procesamiento TIMESTAMP,
    pipeline_version VARCHAR(10),
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging_consolidado IS 'Staging - Datos consolidados listos para carga dimensional';


-- Índices para mejorar performance
CREATE INDEX idx_staging_productos_id ON staging_productos(producto_id);
CREATE INDEX idx_staging_productos_fecha ON staging_productos(fecha);
CREATE INDEX idx_staging_tipos_cambio_fecha ON staging_tipos_cambio(fecha);
CREATE INDEX idx_staging_consolidado_id ON staging_consolidado(producto_id);
CREATE INDEX idx_staging_consolidado_fecha ON staging_consolidado(fecha);


-- Log de ejecución
SELECT 
    'Tablas de staging creadas exitosamente' as mensaje,
    CURRENT_TIMESTAMP as fecha_creacion;
