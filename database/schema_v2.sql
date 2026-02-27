-- ============================================
-- PLATAFORMA DE GESTIÓN DE INVENTARIO
-- Distribuidora de Seguridad Electrónica
-- Schema PostgreSQL - Fase 1 (ACTUALIZADO)
-- ============================================

-- Extensiones necesarias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- TABLAS MAESTRAS
-- ============================================

-- Tipos de Almacén
CREATE TABLE tipos_almacen (
    tipo VARCHAR(50) PRIMARY KEY,
    descripcion TEXT,
    incluir_en_analisis_ventas BOOLEAN DEFAULT FALSE,  -- Solo "Venta" = TRUE
    incluir_en_analisis_inventario BOOLEAN DEFAULT TRUE,
    activo BOOLEAN DEFAULT TRUE
);

-- Regionales
CREATE TABLE regionales (
    codigo VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    activo BOOLEAN DEFAULT TRUE
);

-- Almacenes/Bodegas (con datos reales)
CREATE TABLE almacenes (
    codigo VARCHAR(10) PRIMARY KEY,          -- "0001", "0012", etc.
    nombre VARCHAR(100) NOT NULL,            -- Nombre para dashboard
    tipo VARCHAR(50) REFERENCES tipos_almacen(tipo),
    regional VARCHAR(20) REFERENCES regionales(codigo),
    es_cedi BOOLEAN DEFAULT FALSE,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Clasificación de Marcas
CREATE TABLE clasificacion_marcas (
    categoria VARCHAR(20) NOT NULL,          -- PRINCIPAL, OTRAS
    clasificacion CHAR(1) NOT NULL,          -- A, B, C, D
    descripcion_categoria TEXT,
    descripcion_clasificacion TEXT,
    prioridad_compra INTEGER,                -- 1 = más prioritario
    PRIMARY KEY (categoria, clasificacion)
);

-- Marcas con parámetros de compra
CREATE TABLE marcas (
    codigo VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    nombre_fomplus VARCHAR(100),             -- Nombre en tu sistema (para mapeo)
    -- Clasificación
    categoria VARCHAR(20) DEFAULT 'OTRAS',   -- PRINCIPAL, OTRAS
    clasificacion CHAR(1) DEFAULT 'D',       -- A, B, C, D (rotación)
    -- Parámetros de compra
    periodicidad_compra_dias INTEGER,
    dias_cobertura_stock INTEGER,
    lead_time_proveedor INTEGER,
    lead_time_a_cedi INTEGER,
    cobertura_total INTEGER GENERATED ALWAYS AS (dias_cobertura_stock + lead_time_a_cedi) STORED,
    tipo_proveedor VARCHAR(100),
    origen VARCHAR(50),                      -- USA, CHINA, NACIONAL, PANAMA
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Productos/Referencias
CREATE TABLE productos (
    referencia VARCHAR(50) PRIMARY KEY,      -- "DS-PDP15P-EG2-WB(B"
    codigo VARCHAR(20),                      -- Código interno "302402270"
    nombre TEXT,
    unidad_medida VARCHAR(20),               -- "94-und"
    clase VARCHAR(10),
    grupo VARCHAR(10),
    linea VARCHAR(10),
    marca_codigo VARCHAR(10) REFERENCES marcas(codigo),
    modelo VARCHAR(50),
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vendedores
CREATE TABLE vendedores (
    codigo VARCHAR(10) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    almacen_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- TABLAS TRANSACCIONALES
-- ============================================

-- Ventas (histórico desde tu API)
CREATE TABLE ventas (
    id BIGSERIAL PRIMARY KEY,
    -- Identificación documento
    tipo_movimiento VARCHAR(10),             -- TIPMOV
    prefijo VARCHAR(10),                     -- PREFIJO
    numero_documento VARCHAR(20),            -- NUMDOC
    fecha DATE NOT NULL,
    hora TIME,
    -- Cliente
    cedula_cliente VARCHAR(20),
    nombre_cliente VARCHAR(200),
    -- Ubicación
    codigo_seccion VARCHAR(20),              -- CODSEC
    nombre_seccion VARCHAR(100),             -- Ciudad/Sucursal
    bodega_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    -- Producto
    referencia VARCHAR(50) REFERENCES productos(referencia),
    -- Cantidades y valores
    cantidad DECIMAL(15,5) NOT NULL,
    valor_unitario DECIMAL(15,5),
    valor_total DECIMAL(15,5),
    porcentaje_descuento DECIMAL(10,5),
    valor_descuento DECIMAL(15,5),
    valor_costo DECIMAL(15,5),
    valor_utilidad DECIMAL(15,5),
    porcentaje_utilidad DECIMAL(10,5),
    porcentaje_iva DECIMAL(5,2),
    -- Vendedor
    vendedor_codigo VARCHAR(10),
    vendedor_nombre VARCHAR(100),
    -- Control
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Índices para consultas frecuentes
    UNIQUE(prefijo, numero_documento, referencia)
);

-- Inventario snapshot (foto diaria desde tu API)
CREATE TABLE inventario_snapshot (
    id BIGSERIAL PRIMARY KEY,
    fecha_snapshot DATE NOT NULL,
    bodega_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    referencia VARCHAR(50) REFERENCES productos(referencia),
    cantidad DECIMAL(15,5) NOT NULL,
    cantidad_secundaria DECIMAL(15,5),
    valor_costo DECIMAL(15,5),
    valor_venta DECIMAL(15,5),
    valor_descuento DECIMAL(15,5),
    ubicacion VARCHAR(50),
    lote VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Un registro por producto/bodega/día
    UNIQUE(fecha_snapshot, bodega_codigo, referencia)
);

-- Inventario actual (última foto - para consultas rápidas)
CREATE TABLE inventario_actual (
    bodega_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    referencia VARCHAR(50) REFERENCES productos(referencia),
    cantidad DECIMAL(15,5) NOT NULL,
    valor_costo DECIMAL(15,5),
    valor_venta DECIMAL(15,5),
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (bodega_codigo, referencia)
);

-- ============================================
-- TABLAS DE ANÁLISIS (Calculadas)
-- ============================================

-- Métricas de producto por almacén
CREATE TABLE metricas_producto_almacen (
    id BIGSERIAL PRIMARY KEY,
    fecha_calculo DATE NOT NULL,
    referencia VARCHAR(50) REFERENCES productos(referencia),
    bodega_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    
    -- Stock actual
    stock_actual DECIMAL(15,5),
    valor_stock DECIMAL(15,2),
    
    -- Ventas período (solo considera bodegas tipo Venta)
    venta_ultimos_7_dias DECIMAL(15,5),
    venta_ultimos_30_dias DECIMAL(15,5),
    venta_ultimos_90_dias DECIMAL(15,5),
    
    -- Promedios
    venta_diaria_promedio DECIMAL(15,5),      -- Base: últimos 30 días
    
    -- Métricas calculadas
    dias_inventario DECIMAL(10,2),            -- Stock / Venta diaria
    rotacion_mensual DECIMAL(10,4),           -- Venta 30d / Stock promedio
    
    -- Puntos de control
    punto_reorden DECIMAL(15,5),              -- Considerando lead time
    stock_seguridad DECIMAL(15,5),
    stock_maximo DECIMAL(15,5),               -- Basado en cobertura marca
    
    -- Estado
    estado_stock VARCHAR(20),                 -- CRITICO, BAJO, NORMAL, SOBRE, EXCESO
    requiere_traslado BOOLEAN DEFAULT FALSE,
    requiere_compra BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(fecha_calculo, referencia, bodega_codigo)
);

-- Métricas consolidadas por producto (toda la red)
CREATE TABLE metricas_producto_red (
    id BIGSERIAL PRIMARY KEY,
    fecha_calculo DATE NOT NULL,
    referencia VARCHAR(50) REFERENCES productos(referencia),
    
    -- Stock total red (TODOS los tipos de almacén)
    stock_total_red DECIMAL(15,5),
    valor_total_red DECIMAL(15,2),
    
    -- Stock solo en almacenes de VENTA
    stock_almacenes_venta DECIMAL(15,5),
    
    -- Distribución
    almacenes_con_stock INTEGER,
    almacenes_venta_con_stock INTEGER,
    almacenes_venta_sin_stock INTEGER,
    desviacion_stock DECIMAL(10,4),           -- Qué tan desbalanceado está
    
    -- Ventas red (solo bodegas tipo Venta)
    venta_total_30_dias DECIMAL(15,5),
    venta_diaria_red DECIMAL(15,5),
    
    -- Métricas
    dias_inventario_red DECIMAL(10,2),
    rotacion_red DECIMAL(10,4),
    
    -- Clasificación ABC por valor venta
    clasificacion_abc_venta CHAR(1),
    
    -- Estado general
    estado_red VARCHAR(20),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(fecha_calculo, referencia)
);

-- Métricas por Regional
CREATE TABLE metricas_regional (
    id BIGSERIAL PRIMARY KEY,
    fecha_calculo DATE NOT NULL,
    regional VARCHAR(20) REFERENCES regionales(codigo),
    
    -- Totales
    valor_inventario_total DECIMAL(15,2),
    valor_venta_30_dias DECIMAL(15,2),
    
    -- Conteos
    productos_en_stock INTEGER,
    productos_sin_stock INTEGER,
    productos_criticos INTEGER,
    productos_sobreinventario INTEGER,
    
    -- KPIs
    rotacion_promedio DECIMAL(10,4),
    pct_venta_sobre_disponibilidad DECIMAL(10,4),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(fecha_calculo, regional)
);

-- ============================================
-- TABLAS DE ALERTAS Y RECOMENDACIONES
-- ============================================

-- Alertas generadas
CREATE TABLE alertas (
    id BIGSERIAL PRIMARY KEY,
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tipo_alerta VARCHAR(30) NOT NULL,         -- STOCK_CRITICO, STOCK_BAJO, SOBREINVENTARIO, BAJA_ROTACION, SIN_STOCK
    nivel VARCHAR(10) NOT NULL,               -- CRITICO, ALTO, MEDIO, BAJO
    referencia VARCHAR(50) REFERENCES productos(referencia),
    bodega_codigo VARCHAR(10) REFERENCES almacenes(codigo),
    
    -- Datos de contexto
    stock_actual DECIMAL(15,5),
    dias_inventario DECIMAL(10,2),
    venta_diaria DECIMAL(15,5),
    
    -- Info de marca para priorización
    marca_categoria VARCHAR(20),              -- PRINCIPAL, OTRAS
    marca_clasificacion CHAR(1),              -- A, B, C, D
    
    -- Mensaje
    mensaje TEXT,
    
    -- Estado
    estado VARCHAR(20) DEFAULT 'PENDIENTE',   -- PENDIENTE, VISTA, ATENDIDA, IGNORADA
    atendida_por VARCHAR(100),
    fecha_atencion TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recomendaciones de traslado
CREATE TABLE recomendaciones_traslado (
    id BIGSERIAL PRIMARY KEY,
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    referencia VARCHAR(50) REFERENCES productos(referencia),
    
    -- Origen y destino
    bodega_origen VARCHAR(10) REFERENCES almacenes(codigo),
    bodega_destino VARCHAR(10) REFERENCES almacenes(codigo),
    regional_destino VARCHAR(20),
    
    -- Cantidades
    cantidad_sugerida DECIMAL(15,5),
    
    -- Justificación
    dias_inv_origen DECIMAL(10,2),            
    dias_inv_destino DECIMAL(10,2),           
    dias_inv_destino_despues DECIMAL(10,2),   
    
    -- Prioridad basada en clasificación marca
    marca_categoria VARCHAR(20),
    marca_clasificacion CHAR(1),
    prioridad VARCHAR(10),                    -- URGENTE, ALTA, MEDIA, BAJA
    
    -- Estado
    estado VARCHAR(20) DEFAULT 'PENDIENTE',
    ejecutada BOOLEAN DEFAULT FALSE,
    fecha_ejecucion TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recomendaciones de compra
CREATE TABLE recomendaciones_compra (
    id BIGSERIAL PRIMARY KEY,
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    referencia VARCHAR(50) REFERENCES productos(referencia),
    marca_codigo VARCHAR(10) REFERENCES marcas(codigo),
    
    -- Clasificación para priorizar
    marca_categoria VARCHAR(20),
    marca_clasificacion CHAR(1),
    
    -- Cantidades
    stock_actual_red DECIMAL(15,5),
    venta_proyectada DECIMAL(15,5),           
    cantidad_sugerida DECIMAL(15,5),
    
    -- Cálculo
    dias_cobertura_actual DECIMAL(10,2),
    dias_cobertura_objetivo DECIMAL(10,2),    
    
    -- Valores
    costo_unitario_estimado DECIMAL(15,5),
    valor_compra_estimado DECIMAL(15,2),
    
    -- Fechas
    fecha_sugerida_pedido DATE,               
    fecha_llegada_estimada DATE,
    
    -- Prioridad y estado
    prioridad VARCHAR(10),
    estado VARCHAR(20) DEFAULT 'PENDIENTE',
    incluida_en_orden BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- VISTAS ÚTILES
-- ============================================

-- Vista: % Venta sobre Disponibilidad por Almacén (SOLO tipo Venta)
CREATE VIEW v_eficiencia_almacen AS
SELECT 
    a.codigo as bodega_codigo,
    a.nombre as bodega_nombre,
    a.tipo,
    a.regional,
    COALESCE(SUM(v.cantidad), 0) as unidades_vendidas_30d,
    COALESCE(SUM(v.valor_total), 0) as valor_vendido_30d,
    COALESCE(AVG(i.cantidad), 0) as stock_promedio,
    COALESCE(SUM(i.valor_costo * i.cantidad), 0) as valor_inventario,
    CASE 
        WHEN COALESCE(AVG(i.cantidad), 0) = 0 THEN 0
        ELSE ROUND((COALESCE(SUM(v.cantidad), 0) / NULLIF(AVG(i.cantidad), 0)) * 100, 2)
    END as pct_venta_sobre_disponibilidad,
    COUNT(DISTINCT v.referencia) as productos_vendidos,
    COUNT(DISTINCT i.referencia) as productos_en_stock
FROM almacenes a
LEFT JOIN ventas v ON v.bodega_codigo = a.codigo 
    AND v.fecha >= CURRENT_DATE - INTERVAL '30 days'
LEFT JOIN inventario_actual i ON i.bodega_codigo = a.codigo
WHERE a.tipo = 'Venta'  -- Solo almacenes de venta
GROUP BY a.codigo, a.nombre, a.tipo, a.regional;

-- Vista: Eficiencia por Regional
CREATE VIEW v_eficiencia_regional AS
SELECT 
    r.codigo as regional,
    r.nombre as regional_nombre,
    COUNT(DISTINCT a.codigo) as almacenes_venta,
    COALESCE(SUM(v.valor_total), 0) as valor_vendido_30d,
    COALESCE(SUM(i.valor_costo * i.cantidad), 0) as valor_inventario,
    CASE 
        WHEN COALESCE(SUM(i.cantidad), 0) = 0 THEN 0
        ELSE ROUND((COALESCE(SUM(v.cantidad), 0) / NULLIF(SUM(i.cantidad), 0)) * 100, 2)
    END as pct_venta_sobre_disponibilidad
FROM regionales r
LEFT JOIN almacenes a ON a.regional = r.codigo AND a.tipo = 'Venta'
LEFT JOIN ventas v ON v.bodega_codigo = a.codigo 
    AND v.fecha >= CURRENT_DATE - INTERVAL '30 days'
LEFT JOIN inventario_actual i ON i.bodega_codigo = a.codigo
GROUP BY r.codigo, r.nombre;

-- Vista: Inventario TOTAL por tipo de almacén
CREATE VIEW v_inventario_por_tipo_almacen AS
SELECT 
    ta.tipo,
    COUNT(DISTINCT a.codigo) as cantidad_almacenes,
    COUNT(DISTINCT i.referencia) as productos_diferentes,
    COALESCE(SUM(i.cantidad), 0) as unidades_totales,
    COALESCE(SUM(i.valor_costo * i.cantidad), 0) as valor_costo_total,
    COALESCE(SUM(i.valor_venta * i.cantidad), 0) as valor_venta_total
FROM tipos_almacen ta
LEFT JOIN almacenes a ON a.tipo = ta.tipo
LEFT JOIN inventario_actual i ON i.bodega_codigo = a.codigo
GROUP BY ta.tipo
ORDER BY valor_costo_total DESC;

-- Vista: Productos en estado crítico (priorizando por clasificación marca)
CREATE VIEW v_productos_criticos AS
SELECT 
    m.referencia,
    p.nombre as producto_nombre,
    ma.nombre as marca,
    ma.categoria as marca_categoria,
    ma.clasificacion as marca_clasificacion,
    m.bodega_codigo,
    a.nombre as almacen_nombre,
    a.regional,
    m.stock_actual,
    m.venta_diaria_promedio,
    m.dias_inventario,
    m.estado_stock,
    m.punto_reorden,
    -- Prioridad: A+PRINCIPAL > A+OTRAS > B+PRINCIPAL > etc.
    CASE 
        WHEN ma.categoria = 'PRINCIPAL' AND ma.clasificacion = 'A' THEN 1
        WHEN ma.categoria = 'PRINCIPAL' AND ma.clasificacion = 'B' THEN 2
        WHEN ma.categoria = 'OTRAS' AND ma.clasificacion = 'A' THEN 3
        WHEN ma.categoria = 'PRINCIPAL' AND ma.clasificacion = 'C' THEN 4
        WHEN ma.categoria = 'OTRAS' AND ma.clasificacion = 'B' THEN 5
        ELSE 6
    END as prioridad_atencion,
    CASE 
        WHEN m.dias_inventario < 3 THEN 'CRÍTICO - Menos de 3 días'
        WHEN m.dias_inventario < 7 THEN 'ALTO - Menos de 7 días'
        WHEN m.dias_inventario < 15 THEN 'MEDIO - Menos de 15 días'
    END as mensaje_alerta
FROM metricas_producto_almacen m
JOIN productos p ON p.referencia = m.referencia
JOIN marcas ma ON ma.codigo = p.marca_codigo
JOIN almacenes a ON a.codigo = m.bodega_codigo
WHERE m.fecha_calculo = CURRENT_DATE
    AND m.dias_inventario < 15
    AND m.venta_diaria_promedio > 0
    AND a.tipo = 'Venta'  -- Solo almacenes de venta
ORDER BY prioridad_atencion, m.dias_inventario ASC;

-- Vista: Sobreinventario
CREATE VIEW v_sobreinventario AS
SELECT 
    m.referencia,
    p.nombre as producto_nombre,
    ma.nombre as marca,
    ma.categoria,
    ma.clasificacion,
    m.bodega_codigo,
    a.nombre as almacen_nombre,
    a.tipo as tipo_almacen,
    a.regional,
    m.stock_actual,
    m.venta_diaria_promedio,
    m.dias_inventario,
    ma.dias_cobertura_stock as dias_objetivo,
    m.dias_inventario - ma.dias_cobertura_stock as dias_exceso,
    m.valor_stock as valor_exceso_estimado
FROM metricas_producto_almacen m
JOIN productos p ON p.referencia = m.referencia
JOIN marcas ma ON ma.codigo = p.marca_codigo
JOIN almacenes a ON a.codigo = m.bodega_codigo
WHERE m.fecha_calculo = CURRENT_DATE
    AND m.dias_inventario > ma.dias_cobertura_stock * 1.5
ORDER BY m.valor_stock DESC;

-- Vista: Productos baja rotación (con clasificación de marca)
CREATE VIEW v_baja_rotacion AS
SELECT 
    m.referencia,
    p.nombre as producto_nombre,
    ma.nombre as marca,
    ma.categoria,
    ma.clasificacion,
    m.bodega_codigo,
    a.nombre as almacen_nombre,
    a.tipo as tipo_almacen,
    m.stock_actual,
    m.venta_ultimos_90_dias,
    m.rotacion_mensual,
    m.valor_stock,
    -- Clasificación D ya es baja rotación esperada
    CASE 
        WHEN ma.clasificacion = 'D' THEN 'ESPERADO (Marca clase D)'
        WHEN m.venta_ultimos_90_dias = 0 THEN 'SIN MOVIMIENTO 90 DÍAS'
        WHEN m.rotacion_mensual < 0.5 THEN 'ROTACIÓN MUY BAJA'
        ELSE 'ROTACIÓN BAJA'
    END as clasificacion_rotacion
FROM metricas_producto_almacen m
JOIN productos p ON p.referencia = m.referencia
JOIN marcas ma ON ma.codigo = p.marca_codigo
JOIN almacenes a ON a.codigo = m.bodega_codigo
WHERE m.fecha_calculo = CURRENT_DATE
    AND m.stock_actual > 0
    AND (m.venta_ultimos_90_dias < 3 OR m.rotacion_mensual < 1)
ORDER BY 
    CASE WHEN ma.categoria = 'PRINCIPAL' THEN 1 ELSE 2 END,
    m.valor_stock DESC;

-- Vista: Dashboard resumen por marca
CREATE VIEW v_resumen_marca AS
SELECT 
    ma.nombre as marca,
    ma.categoria,
    ma.clasificacion,
    ma.periodicidad_compra_dias,
    ma.dias_cobertura_stock as cobertura_objetivo,
    ma.cobertura_total as cobertura_con_leadtime,
    COUNT(DISTINCT p.referencia) as total_referencias,
    COUNT(DISTINCT CASE WHEN i.cantidad > 0 THEN p.referencia END) as referencias_con_stock,
    COALESCE(SUM(i.cantidad), 0) as unidades_totales,
    COALESCE(SUM(i.valor_costo * i.cantidad), 0) as valor_inventario,
    COALESCE(SUM(v.cantidad), 0) as unidades_vendidas_30d,
    COALESCE(SUM(v.valor_total), 0) as valor_vendido_30d,
    CASE 
        WHEN COALESCE(SUM(i.cantidad), 0) = 0 THEN 0
        ELSE ROUND(COALESCE(SUM(v.cantidad), 0) / NULLIF(SUM(i.cantidad), 0) * 30, 1)
    END as dias_inventario_promedio
FROM marcas ma
LEFT JOIN productos p ON p.marca_codigo = ma.codigo
LEFT JOIN inventario_actual i ON i.referencia = p.referencia
LEFT JOIN (
    SELECT referencia, SUM(cantidad) as cantidad, SUM(valor_total) as valor_total
    FROM ventas 
    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY referencia
) v ON v.referencia = p.referencia
GROUP BY ma.codigo, ma.nombre, ma.categoria, ma.clasificacion, 
         ma.periodicidad_compra_dias, ma.dias_cobertura_stock, ma.cobertura_total
ORDER BY 
    CASE WHEN ma.categoria = 'PRINCIPAL' THEN 1 ELSE 2 END,
    ma.clasificacion;

-- ============================================
-- ÍNDICES PARA PERFORMANCE
-- ============================================

CREATE INDEX idx_ventas_fecha ON ventas(fecha);
CREATE INDEX idx_ventas_bodega ON ventas(bodega_codigo);
CREATE INDEX idx_ventas_referencia ON ventas(referencia);
CREATE INDEX idx_ventas_fecha_bodega ON ventas(fecha, bodega_codigo);
CREATE INDEX idx_ventas_fecha_referencia ON ventas(fecha, referencia);

CREATE INDEX idx_inventario_snapshot_fecha ON inventario_snapshot(fecha_snapshot);
CREATE INDEX idx_inventario_snapshot_bodega ON inventario_snapshot(bodega_codigo);
CREATE INDEX idx_inventario_snapshot_ref ON inventario_snapshot(referencia);

CREATE INDEX idx_metricas_fecha ON metricas_producto_almacen(fecha_calculo);
CREATE INDEX idx_metricas_estado ON metricas_producto_almacen(estado_stock);

CREATE INDEX idx_alertas_estado ON alertas(estado);
CREATE INDEX idx_alertas_tipo ON alertas(tipo_alerta);
CREATE INDEX idx_alertas_fecha ON alertas(fecha_generacion);

CREATE INDEX idx_almacenes_tipo ON almacenes(tipo);
CREATE INDEX idx_almacenes_regional ON almacenes(regional);
CREATE INDEX idx_marcas_categoria ON marcas(categoria);
CREATE INDEX idx_marcas_clasificacion ON marcas(clasificacion);

-- ============================================
-- DATOS INICIALES
-- ============================================

-- Tipos de Almacén
INSERT INTO tipos_almacen (tipo, descripcion, incluir_en_analisis_ventas, incluir_en_analisis_inventario) VALUES
('Venta', 'Puntos de venta activos', TRUE, TRUE),
('Reserva', 'Stock de reserva para pedidos', FALSE, TRUE),
('Laboratorio', 'Equipos para pruebas y demostraciones técnicas', FALSE, TRUE),
('Show Room', 'Exhibición y demostraciones comerciales', FALSE, TRUE),
('Transito', 'Mercancía en proceso de nacionalización o traslado', FALSE, TRUE),
('Demo', 'Equipos de demostración', FALSE, TRUE),
('Soporte', 'Equipos para soporte técnico y capacitación', FALSE, TRUE),
('Feria', 'Equipos para eventos y ferias', FALSE, TRUE),
('Descontinuado', 'Producto descontinuado o reacondicionado', FALSE, TRUE),
('Garantias', 'Equipos en proceso de garantía', FALSE, TRUE),
('Devolución Proveedores', 'Pendiente devolución a proveedores', FALSE, TRUE),
('Facturación', 'Transitoria de facturación', FALSE, FALSE),
('Auditoria', 'Auditoría de inventario', FALSE, FALSE);

-- Regionales
INSERT INTO regionales (codigo, nombre) VALUES
('Norte', 'Regional Norte'),
('Centro', 'Regional Centro'),
('Occidente', 'Regional Occidente'),
('Antioquia', 'Regional Antioquia'),
('Nacional', 'Nacional');

-- Almacenes (TODOS los de tu archivo)
INSERT INTO almacenes (codigo, nombre, tipo, regional, es_cedi) VALUES
-- Venta
('0001', 'PRINCIPAL', 'Venta', 'Norte', TRUE),
('0002', 'PEREIRA', 'Venta', 'Occidente', FALSE),
('0003', 'PARQUE CENTRAL', 'Venta', 'Norte', FALSE),
('0004', 'BUCARAMANGA', 'Venta', 'Norte', FALSE),
('0010', 'CALI', 'Venta', 'Occidente', FALSE),
('0011', 'BOGOTA CASTELLANA', 'Venta', 'Centro', FALSE),
('0012', 'MEDELLIN', 'Venta', 'Antioquia', FALSE),
('0016', 'VALLEDUPAR', 'Venta', 'Norte', FALSE),
('0017', 'BOGOTA CENTRO', 'Venta', 'Centro', FALSE),
('0018', 'CARTAGENA', 'Venta', 'Norte', FALSE),
('0019', 'CEDI BOGOTA', 'Venta', 'Centro', TRUE),
('0101', 'CM CENTRO', 'Venta', 'Centro', FALSE),
('0102', 'CM ANTIOQUIA', 'Venta', 'Antioquia', FALSE),
('0103', 'CM OCCIDENTE', 'Venta', 'Occidente', FALSE),
('0104', 'CM SANTANDER', 'Venta', 'Norte', FALSE),
('0105', 'CM VALLEDUPAR', 'Venta', 'Norte', FALSE),
('0106', 'CM PEREIRA', 'Venta', 'Occidente', FALSE),
-- Soporte
('0007', 'CAPACITACION', 'Soporte', 'Nacional', FALSE),
('0097', 'REPUESTOS SOPORTE', 'Soporte', 'Nacional', FALSE),
('0099', 'AVERIAS', 'Soporte', 'Nacional', FALSE),
-- Feria
('0008', 'FERIA', 'Feria', 'Nacional', FALSE),
-- Transito
('0009', 'MERCANCIA EN TRANSITO', 'Transito', 'Nacional', FALSE),
('0050', 'NACIONALIZACIÓN', 'Transito', 'Nacional', FALSE),
-- Demo
('0014', 'BODEGA DEMO', 'Demo', 'Nacional', FALSE),
-- Laboratorio
('0020', 'LABORATORIO CALI', 'Laboratorio', 'Occidente', FALSE),
('0021', 'LABORATORIO PRINCIPAL', 'Laboratorio', 'Norte', FALSE),
('0022', 'LABORATORIO MEDELLIN', 'Laboratorio', 'Antioquia', FALSE),
('0023', 'LABORATORIO AIRLIFE', 'Laboratorio', 'Nacional', FALSE),
('0024', 'LABORATORIO VALLEDUPAR', 'Laboratorio', 'Norte', FALSE),
('0025', 'LABORATORIO BOGOTA', 'Laboratorio', 'Centro', FALSE),
('0028', 'LABORATORIO PEREIRA', 'Laboratorio', 'Occidente', FALSE),
('0029', 'LABORATORIO BUCARAMANGA', 'Laboratorio', 'Norte', FALSE),
('0051', 'LABORATORIO BOGOTA CENTRO', 'Laboratorio', 'Centro', FALSE),
('0053', 'LABORATORIO CARTAGENA', 'Laboratorio', 'Norte', FALSE),
-- Descontinuado
('0026', 'REACONDICIONADO', 'Descontinuado', 'Nacional', FALSE),
('0027', 'REACONDICIONADO BUCARAMANGA', 'Descontinuado', 'Nacional', FALSE),
('0060', 'SUBASTA', 'Descontinuado', 'Nacional', FALSE),
-- Reserva
('0030', 'RESERVA CALI', 'Reserva', 'Occidente', FALSE),
('0031', 'RESERVA PRINCIPAL', 'Reserva', 'Norte', FALSE),
('0032', 'RESERVA MEDELLIN', 'Reserva', 'Antioquia', FALSE),
('0033', 'RESERVA PDV BOGOTA', 'Reserva', 'Centro', FALSE),
('0034', 'RESERVA BUCARAMANGA', 'Reserva', 'Norte', FALSE),
('0035', 'RESERVA BOGOTA', 'Reserva', 'Centro', FALSE),
('0070', 'RESERVA WEB', 'Reserva', 'Nacional', FALSE),
('0080', 'RESERVA PRINCIPAL', 'Reserva', 'Norte', FALSE),
('0081', 'RESERVA PEREIRA', 'Reserva', 'Occidente', FALSE),
('0082', 'RESERVA PARQUE CENTRAL', 'Reserva', 'Norte', FALSE),
('0083', 'RESERVA BUCARAMANGA', 'Reserva', 'Norte', FALSE),
('0084', 'RESERVA CALI', 'Reserva', 'Occidente', FALSE),
('0085', 'RESERVA BOGOTA CASTELLANA', 'Reserva', 'Centro', FALSE),
('0086', 'RESERVA MEDELLIN', 'Reserva', 'Antioquia', FALSE),
('0087', 'RESERVA VALLEDUPAR', 'Reserva', 'Norte', FALSE),
('0088', 'RESERVA BOGOTA CENTRO', 'Reserva', 'Centro', FALSE),
('0089', 'RESERVA CARTAGENA', 'Reserva', 'Norte', FALSE),
('0107', 'RESERVA CEDI BOGOTA', 'Reserva', 'Centro', FALSE),
-- Show Room
('0039', 'SHOWROOM PDV CARTAGENA', 'Show Room', 'Norte', FALSE),
('0040', 'SHOWROOM CALI', 'Show Room', 'Occidente', FALSE),
('0041', 'SHOWROOM PDV BOGOTA', 'Show Room', 'Centro', FALSE),
('0042', 'SHOWROOM MEDELLIN', 'Show Room', 'Antioquia', FALSE),
('0043', 'SHOWROOM PARQUE CENTRAL', 'Show Room', 'Norte', FALSE),
('0044', 'SHOWROOM BUCARAMANGA', 'Show Room', 'Norte', FALSE),
('0045', 'SHOWROOM BOGOTA', 'Show Room', 'Centro', FALSE),
('0046', 'SHOWROOM PEREIRA', 'Show Room', 'Occidente', FALSE),
('0047', 'SHOWROOM PRINCIPAL', 'Show Room', 'Norte', FALSE),
('0048', 'SHOWROOM VALLEDUPAR', 'Show Room', 'Norte', FALSE),
('0049', 'SHOWROOM BOGOTA CENTRO', 'Show Room', 'Centro', FALSE),
-- Otros
('0091', 'DEVOLUCION FACTURA - TRANSITORIA', 'Facturación', 'Nacional', FALSE),
('0092', 'AUDITORIA INVENTARIO', 'Auditoria', 'Nacional', FALSE),
('0098', 'GARANTÍAS', 'Garantias', 'Nacional', FALSE),
('0100', 'DEVOLUCION A PROVEEDORES', 'Devolución Proveedores', 'Nacional', FALSE);

-- Clasificación de Marcas (descripciones)
INSERT INTO clasificacion_marcas (categoria, clasificacion, descripcion_categoria, descripcion_clasificacion, prioridad_compra) VALUES
('PRINCIPAL', 'A', 'Marcas core del negocio - mayor volumen de compra', 'Alta rotación - productos estrella', 1),
('PRINCIPAL', 'B', 'Marcas core del negocio - mayor volumen de compra', 'Rotación media - productos estables', 2),
('PRINCIPAL', 'C', 'Marcas core del negocio - mayor volumen de compra', 'Rotación baja - revisar estrategia', 3),
('OTRAS', 'A', 'Marcas complementarias', 'Alta rotación - oportunidad de crecimiento', 4),
('OTRAS', 'B', 'Marcas complementarias', 'Rotación media', 5),
('OTRAS', 'C', 'Marcas complementarias', 'Rotación baja', 6),
('OTRAS', 'D', 'Marcas complementarias', 'Muy baja rotación - evaluar descontinuar', 7);

-- Marcas PRINCIPALES con parámetros de compra
INSERT INTO marcas (codigo, nombre, nombre_fomplus, categoria, clasificacion, periodicidad_compra_dias, dias_cobertura_stock, lead_time_proveedor, lead_time_a_cedi, tipo_proveedor, origen) VALUES
('DSC', 'DSC', 'DSC', 'PRINCIPAL', 'A', 15, 30, 15, 45, 'Alarmas, controladores, paneles', 'USA'),
('HIKVI', 'HIKVISION', 'HIKVISION', 'PRINCIPAL', 'A', 7, 15, 7, 22, 'CCTV - CAM - NVR - DVR - Control de acceso', 'NACIONAL'),
('HILOOK', 'HILOOK', 'HILOOK', 'PRINCIPAL', 'A', 7, 15, 7, 22, 'CCTV - CAM - NVR - DVR - Accesorios', 'NACIONAL'),
('TPLINK', 'TP-LINK', 'TP-LINK', 'PRINCIPAL', 'A', 15, 15, 7, 22, 'CCTV - CAM - NVR - DVR - Antenas - Radios', 'NACIONAL'),
('UHF', 'UHF BY ZKTECO', 'UHF BY ZKTECO', 'PRINCIPAL', 'A', 30, 60, 45, 105, 'Accesorios marca propia - Alarmas, control', 'CHINA'),
('WESTER', 'WESTERN DIGITAL', 'WESTERN DIGITAL', 'PRINCIPAL', 'A', 15, 30, 15, 45, 'Discos duros y Almacenamiento', 'USA'),
('ZKTECO', 'ZKTECO', 'ZKTECO', 'PRINCIPAL', 'A', 30, 60, 45, 105, 'Control de acceso', 'CHINA'),
('SAMSUN', 'SAMSUNG', 'SAMSUNG', 'PRINCIPAL', 'A', 15, 15, 7, 22, 'Pantallas - TV', 'NACIONAL'),
('CAME', 'CAME', 'CAME', 'PRINCIPAL', 'B', 15, 30, 15, 45, 'Control de acceso', 'USA'),
('EZVIZ', 'EZVIZ', 'EZVIZ', 'PRINCIPAL', 'B', 15, 30, 7, 37, 'CCTV - CAM - NVR - DVR - Citofonía - Smart', 'NACIONAL'),
('FORZA', 'FORZA', 'FORZA', 'PRINCIPAL', 'B', 30, 30, 15, 45, 'Ups protectores de voltaje', 'PANAMA'),
('GENER', 'GENERICO', 'GENERICO', 'PRINCIPAL', 'C', 15, 30, 7, 37, 'Otros', 'NACIONAL'),
('NEXXT', 'NEXXT', 'NEXXT', 'PRINCIPAL', 'C', 30, 30, 15, 45, 'Cableado - Rotters - Smart Home', 'PANAMA'),
('DSCIQ', 'DSC IQ', 'DSC IQ', 'PRINCIPAL', 'C', 15, 30, 15, 45, 'Alarmas, controladores, paneles', 'USA'),
('SANDIS', 'SANDISK', 'SANDISK', 'PRINCIPAL', 'C', 15, 30, 7, 37, 'Discos duros y Almacenamiento', 'NACIONAL'),
('SEAGAT', 'SEAGATE', 'SEAGATE', 'PRINCIPAL', 'C', 15, 30, 7, 37, 'Discos duros y Almacenamiento', 'NACIONAL'),
('VISONI', 'VISONIC', 'VISONIC', 'PRINCIPAL', 'C', 15, 30, 15, 45, 'Alarmas, controladores, paneles', 'USA');

-- Marcas OTRAS (complementarias) - todas clasificación D excepto ARMATURA
INSERT INTO marcas (codigo, nombre, nombre_fomplus, categoria, clasificacion) VALUES
('ARMATU', 'ARMATURA', 'ARMATURA', 'OTRAS', 'C'),
('CYA', 'CYA', 'CYA', 'OTRAS', 'D'),
('EPCOM', 'EPCOM', 'EPCOM', 'OTRAS', 'D'),
('GRANDS', 'GRANDSTREAM', 'GRANDSTREAM', 'OTRAS', 'D'),
('HONEY', 'HONEYWELL', 'HONEYWELL', 'OTRAS', 'D'),
('HORUS', 'HORUS', 'HORUS', 'OTRAS', 'D'),
('HYTERA', 'HYTERA', 'HYTERA', 'OTRAS', 'D'),
('MERCUS', 'MERCUSYS', 'MERCUSYS', 'OTRAS', 'D'),
('POWEST', 'POWEST', 'POWEST', 'OTRAS', 'D'),
('SECOLA', 'SECOLARM', 'SECOLARM', 'OTRAS', 'D'),
('SINMAR', 'SIN MARCA', 'SIN MARCA', 'OTRAS', 'D'),
('SUNTRE', 'SUNTREE', 'SUNTREE', 'OTRAS', 'D'),
('ULTRA', 'ULTRA', 'ULTRA', 'OTRAS', 'D'),
('DKS', 'DKS DOORKING', 'DKS DOORKING', 'OTRAS', 'D'),
('EDWARD', 'EDWARDS', 'EDWARDS', 'OTRAS', 'D'),
('CAMBIU', 'CAMBIUM NETWORKS', 'CAMBIUM NETWORKS', 'OTRAS', 'D'),
('GROWAT', 'GROWATT', 'GROWATT', 'OTRAS', 'D'),
('SIMPLE', 'SIMPLEX', 'SIMPLEX', 'OTRAS', 'D'),
('MIRAGE', 'MIRAGE', 'MIRAGE', 'OTRAS', 'D'),
('ACCESS', 'ACCESSPRO', 'ACCESSPRO', 'OTRAS', 'D'),
('ROSSLA', 'ROSSLARE', 'ROSSLARE', 'OTRAS', 'D'),
('COMMAX', 'COMMAX', 'COMMAX', 'OTRAS', 'D'),
('SYSTEM', 'SYSTEM SENSORS', 'SYSTEM SENSORS', 'OTRAS', 'D'),
('UBIQUI', 'UBIQUITI', 'UBIQUITI', 'OTRAS', 'D'),
('FANVIL', 'FANVIL', 'FANVIL', 'OTRAS', 'D'),
('CENTEL', 'CENTELSA', 'CENTELSA', 'OTRAS', 'D'),
('LINKED', 'LINKEDPRO', 'LINKEDPRO', 'OTRAS', 'D'),
('GOOMAX', 'GOOMAX', 'GOOMAX', 'OTRAS', 'D'),
('FIRELI', 'FIRELITE', 'FIRELITE', 'OTRAS', 'D'),
('TOSHIB', 'TOSHIBA', 'TOSHIBA', 'OTRAS', 'D'),
('HOCHIK', 'HOCHIKI', 'HOCHIKI', 'OTRAS', 'D'),
('SCHNEI', 'SCHNEIDER ELECTRIC', 'SCHNEIDER ELECTRIC', 'OTRAS', 'D'),
('GENIUS', 'GENIUS', 'GENIUS', 'OTRAS', 'D'),
('JASOLA', 'JASOLAR', 'JASOLAR', 'OTRAS', 'D'),
('AOC', 'AOC', 'AOC', 'OTRAS', 'D'),
('EXTRIU', 'EXTRIUM', 'EXTRIUM', 'OTRAS', 'D'),
('KIDDE', 'KIDDE', 'KIDDE', 'OTRAS', 'D'),
('MIKROT', 'MIKROTIK', 'MIKROTIK', 'OTRAS', 'D'),
('RANGER', 'RANGER', 'RANGER', 'OTRAS', 'D'),
('KINGST', 'KINGSTON', 'KINGSTON', 'OTRAS', 'D'),
('SAT', 'SAT', 'SAT', 'OTRAS', 'D'),
('VACRON', 'VACRON', 'VACRON', 'OTRAS', 'D'),
('PELCO', 'PELCO', 'PELCO', 'OTRAS', 'D'),
('SFIRE', 'S-FIRE', 'S-FIRE', 'OTRAS', 'D'),
('PLANET', 'PLANET', 'PLANET', 'OTRAS', 'D'),
('QUEST', 'QUEST', 'QUEST', 'OTRAS', 'D'),
('XTECH', 'XTECH', 'XTECH', 'OTRAS', 'D'),
('NOEXIS', 'Marca No Existe', 'Marca No Existe', 'OTRAS', 'D');

-- ============================================
-- COMENTARIOS DE DOCUMENTACIÓN
-- ============================================

COMMENT ON TABLE tipos_almacen IS 'Tipos de almacén: Venta (para análisis ventas), otros (solo inventario)';
COMMENT ON TABLE regionales IS 'Regionales: Norte, Centro, Occidente, Antioquia, Nacional';
COMMENT ON TABLE clasificacion_marcas IS 'Matriz de priorización: Categoría (PRINCIPAL/OTRAS) × Clasificación (A/B/C/D)';
COMMENT ON TABLE marcas IS 'Marcas con clasificación y parámetros de compra (periodicidad, cobertura, lead times)';
COMMENT ON VIEW v_eficiencia_almacen IS 'KPI: % venta/disponibilidad - SOLO almacenes tipo Venta';
COMMENT ON VIEW v_inventario_por_tipo_almacen IS 'Inventario total desglosado por tipo de almacén (todos los tipos)';
