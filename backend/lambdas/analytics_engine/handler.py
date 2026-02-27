"""
Motor de Análisis de Inventario - StockIQ
Calcula métricas, genera alertas y recomendaciones
"""

import os
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Tuple

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ============================================
# CONFIGURACIÓN
# ============================================

# Umbrales para alertas (en días de inventario)
UMBRAL_CRITICO = 3      # Menos de 3 días = CRÍTICO
UMBRAL_BAJO = 7         # Menos de 7 días = BAJO
UMBRAL_MEDIO = 15       # Menos de 15 días = MEDIO
UMBRAL_SOBRE = 1.5      # 50% sobre cobertura objetivo = SOBREINVENTARIO

# Umbrales para baja rotación
UMBRAL_BAJA_ROTACION_90D = 3      # Menos de 3 unidades en 90 días
UMBRAL_ROTACION_MENSUAL = 0.5    # Rotación < 0.5 mensual


# ============================================
# CONEXIÓN A BASE DE DATOS
# ============================================

def get_db_connection():
    """Obtiene conexión a PostgreSQL"""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT', 5432),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD')
    )


# ============================================
# CÁLCULO DE MÉTRICAS POR PRODUCTO/ALMACÉN
# ============================================

def calcular_metricas_producto_almacen(conn, fecha_calculo: str = None):
    """
    Calcula métricas para cada producto en cada almacén
    """
    if not fecha_calculo:
        fecha_calculo = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Calculando métricas por producto/almacén para {fecha_calculo}")
    
    query = """
    WITH ventas_periodo AS (
        -- Ventas agrupadas por producto y bodega
        SELECT 
            v.referencia,
            v.bodega_codigo,
            SUM(CASE WHEN v.fecha >= CURRENT_DATE - INTERVAL '7 days' THEN v.cantidad ELSE 0 END) as venta_7d,
            SUM(CASE WHEN v.fecha >= CURRENT_DATE - INTERVAL '30 days' THEN v.cantidad ELSE 0 END) as venta_30d,
            SUM(CASE WHEN v.fecha >= CURRENT_DATE - INTERVAL '90 days' THEN v.cantidad ELSE 0 END) as venta_90d
        FROM ventas v
        JOIN almacenes a ON a.codigo = v.bodega_codigo AND a.tipo = 'Venta'
        WHERE v.fecha >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY v.referencia, v.bodega_codigo
    ),
    inventario_con_venta AS (
        -- Cruzar inventario actual con ventas
        SELECT 
            i.bodega_codigo,
            i.referencia,
            i.cantidad as stock_actual,
            i.valor_costo * i.cantidad as valor_stock,
            COALESCE(vp.venta_7d, 0) as venta_7d,
            COALESCE(vp.venta_30d, 0) as venta_30d,
            COALESCE(vp.venta_90d, 0) as venta_90d,
            COALESCE(vp.venta_30d / 30.0, 0) as venta_diaria_promedio,
            -- Parámetros de marca
            COALESCE(m.dias_cobertura_stock, 30) as dias_cobertura_objetivo,
            COALESCE(m.lead_time_a_cedi, 15) as lead_time,
            m.categoria as marca_categoria,
            m.clasificacion as marca_clasificacion
        FROM inventario_actual i
        LEFT JOIN ventas_periodo vp ON vp.referencia = i.referencia AND vp.bodega_codigo = i.bodega_codigo
        LEFT JOIN productos p ON p.referencia = i.referencia
        LEFT JOIN marcas m ON m.codigo = p.marca_codigo
        WHERE i.cantidad > 0
    )
    SELECT 
        bodega_codigo,
        referencia,
        stock_actual,
        valor_stock,
        venta_7d,
        venta_30d,
        venta_90d,
        venta_diaria_promedio,
        -- Días de inventario
        CASE 
            WHEN venta_diaria_promedio > 0 THEN ROUND(stock_actual / venta_diaria_promedio, 2)
            ELSE 9999  -- Sin ventas = inventario infinito
        END as dias_inventario,
        -- Rotación mensual
        CASE 
            WHEN stock_actual > 0 THEN ROUND(venta_30d / stock_actual, 4)
            ELSE 0
        END as rotacion_mensual,
        -- Punto de reorden
        ROUND(venta_diaria_promedio * lead_time * 1.2, 2) as punto_reorden,  -- 20% seguridad
        -- Stock de seguridad
        ROUND(venta_diaria_promedio * 7, 2) as stock_seguridad,  -- 7 días
        -- Stock máximo
        ROUND(venta_diaria_promedio * dias_cobertura_objetivo, 2) as stock_maximo,
        dias_cobertura_objetivo,
        lead_time,
        marca_categoria,
        marca_clasificacion
    FROM inventario_con_venta
    ORDER BY bodega_codigo, referencia
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        resultados = cur.fetchall()
    
    logger.info(f"Métricas calculadas para {len(resultados)} combinaciones producto/almacén")
    
    # Insertar en tabla de métricas
    insert_query = """
    INSERT INTO metricas_producto_almacen (
        fecha_calculo, referencia, bodega_codigo,
        stock_actual, valor_stock,
        venta_ultimos_7_dias, venta_ultimos_30_dias, venta_ultimos_90_dias,
        venta_diaria_promedio, dias_inventario, rotacion_mensual,
        punto_reorden, stock_seguridad, stock_maximo,
        estado_stock, requiere_traslado, requiere_compra
    ) VALUES %s
    ON CONFLICT (fecha_calculo, referencia, bodega_codigo) DO UPDATE SET
        stock_actual = EXCLUDED.stock_actual,
        valor_stock = EXCLUDED.valor_stock,
        venta_ultimos_7_dias = EXCLUDED.venta_ultimos_7_dias,
        venta_ultimos_30_dias = EXCLUDED.venta_ultimos_30_dias,
        venta_ultimos_90_dias = EXCLUDED.venta_ultimos_90_dias,
        venta_diaria_promedio = EXCLUDED.venta_diaria_promedio,
        dias_inventario = EXCLUDED.dias_inventario,
        rotacion_mensual = EXCLUDED.rotacion_mensual,
        punto_reorden = EXCLUDED.punto_reorden,
        stock_seguridad = EXCLUDED.stock_seguridad,
        stock_maximo = EXCLUDED.stock_maximo,
        estado_stock = EXCLUDED.estado_stock,
        requiere_traslado = EXCLUDED.requiere_traslado,
        requiere_compra = EXCLUDED.requiere_compra
    """
    
    values = []
    for r in resultados:
        dias_inv = float(r['dias_inventario']) if r['dias_inventario'] else 9999
        dias_objetivo = float(r['dias_cobertura_objetivo']) if r['dias_cobertura_objetivo'] else 30
        venta_90d = float(r['venta_90d']) if r['venta_90d'] else 0
        rotacion = float(r['rotacion_mensual']) if r['rotacion_mensual'] else 0
        
        # Determinar estado
        if dias_inv < UMBRAL_CRITICO:
            estado = 'CRITICO'
        elif dias_inv < UMBRAL_BAJO:
            estado = 'BAJO'
        elif dias_inv < UMBRAL_MEDIO:
            estado = 'MEDIO'
        elif dias_inv > dias_objetivo * UMBRAL_SOBRE:
            estado = 'EXCESO'
        elif dias_inv > dias_objetivo:
            estado = 'SOBRE'
        else:
            estado = 'NORMAL'
        
        # Determinar si requiere acción
        requiere_traslado = estado in ('CRITICO', 'BAJO', 'EXCESO')
        requiere_compra = estado in ('CRITICO', 'BAJO')
        
        values.append((
            fecha_calculo,
            r['referencia'],
            r['bodega_codigo'],
            r['stock_actual'],
            r['valor_stock'],
            r['venta_7d'],
            r['venta_30d'],
            r['venta_90d'],
            r['venta_diaria_promedio'],
            r['dias_inventario'],
            r['rotacion_mensual'],
            r['punto_reorden'],
            r['stock_seguridad'],
            r['stock_maximo'],
            estado,
            requiere_traslado,
            requiere_compra
        ))
    
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)
    conn.commit()
    
    return len(values)


# ============================================
# GENERACIÓN DE ALERTAS
# ============================================

def generar_alertas(conn, fecha_calculo: str = None):
    """
    Genera alertas basadas en las métricas calculadas
    """
    if not fecha_calculo:
        fecha_calculo = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Generando alertas para {fecha_calculo}")
    
    # Limpiar alertas anteriores del mismo día
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM alertas 
            WHERE DATE(fecha_generacion) = %s AND estado = 'PENDIENTE'
        """, (fecha_calculo,))
    
    # Generar alertas de stock bajo/crítico
    query_stock_bajo = """
    INSERT INTO alertas (tipo_alerta, nivel, referencia, bodega_codigo, 
                         stock_actual, dias_inventario, venta_diaria, 
                         marca_categoria, marca_clasificacion, mensaje)
    SELECT 
        CASE 
            WHEN m.dias_inventario < %s THEN 'STOCK_CRITICO'
            WHEN m.dias_inventario < %s THEN 'STOCK_BAJO'
            ELSE 'STOCK_MEDIO'
        END as tipo_alerta,
        CASE 
            WHEN m.dias_inventario < %s THEN 'CRITICO'
            WHEN m.dias_inventario < %s THEN 'ALTO'
            ELSE 'MEDIO'
        END as nivel,
        m.referencia,
        m.bodega_codigo,
        m.stock_actual,
        m.dias_inventario,
        m.venta_diaria_promedio,
        ma.categoria,
        ma.clasificacion,
        'Stock para ' || ROUND(m.dias_inventario, 1) || ' días. Venta diaria: ' || 
        ROUND(m.venta_diaria_promedio, 2) || ' uds. Se recomienda reabastecer.'
    FROM metricas_producto_almacen m
    JOIN productos p ON p.referencia = m.referencia
    LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
    JOIN almacenes a ON a.codigo = m.bodega_codigo AND a.tipo = 'Venta'
    WHERE m.fecha_calculo = %s
      AND m.dias_inventario < %s
      AND m.venta_diaria_promedio > 0
    """
    
    with conn.cursor() as cur:
        cur.execute(query_stock_bajo, (
            UMBRAL_CRITICO, UMBRAL_BAJO,  # tipo_alerta
            UMBRAL_CRITICO, UMBRAL_BAJO,  # nivel
            fecha_calculo, UMBRAL_MEDIO   # WHERE
        ))
        alertas_stock_bajo = cur.rowcount
    
    # Generar alertas de sobreinventario
    query_sobreinventario = """
    INSERT INTO alertas (tipo_alerta, nivel, referencia, bodega_codigo,
                         stock_actual, dias_inventario, venta_diaria,
                         marca_categoria, marca_clasificacion, mensaje)
    SELECT 
        'SOBREINVENTARIO' as tipo_alerta,
        CASE 
            WHEN m.dias_inventario > m.stock_maximo * 2 THEN 'ALTO'
            ELSE 'MEDIO'
        END as nivel,
        m.referencia,
        m.bodega_codigo,
        m.stock_actual,
        m.dias_inventario,
        m.venta_diaria_promedio,
        ma.categoria,
        ma.clasificacion,
        'Stock para ' || ROUND(m.dias_inventario, 0) || ' días (objetivo: ' || 
        COALESCE(ma.dias_cobertura_stock, 30) || '). Considerar traslado a otro almacén.'
    FROM metricas_producto_almacen m
    JOIN productos p ON p.referencia = m.referencia
    LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
    WHERE m.fecha_calculo = %s
      AND m.dias_inventario > COALESCE(ma.dias_cobertura_stock, 30) * %s
      AND m.venta_diaria_promedio > 0
    """
    
    with conn.cursor() as cur:
        cur.execute(query_sobreinventario, (fecha_calculo, UMBRAL_SOBRE))
        alertas_sobreinventario = cur.rowcount
    
    # Generar alertas de baja rotación
    query_baja_rotacion = """
    INSERT INTO alertas (tipo_alerta, nivel, referencia, bodega_codigo,
                         stock_actual, dias_inventario, venta_diaria,
                         marca_categoria, marca_clasificacion, mensaje)
    SELECT 
        'BAJA_ROTACION' as tipo_alerta,
        CASE 
            WHEN m.venta_ultimos_90_dias = 0 THEN 'ALTO'
            ELSE 'MEDIO'
        END as nivel,
        m.referencia,
        m.bodega_codigo,
        m.stock_actual,
        m.dias_inventario,
        m.venta_diaria_promedio,
        ma.categoria,
        ma.clasificacion,
        CASE 
            WHEN m.venta_ultimos_90_dias = 0 THEN 'Sin movimiento en 90 días. Valor en stock: $' || ROUND(m.valor_stock, 0)
            ELSE 'Solo ' || m.venta_ultimos_90_dias || ' uds vendidas en 90 días. Evaluar promoción o descontinuar.'
        END
    FROM metricas_producto_almacen m
    JOIN productos p ON p.referencia = m.referencia
    LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
    WHERE m.fecha_calculo = %s
      AND m.stock_actual > 0
      AND (m.venta_ultimos_90_dias < %s OR m.rotacion_mensual < %s)
      AND ma.clasificacion != 'D'  -- Excluir marcas clase D (baja rotación esperada)
    """
    
    with conn.cursor() as cur:
        cur.execute(query_baja_rotacion, (
            fecha_calculo, 
            UMBRAL_BAJA_ROTACION_90D, 
            UMBRAL_ROTACION_MENSUAL
        ))
        alertas_baja_rotacion = cur.rowcount
    
    conn.commit()
    
    total_alertas = alertas_stock_bajo + alertas_sobreinventario + alertas_baja_rotacion
    logger.info(f"Alertas generadas: {alertas_stock_bajo} stock bajo, {alertas_sobreinventario} sobreinventario, {alertas_baja_rotacion} baja rotación")
    
    return {
        'stock_bajo': alertas_stock_bajo,
        'sobreinventario': alertas_sobreinventario,
        'baja_rotacion': alertas_baja_rotacion,
        'total': total_alertas
    }


# ============================================
# RECOMENDACIONES DE TRASLADO
# ============================================

def generar_recomendaciones_traslado(conn, fecha_calculo: str = None):
    """
    Genera recomendaciones de traslado entre almacenes
    Busca productos con exceso en un almacén y falta en otro
    """
    if not fecha_calculo:
        fecha_calculo = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Generando recomendaciones de traslado para {fecha_calculo}")
    
    # Limpiar recomendaciones anteriores pendientes
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM recomendaciones_traslado 
            WHERE DATE(fecha_generacion) = %s AND estado = 'PENDIENTE'
        """, (fecha_calculo,))
    
    query = """
    WITH necesidades AS (
        -- Almacenes que necesitan producto (stock bajo)
        SELECT 
            m.referencia,
            m.bodega_codigo,
            a.regional,
            m.stock_actual,
            m.dias_inventario,
            m.venta_diaria_promedio,
            m.punto_reorden,
            m.stock_maximo,
            -- Cantidad que necesita para llegar al punto de reorden
            GREATEST(m.punto_reorden - m.stock_actual, 0) as cantidad_necesaria,
            ma.categoria,
            ma.clasificacion
        FROM metricas_producto_almacen m
        JOIN almacenes a ON a.codigo = m.bodega_codigo AND a.tipo = 'Venta'
        JOIN productos p ON p.referencia = m.referencia
        LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
        WHERE m.fecha_calculo = %s
          AND m.dias_inventario < %s
          AND m.venta_diaria_promedio > 0
    ),
    excesos AS (
        -- Almacenes con exceso de producto
        SELECT 
            m.referencia,
            m.bodega_codigo,
            a.regional,
            a.es_cedi,
            m.stock_actual,
            m.dias_inventario,
            m.venta_diaria_promedio,
            m.stock_maximo,
            -- Cantidad disponible para trasladar
            GREATEST(m.stock_actual - m.stock_maximo, 0) as cantidad_disponible
        FROM metricas_producto_almacen m
        JOIN almacenes a ON a.codigo = m.bodega_codigo
        JOIN productos p ON p.referencia = m.referencia
        LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
        WHERE m.fecha_calculo = %s
          AND m.stock_actual > m.stock_maximo
          AND m.stock_actual > 0
    )
    SELECT 
        n.referencia,
        e.bodega_codigo as bodega_origen,
        n.bodega_codigo as bodega_destino,
        n.regional as regional_destino,
        -- Cantidad a trasladar: el mínimo entre lo disponible y lo necesario
        LEAST(e.cantidad_disponible, n.cantidad_necesaria) as cantidad_sugerida,
        e.dias_inventario as dias_inv_origen,
        n.dias_inventario as dias_inv_destino,
        n.categoria as marca_categoria,
        n.clasificacion as marca_clasificacion,
        -- Prioridad basada en urgencia y clasificación de marca
        CASE 
            WHEN n.dias_inventario < %s AND n.categoria = 'PRINCIPAL' AND n.clasificacion = 'A' THEN 'URGENTE'
            WHEN n.dias_inventario < %s AND n.categoria = 'PRINCIPAL' THEN 'ALTA'
            WHEN n.dias_inventario < %s THEN 'MEDIA'
            ELSE 'BAJA'
        END as prioridad
    FROM necesidades n
    JOIN excesos e ON e.referencia = n.referencia AND e.bodega_codigo != n.bodega_codigo
    WHERE LEAST(e.cantidad_disponible, n.cantidad_necesaria) > 0
    ORDER BY 
        CASE WHEN n.categoria = 'PRINCIPAL' AND n.clasificacion = 'A' THEN 1
             WHEN n.categoria = 'PRINCIPAL' THEN 2
             ELSE 3 END,
        n.dias_inventario ASC
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (
            fecha_calculo, UMBRAL_MEDIO,  # necesidades
            fecha_calculo,                 # excesos
            UMBRAL_CRITICO, UMBRAL_BAJO, UMBRAL_MEDIO  # prioridad
        ))
        recomendaciones = cur.fetchall()
    
    # Insertar recomendaciones
    insert_query = """
    INSERT INTO recomendaciones_traslado (
        referencia, bodega_origen, bodega_destino, regional_destino,
        cantidad_sugerida, dias_inv_origen, dias_inv_destino,
        marca_categoria, marca_clasificacion, prioridad
    ) VALUES %s
    """
    
    values = [(
        r['referencia'],
        r['bodega_origen'],
        r['bodega_destino'],
        r['regional_destino'],
        r['cantidad_sugerida'],
        r['dias_inv_origen'],
        r['dias_inv_destino'],
        r['marca_categoria'],
        r['marca_clasificacion'],
        r['prioridad']
    ) for r in recomendaciones]
    
    if values:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()
    
    logger.info(f"Recomendaciones de traslado generadas: {len(values)}")
    return len(values)


# ============================================
# RECOMENDACIONES DE COMPRA
# ============================================

def generar_recomendaciones_compra(conn, fecha_calculo: str = None):
    """
    Genera recomendaciones de compra basadas en cobertura objetivo
    """
    if not fecha_calculo:
        fecha_calculo = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Generando recomendaciones de compra para {fecha_calculo}")
    
    # Limpiar recomendaciones anteriores pendientes
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM recomendaciones_compra 
            WHERE DATE(fecha_generacion) = %s AND estado = 'PENDIENTE'
        """, (fecha_calculo,))
    
    query = """
    WITH stock_red AS (
        -- Stock total por producto en toda la red
        SELECT 
            i.referencia,
            SUM(i.cantidad) as stock_total_red,
            AVG(i.valor_costo) as costo_promedio
        FROM inventario_actual i
        GROUP BY i.referencia
    ),
    venta_red AS (
        -- Venta total en red últimos 30 días
        SELECT 
            v.referencia,
            SUM(v.cantidad) as venta_30d,
            SUM(v.cantidad) / 30.0 as venta_diaria_red
        FROM ventas v
        JOIN almacenes a ON a.codigo = v.bodega_codigo AND a.tipo = 'Venta'
        WHERE v.fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY v.referencia
    ),
    analisis AS (
        SELECT 
            p.referencia,
            p.nombre,
            p.marca_codigo,
            ma.nombre as marca_nombre,
            ma.categoria,
            ma.clasificacion,
            ma.dias_cobertura_stock,
            ma.lead_time_a_cedi,
            ma.periodicidad_compra_dias,
            COALESCE(sr.stock_total_red, 0) as stock_actual_red,
            COALESCE(sr.costo_promedio, 0) as costo_unitario,
            COALESCE(vr.venta_30d, 0) as venta_30d,
            COALESCE(vr.venta_diaria_red, 0) as venta_diaria_red,
            -- Días de cobertura actual
            CASE 
                WHEN COALESCE(vr.venta_diaria_red, 0) > 0 
                THEN ROUND(COALESCE(sr.stock_total_red, 0) / vr.venta_diaria_red, 2)
                ELSE 9999
            END as dias_cobertura_actual
        FROM productos p
        LEFT JOIN marcas ma ON ma.codigo = p.marca_codigo
        LEFT JOIN stock_red sr ON sr.referencia = p.referencia
        LEFT JOIN venta_red vr ON vr.referencia = p.referencia
        WHERE COALESCE(vr.venta_diaria_red, 0) > 0  -- Solo productos con venta
    )
    SELECT 
        referencia,
        nombre,
        marca_codigo,
        marca_nombre,
        categoria,
        clasificacion,
        dias_cobertura_stock as dias_cobertura_objetivo,
        lead_time_a_cedi,
        stock_actual_red,
        venta_diaria_red,
        dias_cobertura_actual,
        costo_unitario,
        -- Cantidad a comprar para llegar a cobertura objetivo + lead time
        GREATEST(
            ROUND((dias_cobertura_stock + lead_time_a_cedi) * venta_diaria_red - stock_actual_red, 0),
            0
        ) as cantidad_sugerida,
        -- Valor estimado de compra
        GREATEST(
            ROUND((dias_cobertura_stock + lead_time_a_cedi) * venta_diaria_red - stock_actual_red, 0),
            0
        ) * costo_unitario as valor_compra_estimado,
        -- Fecha sugerida para pedir (considerando lead time)
        CURRENT_DATE + INTERVAL '1 day' * GREATEST(
            dias_cobertura_actual - lead_time_a_cedi - 7, 0  -- 7 días de margen
        ) as fecha_sugerida_pedido,
        -- Prioridad
        CASE 
            WHEN dias_cobertura_actual < lead_time_a_cedi THEN 'URGENTE'
            WHEN dias_cobertura_actual < lead_time_a_cedi + 7 THEN 'ALTA'
            WHEN dias_cobertura_actual < dias_cobertura_stock THEN 'MEDIA'
            ELSE 'BAJA'
        END as prioridad
    FROM analisis
    WHERE dias_cobertura_actual < dias_cobertura_stock + lead_time_a_cedi
      AND categoria = 'PRINCIPAL'  -- Solo marcas principales
    ORDER BY 
        CASE WHEN clasificacion = 'A' THEN 1
             WHEN clasificacion = 'B' THEN 2
             ELSE 3 END,
        dias_cobertura_actual ASC
    """
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        recomendaciones = cur.fetchall()
    
    # Insertar recomendaciones
    insert_query = """
    INSERT INTO recomendaciones_compra (
        referencia, marca_codigo, marca_categoria, marca_clasificacion,
        stock_actual_red, venta_proyectada, cantidad_sugerida,
        dias_cobertura_actual, dias_cobertura_objetivo,
        costo_unitario_estimado, valor_compra_estimado,
        fecha_sugerida_pedido, prioridad
    ) VALUES %s
    """
    
    values = [(
        r['referencia'],
        r['marca_codigo'],
        r['categoria'],
        r['clasificacion'],
        r['stock_actual_red'],
        r['venta_diaria_red'] * 30,  # Proyección mensual
        r['cantidad_sugerida'],
        r['dias_cobertura_actual'],
        r['dias_cobertura_objetivo'],
        r['costo_unitario'],
        r['valor_compra_estimado'],
        r['fecha_sugerida_pedido'],
        r['prioridad']
    ) for r in recomendaciones if r['cantidad_sugerida'] > 0]
    
    if values:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()
    
    logger.info(f"Recomendaciones de compra generadas: {len(values)}")
    return len(values)


# ============================================
# HANDLER PRINCIPAL
# ============================================

def handler(event, context):
    """
    Handler principal - ejecuta todo el análisis
    """
    fecha_calculo = event.get('fecha', datetime.now().strftime('%Y-%m-%d'))
    
    logger.info(f"Iniciando análisis para {fecha_calculo}")
    
    try:
        conn = get_db_connection()
        
        # 1. Calcular métricas
        n_metricas = calcular_metricas_producto_almacen(conn, fecha_calculo)
        
        # 2. Generar alertas
        alertas = generar_alertas(conn, fecha_calculo)
        
        # 3. Recomendaciones de traslado
        n_traslados = generar_recomendaciones_traslado(conn, fecha_calculo)
        
        # 4. Recomendaciones de compra
        n_compras = generar_recomendaciones_compra(conn, fecha_calculo)
        
        conn.close()
        
        resultado = {
            'fecha': fecha_calculo,
            'metricas_calculadas': n_metricas,
            'alertas': alertas,
            'recomendaciones_traslado': n_traslados,
            'recomendaciones_compra': n_compras
        }
        
        logger.info(f"Análisis completado: {resultado}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(resultado, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error en análisis: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


# ============================================
# PRUEBA LOCAL
# ============================================

if __name__ == "__main__":
    # Configurar variables de entorno para prueba local
    os.environ['DB_HOST'] = 'inventory-platform-db.cmal9qmniwdx.us-east-1.rds.amazonaws.com'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'inventory_db'
    os.environ['DB_USER'] = 'inventory_admin'
    os.environ['DB_PASSWORD'] = 'Gsp2026*'

    print("=" * 60)
    print("MOTOR DE ANÁLISIS - PRUEBA LOCAL")
    print("=" * 60)

    resultado = handler({}, None)
    print("\nResultado:")
    print(json.dumps(json.loads(resultado['body']), indent=2))