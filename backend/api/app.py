"""
API REST para Dashboard StockIQ
Endpoints para consultar métricas, alertas y recomendaciones
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# ============================================
# CONFIGURACIÓN
# ============================================

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'inventory-platform-db.cmal9qmniwdx.us-east-1.rds.amazonaws.com'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'inventory_db'),
    'user': os.environ.get('DB_USER', 'inventory_admin'),
    'password': os.environ.get('DB_PASSWORD', 'Gsp2026*')
}


def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


# ============================================
# ENDPOINTS - KPIs
# ============================================

@app.route('/api/kpis', methods=['GET'])
def get_kpis():
    """KPIs principales del dashboard"""
    conn = get_db()
    cur = conn.cursor()
    
    # Valor total inventario
    cur.execute("""
        SELECT 
            COUNT(DISTINCT referencia) as total_productos,
            COUNT(DISTINCT bodega_codigo) as total_almacenes,
            SUM(cantidad) as total_unidades,
            SUM(cantidad * valor_costo) as valor_inventario
        FROM inventario_actual
    """)
    inventario = cur.fetchone()
    
    # Ventas últimos 30 días
    cur.execute("""
        SELECT 
            COUNT(*) as total_transacciones,
            SUM(cantidad) as unidades_vendidas,
            SUM(valor_total) as valor_vendido
        FROM ventas
        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
    """)
    ventas = cur.fetchone()
    
    # Alertas activas
    cur.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_CRITICO') as criticas,
            COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_BAJO') as bajas,
            COUNT(*) FILTER (WHERE tipo_alerta = 'SOBREINVENTARIO') as sobreinventario,
            COUNT(*) as total
        FROM alertas
        WHERE estado = 'PENDIENTE'
    """)
    alertas = cur.fetchone()
    
    # Recomendaciones pendientes
    cur.execute("""
        SELECT COUNT(*) as traslados_pendientes
        FROM recomendaciones_traslado
        WHERE estado = 'PENDIENTE'
    """)
    traslados = cur.fetchone()
    
    conn.close()
    
    return jsonify({
        'inventario': {
            'total_productos': inventario['total_productos'],
            'total_almacenes': inventario['total_almacenes'],
            'total_unidades': float(inventario['total_unidades'] or 0),
            'valor_inventario': float(inventario['valor_inventario'] or 0)
        },
        'ventas_30d': {
            'transacciones': ventas['total_transacciones'],
            'unidades': float(ventas['unidades_vendidas'] or 0),
            'valor': float(ventas['valor_vendido'] or 0)
        },
        'alertas': {
            'criticas': alertas['criticas'],
            'bajas': alertas['bajas'],
            'sobreinventario': alertas['sobreinventario'],
            'total': alertas['total']
        },
        'traslados_pendientes': traslados['traslados_pendientes']
    })


# ============================================
# ENDPOINTS - ALERTAS
# ============================================

@app.route('/api/alertas', methods=['GET'])
def get_alertas():
    """Lista de alertas con filtros"""
    tipo = request.args.get('tipo', None)
    nivel = request.args.get('nivel', None)
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT 
            a.id,
            a.tipo_alerta,
            a.nivel,
            a.referencia,
            p.nombre as producto_nombre,
            a.bodega_codigo,
            al.nombre as almacen_nombre,
            a.stock_actual,
            a.dias_inventario,
            a.venta_diaria,
            a.mensaje,
            a.fecha_generacion
        FROM alertas a
        LEFT JOIN productos p ON p.referencia = a.referencia
        LEFT JOIN almacenes al ON al.codigo = a.bodega_codigo
        WHERE a.estado = 'PENDIENTE'
    """
    params = []
    
    if tipo:
        query += " AND a.tipo_alerta = %s"
        params.append(tipo)
    if nivel:
        query += " AND a.nivel = %s"
        params.append(nivel)
    
    query += " ORDER BY a.nivel DESC, a.dias_inventario ASC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    alertas = cur.fetchall()
    conn.close()
    
    # Convertir a formato serializable
    for a in alertas:
        a['stock_actual'] = float(a['stock_actual']) if a['stock_actual'] else 0
        a['dias_inventario'] = float(a['dias_inventario']) if a['dias_inventario'] else 0
        a['venta_diaria'] = float(a['venta_diaria']) if a['venta_diaria'] else 0
        a['fecha_generacion'] = a['fecha_generacion'].isoformat() if a['fecha_generacion'] else None
    
    return jsonify(alertas)


@app.route('/api/alertas/resumen', methods=['GET'])
def get_alertas_resumen():
    """Resumen de alertas por tipo y nivel"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            tipo_alerta,
            nivel,
            COUNT(*) as cantidad
        FROM alertas
        WHERE estado = 'PENDIENTE'
        GROUP BY tipo_alerta, nivel
        ORDER BY tipo_alerta, nivel
    """)
    
    resumen = cur.fetchall()
    conn.close()
    
    return jsonify(resumen)


# ============================================
# ENDPOINTS - RECOMENDACIONES
# ============================================

@app.route('/api/traslados', methods=['GET'])
def get_traslados():
    """Recomendaciones de traslado"""
    prioridad = request.args.get('prioridad', None)
    limit = request.args.get('limit', 50, type=int)
    
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT 
            rt.id,
            rt.referencia,
            p.nombre as producto_nombre,
            rt.bodega_origen,
            ao.nombre as origen_nombre,
            rt.bodega_destino,
            ad.nombre as destino_nombre,
            rt.cantidad_sugerida,
            rt.dias_inv_origen,
            rt.dias_inv_destino,
            rt.prioridad,
            rt.estado
        FROM recomendaciones_traslado rt
        LEFT JOIN productos p ON p.referencia = rt.referencia
        LEFT JOIN almacenes ao ON ao.codigo = rt.bodega_origen
        LEFT JOIN almacenes ad ON ad.codigo = rt.bodega_destino
        WHERE rt.estado = 'PENDIENTE'
    """
    params = []
    
    if prioridad:
        query += " AND rt.prioridad = %s"
        params.append(prioridad)
    
    query += " ORDER BY rt.prioridad DESC, rt.dias_inv_destino ASC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, params)
    traslados = cur.fetchall()
    conn.close()
    
    for t in traslados:
        t['cantidad_sugerida'] = float(t['cantidad_sugerida']) if t['cantidad_sugerida'] else 0
        t['dias_inv_origen'] = float(t['dias_inv_origen']) if t['dias_inv_origen'] else 0
        t['dias_inv_destino'] = float(t['dias_inv_destino']) if t['dias_inv_destino'] else 0
    
    return jsonify(traslados)


# ============================================
# ENDPOINTS - INVENTARIO
# ============================================

@app.route('/api/inventario/almacen/<codigo>', methods=['GET'])
def get_inventario_almacen(codigo):
    """Inventario de un almacén específico"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            m.referencia,
            p.nombre as producto_nombre,
            m.stock_actual,
            m.valor_stock,
            m.venta_ultimos_30_dias,
            m.venta_diaria_promedio,
            m.dias_inventario,
            m.rotacion_mensual,
            m.estado_stock
        FROM metricas_producto_almacen m
        LEFT JOIN productos p ON p.referencia = m.referencia
        WHERE m.bodega_codigo = %s
          AND m.fecha_calculo = CURRENT_DATE
        ORDER BY m.valor_stock DESC
        LIMIT 100
    """, (codigo,))
    
    inventario = cur.fetchall()
    conn.close()
    
    for i in inventario:
        i['stock_actual'] = float(i['stock_actual']) if i['stock_actual'] else 0
        i['valor_stock'] = float(i['valor_stock']) if i['valor_stock'] else 0
        i['venta_ultimos_30_dias'] = float(i['venta_ultimos_30_dias']) if i['venta_ultimos_30_dias'] else 0
        i['venta_diaria_promedio'] = float(i['venta_diaria_promedio']) if i['venta_diaria_promedio'] else 0
        i['dias_inventario'] = float(i['dias_inventario']) if i['dias_inventario'] else 0
        i['rotacion_mensual'] = float(i['rotacion_mensual']) if i['rotacion_mensual'] else 0
    
    return jsonify(inventario)


@app.route('/api/almacenes', methods=['GET'])
def get_almacenes():
    """Lista de almacenes con métricas"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            a.codigo,
            a.nombre,
            a.tipo,
            a.regional,
            a.es_cedi,
            COUNT(DISTINCT m.referencia) as productos,
            SUM(m.stock_actual) as unidades,
            SUM(m.valor_stock) as valor_inventario,
            AVG(m.dias_inventario) FILTER (WHERE m.dias_inventario < 9999) as dias_inv_promedio
        FROM almacenes a
        LEFT JOIN metricas_producto_almacen m ON m.bodega_codigo = a.codigo 
            AND m.fecha_calculo = CURRENT_DATE
        WHERE a.tipo = 'Venta'
        GROUP BY a.codigo, a.nombre, a.tipo, a.regional, a.es_cedi
        ORDER BY valor_inventario DESC NULLS LAST
    """)
    
    almacenes = cur.fetchall()
    conn.close()
    
    for a in almacenes:
        a['unidades'] = float(a['unidades']) if a['unidades'] else 0
        a['valor_inventario'] = float(a['valor_inventario']) if a['valor_inventario'] else 0
        a['dias_inv_promedio'] = float(a['dias_inv_promedio']) if a['dias_inv_promedio'] else 0
    
    return jsonify(almacenes)


# ============================================
# ENDPOINTS - VENTAS
# ============================================

@app.route('/api/ventas/diarias', methods=['GET'])
def get_ventas_diarias():
    """Ventas por día (últimos 30 días)"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            fecha,
            COUNT(*) as transacciones,
            SUM(cantidad) as unidades,
            SUM(valor_total) as valor
        FROM ventas
        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY fecha
        ORDER BY fecha
    """)
    
    ventas = cur.fetchall()
    conn.close()
    
    for v in ventas:
        v['fecha'] = v['fecha'].isoformat()
        v['unidades'] = float(v['unidades']) if v['unidades'] else 0
        v['valor'] = float(v['valor']) if v['valor'] else 0
    
    return jsonify(ventas)


@app.route('/api/ventas/top-productos', methods=['GET'])
def get_top_productos():
    """Top productos más vendidos"""
    limit = request.args.get('limit', 20, type=int)
    
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            v.referencia,
            p.nombre as producto_nombre,
            SUM(v.cantidad) as unidades_vendidas,
            SUM(v.valor_total) as valor_vendido,
            COUNT(DISTINCT v.bodega_codigo) as almacenes_venta
        FROM ventas v
        LEFT JOIN productos p ON p.referencia = v.referencia
        WHERE v.fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY v.referencia, p.nombre
        ORDER BY unidades_vendidas DESC
        LIMIT %s
    """, (limit,))
    
    productos = cur.fetchall()
    conn.close()
    
    for p in productos:
        p['unidades_vendidas'] = float(p['unidades_vendidas']) if p['unidades_vendidas'] else 0
        p['valor_vendido'] = float(p['valor_vendido']) if p['valor_vendido'] else 0
    
    return jsonify(productos)


# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    print("=" * 50)
    print("StockIQ API - Dashboard Backend")
    print("=" * 50)
    print(f"Conectando a: {DB_CONFIG['host']}")
    print("Endpoints disponibles:")
    print("  GET /api/kpis")
    print("  GET /api/alertas")
    print("  GET /api/alertas/resumen")
    print("  GET /api/traslados")
    print("  GET /api/almacenes")
    print("  GET /api/inventario/almacen/<codigo>")
    print("  GET /api/ventas/diarias")
    print("  GET /api/ventas/top-productos")
    print("=" * 50)
    
    app.run(host='0.0.0.0', port=5001, debug=True)
