"""
API REST para Dashboard StockIQ - Versión Lambda
Usa pg8000 (Python puro) en lugar de psycopg2
"""

import json
import os
import pg8000
import boto3


def get_db_credentials():
    """Obtiene credenciales de Secrets Manager"""
    if os.environ.get('LOCAL_DEV'):
        return {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT', '5432'),
            'database': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD')
        }
    
    secrets_client = boto3.client('secretsmanager')
    secret_arn = os.environ['DB_SECRET_ARN']
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    
    return {
        'host': secret['host'],
        'port': secret['port'],
        'database': secret['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }


def get_db():
    creds = get_db_credentials()
    conn = pg8000.connect(
        host=creds['host'],
        port=int(creds['port']),
        database=creds['database'],
        user=creds['user'],
        password=creds['password']
    )
    return conn


def query_to_dict(cursor, rows):
    """Convierte resultados a lista de diccionarios"""
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in rows]


def response(status_code, body):
    """Genera respuesta HTTP para API Gateway"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }


def get_kpis():
    """KPIs principales del dashboard"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(DISTINCT referencia),
            COUNT(DISTINCT bodega_codigo),
            COALESCE(SUM(cantidad), 0),
            COALESCE(SUM(cantidad * valor_costo), 0)
        FROM inventario_actual
    """)
    row = cur.fetchone()
    inventario = {
        'total_productos': row[0],
        'total_almacenes': row[1],
        'total_unidades': float(row[2]) if row[2] else 0,
        'valor_inventario': float(row[3]) if row[3] else 0
    }
    
    cur.execute("""
        SELECT 
            COUNT(*),
            COALESCE(SUM(cantidad), 0),
            COALESCE(SUM(valor_total), 0)
        FROM ventas
        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
    """)
    row = cur.fetchone()
    ventas = {
        'transacciones': row[0],
        'unidades': float(row[1]) if row[1] else 0,
        'valor': float(row[2]) if row[2] else 0
    }
    
    cur.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_CRITICO'),
            COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_BAJO'),
            COUNT(*) FILTER (WHERE tipo_alerta = 'SOBREINVENTARIO'),
            COUNT(*)
        FROM alertas
        WHERE estado = 'PENDIENTE'
    """)
    row = cur.fetchone()
    alertas = {
        'criticas': row[0],
        'bajas': row[1],
        'sobreinventario': row[2],
        'total': row[3]
    }
    
    cur.execute("SELECT COUNT(*) FROM recomendaciones_traslado WHERE estado = 'PENDIENTE'")
    row = cur.fetchone()
    traslados_pendientes = row[0]
    
    conn.close()
    
    return {
        'inventario': inventario,
        'ventas_30d': ventas,
        'alertas': alertas,
        'traslados_pendientes': traslados_pendientes
    }


def get_alertas(limit=50, tipo=None, nivel=None):
    """Lista de alertas con filtros"""
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT 
            a.id, a.tipo_alerta, a.nivel, a.referencia,
            p.nombre, a.bodega_codigo, al.nombre,
            a.stock_actual, a.dias_inventario, a.venta_diaria,
            a.mensaje, a.fecha_generacion
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
    
    query += " ORDER BY a.dias_inventario ASC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()
    
    alertas = []
    for row in rows:
        alertas.append({
            'id': row[0],
            'tipo_alerta': row[1],
            'nivel': row[2],
            'referencia': row[3],
            'producto_nombre': row[4],
            'bodega_codigo': row[5],
            'almacen_nombre': row[6],
            'stock_actual': float(row[7]) if row[7] else 0,
            'dias_inventario': float(row[8]) if row[8] else 0,
            'venta_diaria': float(row[9]) if row[9] else 0,
            'mensaje': row[10],
            'fecha_generacion': row[11].isoformat() if row[11] else None
        })
    
    return alertas


def get_traslados(limit=50, prioridad=None):
    """Recomendaciones de traslado"""
    conn = get_db()
    cur = conn.cursor()
    
    query = """
        SELECT 
            rt.id, rt.referencia, p.nombre,
            rt.bodega_origen, ao.nombre,
            rt.bodega_destino, ad.nombre,
            rt.cantidad_sugerida, rt.dias_inv_origen, rt.dias_inv_destino,
            rt.prioridad, rt.estado
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
    
    query += " ORDER BY rt.dias_inv_destino ASC LIMIT %s"
    params.append(limit)
    
    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()
    
    traslados = []
    for row in rows:
        traslados.append({
            'id': row[0],
            'referencia': row[1],
            'producto_nombre': row[2],
            'bodega_origen': row[3],
            'origen_nombre': row[4],
            'bodega_destino': row[5],
            'destino_nombre': row[6],
            'cantidad_sugerida': float(row[7]) if row[7] else 0,
            'dias_inv_origen': float(row[8]) if row[8] else 0,
            'dias_inv_destino': float(row[9]) if row[9] else 0,
            'prioridad': row[10],
            'estado': row[11]
        })
    
    return traslados


def get_almacenes():
    """Lista de almacenes con métricas"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            a.codigo, a.nombre, a.tipo, a.regional, a.es_cedi,
            COUNT(DISTINCT m.referencia),
            COALESCE(SUM(m.stock_actual), 0),
            COALESCE(SUM(m.valor_stock), 0),
            AVG(m.dias_inventario) FILTER (WHERE m.dias_inventario < 9999)
        FROM almacenes a
        LEFT JOIN metricas_producto_almacen m ON m.bodega_codigo = a.codigo 
            AND m.fecha_calculo = CURRENT_DATE
        WHERE a.tipo = 'Venta'
        GROUP BY a.codigo, a.nombre, a.tipo, a.regional, a.es_cedi
        ORDER BY 8 DESC NULLS LAST
    """)
    
    rows = cur.fetchall()
    conn.close()
    
    almacenes = []
    for row in rows:
        almacenes.append({
            'codigo': row[0],
            'nombre': row[1],
            'tipo': row[2],
            'regional': row[3],
            'es_cedi': row[4],
            'productos': row[5],
            'unidades': float(row[6]) if row[6] else 0,
            'valor_inventario': float(row[7]) if row[7] else 0,
            'dias_inv_promedio': float(row[8]) if row[8] else 0
        })
    
    return almacenes


def get_ventas_diarias():
    """Ventas por día (últimos 30 días)"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT fecha, COUNT(*), COALESCE(SUM(cantidad), 0), COALESCE(SUM(valor_total), 0)
        FROM ventas
        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY fecha
        ORDER BY fecha
    """)
    
    rows = cur.fetchall()
    conn.close()
    
    ventas = []
    for row in rows:
        ventas.append({
            'fecha': row[0].isoformat(),
            'transacciones': row[1],
            'unidades': float(row[2]) if row[2] else 0,
            'valor': float(row[3]) if row[3] else 0
        })
    
    return ventas


def get_top_productos(limit=20):
    """Top productos más vendidos"""
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            v.referencia, p.nombre,
            COALESCE(SUM(v.cantidad), 0),
            COALESCE(SUM(v.valor_total), 0),
            COUNT(DISTINCT v.bodega_codigo)
        FROM ventas v
        LEFT JOIN productos p ON p.referencia = v.referencia
        WHERE v.fecha >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY v.referencia, p.nombre
        ORDER BY 3 DESC
        LIMIT %s
    """, (limit,))
    
    rows = cur.fetchall()
    conn.close()
    
    productos = []
    for row in rows:
        productos.append({
            'referencia': row[0],
            'producto_nombre': row[1],
            'unidades_vendidas': float(row[2]) if row[2] else 0,
            'valor_vendido': float(row[3]) if row[3] else 0,
            'almacenes_venta': row[4]
        })
    
    return productos


def handler(event, context):
    """Handler principal para API Gateway"""
    
    if event.get('httpMethod') == 'OPTIONS':
        return response(200, {})
    
    path = event.get('path', '')
    query_params = event.get('queryStringParameters') or {}
    
    try:
        if path == '/api/kpis':
            return response(200, get_kpis())
        
        elif path == '/api/alertas':
            limit = int(query_params.get('limit', 50))
            tipo = query_params.get('tipo')
            nivel = query_params.get('nivel')
            return response(200, get_alertas(limit, tipo, nivel))
        
        elif path == '/api/traslados':
            limit = int(query_params.get('limit', 50))
            prioridad = query_params.get('prioridad')
            return response(200, get_traslados(limit, prioridad))
        
        elif path == '/api/almacenes':
            return response(200, get_almacenes())
        
        elif path == '/api/ventas/diarias':
            return response(200, get_ventas_diarias())
        
        elif path == '/api/ventas/top-productos':
            limit = int(query_params.get('limit', 20))
            return response(200, get_top_productos(limit))
        
        else:
            return response(404, {'error': 'Endpoint no encontrado'})
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return response(500, {'error': str(e)})
