"""
API REST para Dashboard StockIQ - Versión Lambda
Incluye: Auth, Alertas, Traslados, Compras, Búsqueda, Regionales, Exportación CSV
"""

import json
import os
import hashlib
import uuid
from datetime import date, datetime, timedelta
import pg8000
import boto3


# ============================================
# DATABASE
# ============================================

def get_db_credentials():
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
    resp = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(resp['SecretString'])
    return {
        'host': secret['host'],
        'port': secret['port'],
        'database': secret['dbname'],
        'user': secret['username'],
        'password': secret['password']
    }


def get_db():
    creds = get_db_credentials()
    return pg8000.connect(
        host=creds['host'],
        port=int(creds['port']),
        database=creds['database'],
        user=creds['user'],
        password=creds['password']
    )


# ============================================
# HELPERS
# ============================================

def api_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PATCH,OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }


def csv_response(filename, csv_content):
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/csv; charset=utf-8',
            'Content-Disposition': f'attachment; filename="{filename}"',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PATCH,OPTIONS'
        },
        'body': csv_content
    }


def safe_float(val):
    return float(val) if val is not None else 0


def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def generate_token():
    return uuid.uuid4().hex + uuid.uuid4().hex


# ============================================
# AUTH
# ============================================

def authenticate(event):
    headers = event.get('headers', {}) or {}
    auth = headers.get('Authorization') or headers.get('authorization', '')
    if not auth.startswith('Bearer '):
        return None

    token = auth[7:]
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT u.id, u.username, u.nombre, u.rol
        FROM sesiones s JOIN usuarios u ON u.id = s.usuario_id
        WHERE s.token = %s AND s.expires_at > NOW() AND u.activo = true
    """, (token,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {'id': row[0], 'username': row[1], 'nombre': row[2], 'rol': row[3]}


def login(body):
    data = json.loads(body) if isinstance(body, str) else (body or {})
    username = data.get('username', '')
    password = data.get('password', '')

    if not username or not password:
        return api_response(400, {'error': 'Usuario y contraseña requeridos'})

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, username, nombre, rol, password_hash FROM usuarios WHERE username = %s AND activo = true",
        (username,)
    )
    row = cur.fetchone()

    if not row or row[4] != hash_password(password):
        conn.close()
        return api_response(401, {'error': 'Credenciales inválidas'})

    token = generate_token()
    expires = datetime.now() + timedelta(hours=24)
    cur.execute("INSERT INTO sesiones (token, usuario_id, expires_at) VALUES (%s, %s, %s)",
                (token, row[0], expires))
    cur.execute("DELETE FROM sesiones WHERE expires_at < NOW()")
    conn.commit()
    conn.close()

    return api_response(200, {
        'token': token,
        'user': {'id': row[0], 'username': row[1], 'nombre': row[2], 'rol': row[3]}
    })


def logout(event):
    headers = event.get('headers', {}) or {}
    auth = headers.get('Authorization') or headers.get('authorization', '')
    if auth.startswith('Bearer '):
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM sesiones WHERE token = %s", (auth[7:],))
        conn.commit()
        conn.close()
    return api_response(200, {'message': 'Sesión cerrada'})


# ============================================
# FILTROS (listas para dropdowns)
# ============================================

def get_filtros():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT codigo, nombre FROM regionales WHERE activo = true ORDER BY nombre")
    regionales = [{'codigo': r[0], 'nombre': r[1]} for r in cur.fetchall()]

    cur.execute("SELECT codigo, nombre, regional FROM almacenes WHERE activo = true AND tipo = 'Venta' ORDER BY nombre")
    almacenes = [{'codigo': r[0], 'nombre': r[1], 'regional': r[2]} for r in cur.fetchall()]

    cur.execute("SELECT codigo, nombre, categoria, clasificacion FROM marcas WHERE activo = true ORDER BY categoria, clasificacion, nombre")
    marcas = [{'codigo': r[0], 'nombre': r[1], 'categoria': r[2], 'clasificacion': r[3]} for r in cur.fetchall()]

    conn.close()
    return {
        'regionales': regionales,
        'almacenes': almacenes,
        'marcas': marcas,
        'tipos_alerta': ['STOCK_CRITICO', 'STOCK_BAJO', 'SOBREINVENTARIO', 'BAJA_ROTACION'],
        'niveles_alerta': ['CRITICO', 'ALTO', 'MEDIO', 'BAJO'],
        'prioridades': ['URGENTE', 'ALTA', 'MEDIA', 'BAJA']
    }


# ============================================
# KPIs
# ============================================

def get_kpis(params):
    regional = params.get('regional')
    marca = params.get('marca')
    conn = get_db()
    cur = conn.cursor()

    # Inventario solo almacenes tipo Venta
    inv_q = """
        SELECT COUNT(DISTINCT ia.referencia), COUNT(DISTINCT ia.bodega_codigo),
               COALESCE(SUM(ia.cantidad), 0), COALESCE(SUM(ia.cantidad * ia.valor_costo), 0)
        FROM inventario_actual ia
        JOIN almacenes a ON a.codigo = ia.bodega_codigo AND a.tipo = 'Venta'
    """
    inv_p = []
    conditions = []
    if regional:
        conditions.append("a.regional = %s"); inv_p.append(regional)
    if marca:
        inv_q += " JOIN productos p ON p.referencia = ia.referencia"
        conditions.append("p.marca_codigo = %s"); inv_p.append(marca)
    if conditions:
        inv_q += " WHERE " + " AND ".join(conditions)
    cur.execute(inv_q, tuple(inv_p))
    r = cur.fetchone()
    inventario = {
        'total_productos': r[0], 'total_almacenes': r[1],
        'total_unidades': safe_float(r[2]), 'valor_inventario': safe_float(r[3])
    }

    d30 = date.today() - timedelta(days=30)
    if regional:
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(v.cantidad), 0), COALESCE(SUM(v.valor_total), 0)
            FROM ventas v JOIN almacenes a ON a.codigo = v.bodega_codigo
            WHERE v.fecha >= %s AND a.regional = %s
        """, (d30, regional))
    else:
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(cantidad), 0), COALESCE(SUM(valor_total), 0)
            FROM ventas WHERE fecha >= %s
        """, (d30,))
    r = cur.fetchone()
    ventas = {'transacciones': r[0], 'unidades': safe_float(r[1]), 'valor': safe_float(r[2])}

    if regional:
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE al.tipo_alerta = 'STOCK_CRITICO'),
                   COUNT(*) FILTER (WHERE al.tipo_alerta = 'STOCK_BAJO'),
                   COUNT(*) FILTER (WHERE al.tipo_alerta = 'SOBREINVENTARIO'),
                   COUNT(*)
            FROM alertas al JOIN almacenes a ON a.codigo = al.bodega_codigo
            WHERE al.estado = 'PENDIENTE' AND a.regional = %s
        """, (regional,))
    else:
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_CRITICO'),
                   COUNT(*) FILTER (WHERE tipo_alerta = 'STOCK_BAJO'),
                   COUNT(*) FILTER (WHERE tipo_alerta = 'SOBREINVENTARIO'),
                   COUNT(*)
            FROM alertas WHERE estado = 'PENDIENTE'
        """)
    r = cur.fetchone()
    alertas = {'criticas': r[0], 'bajas': r[1], 'sobreinventario': r[2], 'total': r[3]}

    if regional:
        cur.execute("SELECT COUNT(*) FROM recomendaciones_traslado WHERE estado = 'PENDIENTE' AND regional_destino = %s", (regional,))
    else:
        cur.execute("SELECT COUNT(*) FROM recomendaciones_traslado WHERE estado = 'PENDIENTE'")
    traslados_pendientes = cur.fetchone()[0]

    # Compras pendientes
    cur.execute("SELECT COUNT(*), COALESCE(SUM(valor_compra_estimado), 0) FROM recomendaciones_compra WHERE estado = 'PENDIENTE'")
    r = cur.fetchone()
    compras = {'total': r[0], 'valor_estimado': safe_float(r[1])}

    conn.close()
    return {
        'inventario': inventario, 'ventas_30d': ventas, 'alertas': alertas,
        'traslados_pendientes': traslados_pendientes, 'compras': compras
    }


# ============================================
# ALERTAS
# ============================================

def get_alertas(params):
    limit = int(params.get('limit', 50))
    tipo = params.get('tipo')
    nivel = params.get('nivel')
    regional = params.get('regional')
    almacen = params.get('almacen')
    estado = params.get('estado', 'PENDIENTE')

    conn = get_db()
    cur = conn.cursor()
    query = """
        SELECT a.id, a.tipo_alerta, a.nivel, a.referencia, p.nombre,
               a.bodega_codigo, al.nombre, a.stock_actual, a.dias_inventario,
               a.venta_diaria, a.mensaje, a.fecha_generacion, a.estado,
               al.regional, a.marca_categoria, a.marca_clasificacion,
               a.atendida_por, a.fecha_atencion
        FROM alertas a
        LEFT JOIN productos p ON p.referencia = a.referencia
        LEFT JOIN almacenes al ON al.codigo = a.bodega_codigo
        WHERE 1=1
    """
    p = []
    if estado:
        query += " AND a.estado = %s"; p.append(estado)
    if tipo:
        query += " AND a.tipo_alerta = %s"; p.append(tipo)
    if nivel:
        query += " AND a.nivel = %s"; p.append(nivel)
    if regional:
        query += " AND al.regional = %s"; p.append(regional)
    if almacen:
        query += " AND a.bodega_codigo = %s"; p.append(almacen)
    query += " ORDER BY a.fecha_generacion DESC LIMIT %s"
    p.append(limit)

    cur.execute(query, tuple(p))
    rows = cur.fetchall()
    conn.close()

    return [{
        'id': r[0], 'tipo_alerta': r[1], 'nivel': r[2], 'referencia': r[3],
        'producto_nombre': r[4], 'bodega_codigo': r[5], 'almacen_nombre': r[6],
        'stock_actual': safe_float(r[7]), 'dias_inventario': safe_float(r[8]),
        'venta_diaria': safe_float(r[9]), 'mensaje': r[10],
        'fecha_generacion': r[11].isoformat() if r[11] else None,
        'estado': r[12], 'regional': r[13],
        'marca_categoria': r[14], 'marca_clasificacion': r[15],
        'atendida_por': r[16],
        'fecha_atencion': r[17].isoformat() if r[17] else None
    } for r in rows]


def update_alerta_estado(alerta_id, body, user):
    data = json.loads(body) if isinstance(body, str) else (body or {})
    nuevo_estado = data.get('estado')
    if nuevo_estado not in ('VISTA', 'ATENDIDA', 'IGNORADA'):
        return api_response(400, {'error': 'Estado inválido. Usar: VISTA, ATENDIDA, IGNORADA'})

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE alertas SET estado = %s, atendida_por = %s, fecha_atencion = NOW()
        WHERE id = %s
    """, (nuevo_estado, user.get('nombre', user['username']), int(alerta_id)))
    conn.commit()
    conn.close()
    return api_response(200, {'message': 'Alerta actualizada', 'id': alerta_id, 'estado': nuevo_estado})


def get_alertas_resumen():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT tipo_alerta, nivel, COUNT(*) FROM alertas
        WHERE estado = 'PENDIENTE' GROUP BY tipo_alerta, nivel ORDER BY tipo_alerta, nivel
    """)
    rows = cur.fetchall()
    conn.close()
    return [{'tipo': r[0], 'nivel': r[1], 'cantidad': r[2]} for r in rows]


# ============================================
# TRASLADOS
# ============================================

def get_traslados(params):
    limit = int(params.get('limit', 50))
    prioridad = params.get('prioridad')
    regional = params.get('regional')
    estado = params.get('estado', 'PENDIENTE')

    conn = get_db()
    cur = conn.cursor()
    query = """
        SELECT rt.id, rt.referencia, p.nombre, rt.bodega_origen, ao.nombre,
               rt.bodega_destino, ad.nombre, rt.cantidad_sugerida,
               rt.dias_inv_origen, rt.dias_inv_destino, rt.prioridad, rt.estado,
               rt.regional_destino, rt.ejecutada, rt.fecha_ejecucion
        FROM recomendaciones_traslado rt
        LEFT JOIN productos p ON p.referencia = rt.referencia
        LEFT JOIN almacenes ao ON ao.codigo = rt.bodega_origen
        LEFT JOIN almacenes ad ON ad.codigo = rt.bodega_destino
        WHERE 1=1
    """
    p = []
    if estado:
        query += " AND rt.estado = %s"; p.append(estado)
    if prioridad:
        query += " AND rt.prioridad = %s"; p.append(prioridad)
    if regional:
        query += " AND rt.regional_destino = %s"; p.append(regional)
    query += " ORDER BY rt.dias_inv_destino ASC LIMIT %s"
    p.append(limit)

    cur.execute(query, tuple(p))
    rows = cur.fetchall()
    conn.close()

    return [{
        'id': r[0], 'referencia': r[1], 'producto_nombre': r[2],
        'bodega_origen': r[3], 'origen_nombre': r[4],
        'bodega_destino': r[5], 'destino_nombre': r[6],
        'cantidad_sugerida': safe_float(r[7]),
        'dias_inv_origen': safe_float(r[8]), 'dias_inv_destino': safe_float(r[9]),
        'prioridad': r[10], 'estado': r[11], 'regional': r[12],
        'ejecutada': r[13],
        'fecha_ejecucion': r[14].isoformat() if r[14] else None
    } for r in rows]


def update_traslado_estado(traslado_id, body, user):
    data = json.loads(body) if isinstance(body, str) else (body or {})
    nuevo_estado = data.get('estado')
    if nuevo_estado not in ('APROBADO', 'RECHAZADO', 'EJECUTADO'):
        return api_response(400, {'error': 'Estado inválido. Usar: APROBADO, RECHAZADO, EJECUTADO'})

    conn = get_db()
    cur = conn.cursor()
    ejecutada = nuevo_estado == 'EJECUTADO'
    cur.execute("""
        UPDATE recomendaciones_traslado
        SET estado = %s, ejecutada = %s, fecha_ejecucion = CASE WHEN %s THEN NOW() ELSE fecha_ejecucion END
        WHERE id = %s
    """, (nuevo_estado, ejecutada, ejecutada, int(traslado_id)))
    conn.commit()
    conn.close()
    return api_response(200, {'message': 'Traslado actualizado', 'id': traslado_id, 'estado': nuevo_estado})


# ============================================
# COMPRAS
# ============================================

def get_compras(params):
    limit = int(params.get('limit', 50))
    prioridad = params.get('prioridad')
    marca = params.get('marca')

    conn = get_db()
    cur = conn.cursor()

    # Verificar si hay datos pre-computados
    cur.execute("SELECT COUNT(*) FROM recomendaciones_compra WHERE estado = 'PENDIENTE'")
    precomputed = cur.fetchone()[0]

    if precomputed > 0:
        query = """
            SELECT rc.id, rc.referencia, p.nombre, m.nombre, rc.marca_categoria,
                   rc.marca_clasificacion, rc.stock_actual_red, rc.venta_proyectada,
                   rc.cantidad_sugerida, rc.dias_cobertura_actual, rc.dias_cobertura_objetivo,
                   rc.costo_unitario_estimado, rc.valor_compra_estimado,
                   rc.fecha_sugerida_pedido, rc.fecha_llegada_estimada, rc.prioridad, rc.estado
            FROM recomendaciones_compra rc
            LEFT JOIN productos p ON p.referencia = rc.referencia
            LEFT JOIN marcas m ON m.codigo = rc.marca_codigo
            WHERE rc.estado = 'PENDIENTE'
        """
        p = []
        if prioridad:
            query += " AND rc.prioridad = %s"; p.append(prioridad)
        if marca:
            query += " AND rc.marca_codigo = %s"; p.append(marca)
        query += " ORDER BY rc.prioridad ASC, rc.dias_cobertura_actual ASC LIMIT %s"
        p.append(limit)
        cur.execute(query, tuple(p))
        rows = cur.fetchall()
        conn.close()
        return [{
            'id': r[0], 'referencia': r[1], 'producto_nombre': r[2],
            'marca_nombre': r[3], 'marca_categoria': r[4], 'marca_clasificacion': r[5],
            'stock_actual_red': safe_float(r[6]), 'venta_proyectada': safe_float(r[7]),
            'cantidad_sugerida': safe_float(r[8]),
            'dias_cobertura_actual': safe_float(r[9]), 'dias_cobertura_objetivo': safe_float(r[10]),
            'costo_unitario_estimado': safe_float(r[11]), 'valor_compra_estimado': safe_float(r[12]),
            'fecha_sugerida_pedido': r[13].isoformat() if r[13] else None,
            'fecha_llegada_estimada': r[14].isoformat() if r[14] else None,
            'prioridad': r[15], 'estado': r[16]
        } for r in rows]

    # Cálculo en tiempo real: productos con venta pero stock insuficiente
    return _compras_live(conn, cur, limit, prioridad, marca)


def _compras_live(conn, cur, limit, prioridad_filter, marca_filter):
    """Calcula recomendaciones de compra en tiempo real"""
    d30 = date.today() - timedelta(days=30)

    query = """
        WITH venta_red AS (
            SELECT v.referencia, SUM(v.cantidad) as venta_30d,
                   SUM(v.cantidad) / 30.0 as venta_diaria
            FROM ventas v
            JOIN almacenes a ON a.codigo = v.bodega_codigo AND a.tipo = 'Venta'
            WHERE v.fecha >= %s
            GROUP BY v.referencia
            HAVING SUM(v.cantidad) > 0
        ),
        stock_red AS (
            SELECT referencia, SUM(cantidad) as stock_total,
                   AVG(NULLIF(valor_costo, 0)) as costo_prom
            FROM inventario_actual WHERE cantidad > 0
            GROUP BY referencia
        )
        SELECT
            0 as id, p.referencia, p.nombre, m.nombre as marca,
            m.categoria, m.clasificacion,
            COALESCE(sr.stock_total, 0) as stock_red,
            vr.venta_30d as venta_proyectada,
            GREATEST(
                CEIL((COALESCE(m.dias_cobertura_stock, 30) + COALESCE(m.lead_time_a_cedi, 15))
                     * vr.venta_diaria - COALESCE(sr.stock_total, 0)),
                0
            ) as cant_sugerida,
            CASE WHEN vr.venta_diaria > 0
                 THEN ROUND(COALESCE(sr.stock_total, 0) / vr.venta_diaria, 1)
                 ELSE 0 END as dias_cob_actual,
            COALESCE(m.dias_cobertura_stock, 30) as dias_cob_obj,
            COALESCE(sr.costo_prom, 0) as costo_unit,
            GREATEST(
                CEIL((COALESCE(m.dias_cobertura_stock, 30) + COALESCE(m.lead_time_a_cedi, 15))
                     * vr.venta_diaria - COALESCE(sr.stock_total, 0)),
                0
            ) * COALESCE(sr.costo_prom, 0) as valor_est,
            CASE
                WHEN COALESCE(sr.stock_total, 0) = 0 THEN 'URGENTE'
                WHEN vr.venta_diaria > 0 AND COALESCE(sr.stock_total,0) / vr.venta_diaria < COALESCE(m.lead_time_a_cedi, 15) THEN 'URGENTE'
                WHEN vr.venta_diaria > 0 AND COALESCE(sr.stock_total,0) / vr.venta_diaria < COALESCE(m.lead_time_a_cedi, 15) + 7 THEN 'ALTA'
                WHEN vr.venta_diaria > 0 AND COALESCE(sr.stock_total,0) / vr.venta_diaria < COALESCE(m.dias_cobertura_stock, 30) THEN 'MEDIA'
                ELSE 'BAJA'
            END as prioridad
        FROM venta_red vr
        JOIN productos p ON p.referencia = vr.referencia
        LEFT JOIN marcas m ON m.codigo = p.marca_codigo
        LEFT JOIN stock_red sr ON sr.referencia = vr.referencia
        WHERE COALESCE(sr.stock_total, 0) <
              (COALESCE(m.dias_cobertura_stock, 30) + COALESCE(m.lead_time_a_cedi, 15)) * vr.venta_diaria
    """
    params = [d30]
    if marca_filter:
        query += " AND m.codigo = %s"
        params.append(marca_filter)

    query += """ ORDER BY
        CASE WHEN COALESCE(sr.stock_total, 0) = 0 THEN 0 ELSE 1 END,
        CASE WHEN m.categoria = 'PRINCIPAL' THEN 0 ELSE 1 END,
        CASE WHEN m.clasificacion = 'A' THEN 1 WHEN m.clasificacion = 'B' THEN 2
             WHEN m.clasificacion = 'C' THEN 3 ELSE 4 END,
        dias_cob_actual ASC
        LIMIT %s
    """
    params.append(limit)

    cur.execute(query, tuple(params))
    rows = cur.fetchall()
    conn.close()

    results = [{
        'id': r[0], 'referencia': r[1], 'producto_nombre': r[2],
        'marca_nombre': r[3], 'marca_categoria': r[4], 'marca_clasificacion': r[5],
        'stock_actual_red': safe_float(r[6]), 'venta_proyectada': safe_float(r[7]),
        'cantidad_sugerida': safe_float(r[8]),
        'dias_cobertura_actual': safe_float(r[9]), 'dias_cobertura_objetivo': safe_float(r[10]),
        'costo_unitario_estimado': safe_float(r[11]), 'valor_compra_estimado': safe_float(r[12]),
        'fecha_sugerida_pedido': None, 'fecha_llegada_estimada': None,
        'prioridad': r[13], 'estado': 'CALCULADO'
    } for r in rows]

    if prioridad_filter:
        results = [r for r in results if r['prioridad'] == prioridad_filter]

    return results


# ============================================
# ALMACENES
# ============================================

def get_almacenes(params):
    regional = params.get('regional')
    tipo = params.get('tipo', 'Venta')

    conn = get_db()
    cur = conn.cursor()
    query = """
        SELECT a.codigo, a.nombre, a.tipo, a.regional, a.es_cedi,
               COUNT(DISTINCT m.referencia), COALESCE(SUM(m.stock_actual), 0),
               COALESCE(SUM(m.valor_stock), 0),
               AVG(m.dias_inventario) FILTER (WHERE m.dias_inventario < 9999)
        FROM almacenes a
        LEFT JOIN metricas_producto_almacen m ON m.bodega_codigo = a.codigo
            AND m.fecha_calculo = CURRENT_DATE
        WHERE a.activo = true
    """
    p = []
    if tipo:
        query += " AND a.tipo = %s"; p.append(tipo)
    if regional:
        query += " AND a.regional = %s"; p.append(regional)
    query += " GROUP BY a.codigo, a.nombre, a.tipo, a.regional, a.es_cedi ORDER BY 8 DESC NULLS LAST"

    cur.execute(query, tuple(p))
    rows = cur.fetchall()
    conn.close()

    return [{
        'codigo': r[0], 'nombre': r[1], 'tipo': r[2], 'regional': r[3],
        'es_cedi': r[4], 'productos': r[5], 'unidades': safe_float(r[6]),
        'valor_inventario': safe_float(r[7]), 'dias_inv_promedio': safe_float(r[8])
    } for r in rows]


# ============================================
# VENTAS
# ============================================

def get_ventas_diarias(params):
    regional = params.get('regional')
    dias = int(params.get('dias', 30))
    start = date.today() - timedelta(days=dias)

    conn = get_db()
    cur = conn.cursor()
    if regional:
        cur.execute("""
            SELECT v.fecha, COUNT(*), COALESCE(SUM(v.cantidad), 0), COALESCE(SUM(v.valor_total), 0)
            FROM ventas v JOIN almacenes a ON a.codigo = v.bodega_codigo
            WHERE v.fecha >= %s AND a.regional = %s
            GROUP BY v.fecha ORDER BY v.fecha
        """, (start, regional))
    else:
        cur.execute("""
            SELECT fecha, COUNT(*), COALESCE(SUM(cantidad), 0), COALESCE(SUM(valor_total), 0)
            FROM ventas WHERE fecha >= %s GROUP BY fecha ORDER BY fecha
        """, (start,))

    rows = cur.fetchall()
    conn.close()
    return [{
        'fecha': r[0].isoformat(), 'transacciones': r[1],
        'unidades': safe_float(r[2]), 'valor': safe_float(r[3])
    } for r in rows]


def get_eficiencia_diaria(params):
    """Ventas diarias vs valor inventario para gráfica de eficiencia"""
    regional = params.get('regional')
    d30 = date.today() - timedelta(days=30)

    conn = get_db()
    cur = conn.cursor()

    # Valor inventario actual (denominador constante)
    if regional:
        cur.execute("""
            SELECT COALESCE(SUM(ia.cantidad * ia.valor_costo), 0)
            FROM inventario_actual ia
            JOIN almacenes a ON a.codigo = ia.bodega_codigo
            WHERE a.regional = %s
        """, (regional,))
    else:
        cur.execute("SELECT COALESCE(SUM(cantidad * valor_costo), 0) FROM inventario_actual")
    valor_inventario = safe_float(cur.fetchone()[0])

    # Ventas diarias
    if regional:
        cur.execute("""
            SELECT v.fecha, COALESCE(SUM(v.valor_total), 0)
            FROM ventas v JOIN almacenes a ON a.codigo = v.bodega_codigo
            WHERE v.fecha >= %s AND a.regional = %s
            GROUP BY v.fecha ORDER BY v.fecha
        """, (d30, regional))
    else:
        cur.execute("""
            SELECT fecha, COALESCE(SUM(valor_total), 0)
            FROM ventas WHERE fecha >= %s GROUP BY fecha ORDER BY fecha
        """, (d30,))

    rows = cur.fetchall()

    # Snapshots de inventario (si existen, para valor histórico real)
    if regional:
        cur.execute("""
            SELECT s.fecha_snapshot, COALESCE(SUM(s.cantidad * s.valor_costo), 0)
            FROM inventario_snapshot s
            JOIN almacenes a ON a.codigo = s.bodega_codigo
            WHERE s.fecha_snapshot >= %s AND a.regional = %s
            GROUP BY s.fecha_snapshot ORDER BY s.fecha_snapshot
        """, (d30, regional))
    else:
        cur.execute("""
            SELECT fecha_snapshot, COALESCE(SUM(cantidad * valor_costo), 0)
            FROM inventario_snapshot WHERE fecha_snapshot >= %s
            GROUP BY fecha_snapshot ORDER BY fecha_snapshot
        """, (d30,))
    snapshots = {r[0].isoformat(): safe_float(r[1]) for r in cur.fetchall()}

    conn.close()

    result = []
    for r in rows:
        fecha_str = r[0].isoformat()
        venta = safe_float(r[1])
        inv = snapshots.get(fecha_str, valor_inventario)
        eficiencia = round((venta / inv * 100), 4) if inv > 0 else 0
        result.append({
            'fecha': fecha_str, 'venta': venta, 'inventario': inv,
            'eficiencia': eficiencia
        })

    return {'datos': result, 'valor_inventario_actual': valor_inventario}


def get_top_productos(params):
    limit = int(params.get('limit', 20))
    regional = params.get('regional')
    d30 = date.today() - timedelta(days=30)

    conn = get_db()
    cur = conn.cursor()
    if regional:
        cur.execute("""
            SELECT v.referencia, p.nombre, COALESCE(SUM(v.cantidad), 0),
                   COALESCE(SUM(v.valor_total), 0), COUNT(DISTINCT v.bodega_codigo)
            FROM ventas v
            LEFT JOIN productos p ON p.referencia = v.referencia
            JOIN almacenes a ON a.codigo = v.bodega_codigo
            WHERE v.fecha >= %s AND a.regional = %s
            GROUP BY v.referencia, p.nombre ORDER BY 3 DESC LIMIT %s
        """, (d30, regional, limit))
    else:
        cur.execute("""
            SELECT v.referencia, p.nombre, COALESCE(SUM(v.cantidad), 0),
                   COALESCE(SUM(v.valor_total), 0), COUNT(DISTINCT v.bodega_codigo)
            FROM ventas v LEFT JOIN productos p ON p.referencia = v.referencia
            WHERE v.fecha >= %s GROUP BY v.referencia, p.nombre ORDER BY 3 DESC LIMIT %s
        """, (d30, limit))

    rows = cur.fetchall()
    conn.close()
    return [{
        'referencia': r[0], 'producto_nombre': r[1],
        'unidades_vendidas': safe_float(r[2]), 'valor_vendido': safe_float(r[3]),
        'almacenes_venta': r[4]
    } for r in rows]


# ============================================
# PRODUCTOS (Búsqueda + Detalle)
# ============================================

def buscar_productos(params):
    q = params.get('q', '')
    limit = int(params.get('limit', 30))
    if len(q) < 2:
        return []

    conn = get_db()
    cur = conn.cursor()
    pattern = f'%{q}%'
    cur.execute("""
        SELECT p.referencia, p.nombre, p.codigo, m.nombre,
               COALESCE(SUM(ia.cantidad), 0), COUNT(DISTINCT ia.bodega_codigo) FILTER (WHERE ia.cantidad > 0)
        FROM productos p
        LEFT JOIN marcas m ON m.codigo = p.marca_codigo
        LEFT JOIN inventario_actual ia ON ia.referencia = p.referencia
        WHERE LOWER(p.nombre) LIKE LOWER(%s) OR LOWER(p.referencia) LIKE LOWER(%s)
            OR LOWER(p.codigo) LIKE LOWER(%s)
        GROUP BY p.referencia, p.nombre, p.codigo, m.nombre
        ORDER BY 5 DESC LIMIT %s
    """, (pattern, pattern, pattern, limit))

    rows = cur.fetchall()
    conn.close()
    return [{
        'referencia': r[0], 'nombre': r[1], 'codigo': r[2], 'marca': r[3],
        'stock_total': safe_float(r[4]), 'almacenes_con_stock': r[5]
    } for r in rows]


def get_producto_detalle(params):
    ref = params.get('ref', '')
    if not ref:
        return None

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT p.referencia, p.nombre, p.codigo, m.nombre, m.categoria,
               m.clasificacion, m.dias_cobertura_stock
        FROM productos p LEFT JOIN marcas m ON m.codigo = p.marca_codigo
        WHERE p.referencia = %s
    """, (ref,))
    prod = cur.fetchone()
    if not prod:
        conn.close()
        return None

    cur.execute("""
        SELECT ia.bodega_codigo, a.nombre, a.tipo, a.regional,
               ia.cantidad, ia.valor_costo,
               COALESCE(m.venta_diaria_promedio, 0), COALESCE(m.dias_inventario, 0),
               COALESCE(m.estado_stock, 'SIN_DATOS')
        FROM inventario_actual ia
        JOIN almacenes a ON a.codigo = ia.bodega_codigo
        LEFT JOIN metricas_producto_almacen m ON m.referencia = ia.referencia
            AND m.bodega_codigo = ia.bodega_codigo AND m.fecha_calculo = CURRENT_DATE
        WHERE ia.referencia = %s AND ia.cantidad > 0
        ORDER BY ia.cantidad DESC
    """, (ref,))
    almacenes = cur.fetchall()

    d30 = date.today() - timedelta(days=30)
    cur.execute("""
        SELECT fecha, SUM(cantidad), SUM(valor_total)
        FROM ventas WHERE referencia = %s AND fecha >= %s
        GROUP BY fecha ORDER BY fecha
    """, (ref, d30))
    ventas = cur.fetchall()
    conn.close()

    return {
        'producto': {
            'referencia': prod[0], 'nombre': prod[1], 'codigo': prod[2],
            'marca': prod[3], 'categoria': prod[4], 'clasificacion': prod[5],
            'dias_cobertura': prod[6]
        },
        'almacenes': [{
            'codigo': r[0], 'nombre': r[1], 'tipo': r[2], 'regional': r[3],
            'stock': safe_float(r[4]), 'valor_costo': safe_float(r[5]),
            'venta_diaria': safe_float(r[6]), 'dias_inventario': safe_float(r[7]),
            'estado': r[8]
        } for r in almacenes],
        'ventas_30d': [{
            'fecha': r[0].isoformat(), 'cantidad': safe_float(r[1]), 'valor': safe_float(r[2])
        } for r in ventas]
    }


# ============================================
# REGIONALES
# ============================================

def get_regionales():
    conn = get_db()
    cur = conn.cursor()
    d30 = date.today() - timedelta(days=30)

    cur.execute("""
        SELECT r.codigo, r.nombre,
               COUNT(DISTINCT a.codigo) FILTER (WHERE a.tipo = 'Venta'),
               COALESCE(SUM(ia.cantidad * ia.valor_costo), 0)
        FROM regionales r
        LEFT JOIN almacenes a ON a.regional = r.codigo
        LEFT JOIN inventario_actual ia ON ia.bodega_codigo = a.codigo
        WHERE r.activo = true
        GROUP BY r.codigo, r.nombre
        ORDER BY 4 DESC
    """)
    regiones = cur.fetchall()

    result = []
    for rg in regiones:
        cur.execute("""
            SELECT COALESCE(SUM(v.valor_total), 0)
            FROM ventas v JOIN almacenes a ON a.codigo = v.bodega_codigo
            WHERE a.regional = %s AND v.fecha >= %s
        """, (rg[0], d30))
        ventas = safe_float(cur.fetchone()[0])

        cur.execute("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE al.nivel = 'CRITICO')
            FROM alertas al JOIN almacenes a ON a.codigo = al.bodega_codigo
            WHERE a.regional = %s AND al.estado = 'PENDIENTE'
        """, (rg[0],))
        ar = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*) FROM recomendaciones_traslado
            WHERE regional_destino = %s AND estado = 'PENDIENTE'
        """, (rg[0],))
        traslados = cur.fetchone()[0]

        result.append({
            'codigo': rg[0], 'nombre': rg[1], 'almacenes_venta': rg[2],
            'valor_inventario': safe_float(rg[3]), 'ventas_30d': ventas,
            'alertas_pendientes': ar[0], 'alertas_criticas': ar[1],
            'traslados_pendientes': traslados
        })

    conn.close()
    return result


# ============================================
# ANÁLISIS DE INVENTARIO
# ============================================

def get_analisis_inventario(params):
    """Desglose de inventario por marca con porcentajes"""
    regional = params.get('regional')
    tipo_almacen = params.get('tipo', 'Venta')

    conn = get_db()
    cur = conn.cursor()

    # Total general para calcular porcentajes
    tot_q = """
        SELECT COALESCE(SUM(ia.cantidad), 0), COALESCE(SUM(ia.cantidad * ia.valor_costo), 0)
        FROM inventario_actual ia
        JOIN almacenes a ON a.codigo = ia.bodega_codigo
        WHERE 1=1
    """
    tot_p = []
    if tipo_almacen:
        tot_q += " AND a.tipo = %s"; tot_p.append(tipo_almacen)
    if regional:
        tot_q += " AND a.regional = %s"; tot_p.append(regional)
    cur.execute(tot_q, tuple(tot_p))
    tr = cur.fetchone()
    total_unidades = safe_float(tr[0])
    total_valor = safe_float(tr[1])

    # Desglose por marca
    q = """
        SELECT m.codigo, m.nombre, m.categoria, m.clasificacion,
               COUNT(DISTINCT ia.referencia) as productos,
               COUNT(DISTINCT ia.bodega_codigo) as almacenes,
               COALESCE(SUM(ia.cantidad), 0) as unidades,
               COALESCE(SUM(ia.cantidad * ia.valor_costo), 0) as valor
        FROM inventario_actual ia
        JOIN almacenes a ON a.codigo = ia.bodega_codigo
        JOIN productos p ON p.referencia = ia.referencia
        LEFT JOIN marcas m ON m.codigo = p.marca_codigo
        WHERE ia.cantidad > 0
    """
    p = []
    if tipo_almacen:
        q += " AND a.tipo = %s"; p.append(tipo_almacen)
    if regional:
        q += " AND a.regional = %s"; p.append(regional)
    q += """
        GROUP BY m.codigo, m.nombre, m.categoria, m.clasificacion
        ORDER BY valor DESC
    """
    cur.execute(q, tuple(p))
    rows = cur.fetchall()

    # Ventas 30d por marca
    d30 = date.today() - timedelta(days=30)
    vq = """
        SELECT p.marca_codigo, COALESCE(SUM(v.cantidad), 0), COALESCE(SUM(v.valor_total), 0)
        FROM ventas v
        JOIN almacenes a ON a.codigo = v.bodega_codigo AND a.tipo = 'Venta'
        JOIN productos p ON p.referencia = v.referencia
        WHERE v.fecha >= %s
    """
    vp = [d30]
    if regional:
        vq += " AND a.regional = %s"; vp.append(regional)
    vq += " GROUP BY p.marca_codigo"
    cur.execute(vq, tuple(vp))
    ventas_marca = {r[0]: {'unidades': safe_float(r[1]), 'valor': safe_float(r[2])} for r in cur.fetchall()}

    conn.close()

    marcas = []
    for r in rows:
        codigo = r[0]
        valor = safe_float(r[7])
        unidades = safe_float(r[6])
        vta = ventas_marca.get(codigo, {'unidades': 0, 'valor': 0})
        rotacion = (vta['valor'] / valor * 100) if valor > 0 else 0

        marcas.append({
            'codigo': codigo, 'nombre': r[1] or 'Sin marca',
            'categoria': r[2], 'clasificacion': r[3],
            'productos': r[4], 'almacenes': r[5],
            'unidades': unidades, 'valor': valor,
            'pct_unidades': round(unidades / total_unidades * 100, 2) if total_unidades > 0 else 0,
            'pct_valor': round(valor / total_valor * 100, 2) if total_valor > 0 else 0,
            'venta_30d_unidades': vta['unidades'], 'venta_30d_valor': vta['valor'],
            'rotacion_pct': round(rotacion, 2)
        })

    return {
        'total_unidades': total_unidades, 'total_valor': total_valor,
        'total_marcas': len(marcas), 'marcas': marcas
    }


# ============================================
# EXPORTACIÓN CSV
# ============================================

def _csv_escape(val):
    s = str(val).replace('"', '""')
    return f'"{s}"'


def export_csv(tipo, params):
    params_copy = dict(params)
    params_copy['limit'] = params.get('limit', '500')

    if tipo == 'alertas':
        data = get_alertas(params_copy)
        headers = ['ID', 'Tipo', 'Nivel', 'Referencia', 'Producto', 'Almacén', 'Regional', 'Stock', 'Días Inv.', 'Venta Diaria', 'Estado', 'Fecha']
        rows = [[d['id'], d['tipo_alerta'], d['nivel'], d['referencia'],
                 d.get('producto_nombre', ''), d.get('almacen_nombre', ''), d.get('regional', ''),
                 d['stock_actual'], d['dias_inventario'], d['venta_diaria'],
                 d['estado'], d.get('fecha_generacion', '')] for d in data]
    elif tipo == 'traslados':
        data = get_traslados(params_copy)
        headers = ['ID', 'Referencia', 'Producto', 'Origen', 'Destino', 'Regional', 'Cantidad', 'Días Origen', 'Días Destino', 'Prioridad', 'Estado']
        rows = [[d['id'], d['referencia'], d.get('producto_nombre', ''),
                 d.get('origen_nombre', ''), d.get('destino_nombre', ''), d.get('regional', ''),
                 d['cantidad_sugerida'], d['dias_inv_origen'], d['dias_inv_destino'],
                 d['prioridad'], d['estado']] for d in data]
    elif tipo == 'compras':
        data = get_compras(params_copy)
        headers = ['ID', 'Referencia', 'Producto', 'Marca', 'Stock Red', 'Cant. Sugerida', 'Cobertura Actual', 'Cobertura Obj.', 'Valor Estimado', 'Prioridad']
        rows = [[d['id'], d['referencia'], d.get('producto_nombre', ''),
                 d.get('marca_nombre', ''), d['stock_actual_red'], d['cantidad_sugerida'],
                 d['dias_cobertura_actual'], d['dias_cobertura_objetivo'],
                 d['valor_compra_estimado'], d['prioridad']] for d in data]
    elif tipo == 'almacenes':
        data = get_almacenes(params_copy)
        headers = ['Código', 'Nombre', 'Tipo', 'Regional', 'Productos', 'Unidades', 'Valor Inventario', 'Días Inv. Prom.']
        rows = [[d['codigo'], d['nombre'], d['tipo'], d.get('regional', ''),
                 d['productos'], d['unidades'], d['valor_inventario'],
                 d['dias_inv_promedio']] for d in data]
    else:
        return api_response(400, {'error': f'Tipo de exportación no válido: {tipo}'})

    csv_lines = [','.join(headers)]
    for row in rows:
        csv_lines.append(','.join([_csv_escape(c) for c in row]))

    filename = f'stockiq_{tipo}_{date.today().strftime("%Y%m%d")}.csv'
    return csv_response(filename, '\n'.join(csv_lines))


# ============================================
# HANDLER PRINCIPAL
# ============================================

def handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return api_response(200, {})

    path = event.get('path', '')
    method = event.get('httpMethod', 'GET')
    params = event.get('queryStringParameters') or {}
    body = event.get('body') or '{}'

    # Ruta pública
    if path == '/api/auth/login' and method == 'POST':
        return login(body)

    # Todas las demás rutas requieren autenticación
    user = authenticate(event)
    if not user:
        return api_response(401, {'error': 'No autorizado. Inicia sesión.'})

    try:
        # Auth
        if path == '/api/auth/logout' and method == 'POST':
            return logout(event)
        if path == '/api/auth/me':
            return api_response(200, user)

        # Filtros
        if path == '/api/filtros':
            return api_response(200, get_filtros())

        # KPIs
        if path == '/api/kpis':
            return api_response(200, get_kpis(params))

        # Alertas
        if path == '/api/alertas' and method == 'GET':
            return api_response(200, get_alertas(params))
        if path == '/api/alertas/resumen':
            return api_response(200, get_alertas_resumen())
        if path.startswith('/api/alertas/') and path.endswith('/estado') and method == 'PATCH':
            alerta_id = path.split('/')[3]
            return update_alerta_estado(alerta_id, body, user)

        # Traslados
        if path == '/api/traslados' and method == 'GET':
            return api_response(200, get_traslados(params))
        if path.startswith('/api/traslados/') and path.endswith('/estado') and method == 'PATCH':
            traslado_id = path.split('/')[3]
            return update_traslado_estado(traslado_id, body, user)

        # Compras
        if path == '/api/compras':
            return api_response(200, get_compras(params))

        # Almacenes
        if path == '/api/almacenes':
            return api_response(200, get_almacenes(params))

        # Ventas
        if path == '/api/ventas/diarias':
            return api_response(200, get_ventas_diarias(params))
        if path == '/api/ventas/top-productos':
            return api_response(200, get_top_productos(params))
        if path == '/api/ventas/eficiencia':
            return api_response(200, get_eficiencia_diaria(params))

        # Productos
        if path == '/api/productos/buscar':
            return api_response(200, buscar_productos(params))
        if path == '/api/productos/detalle':
            detalle = get_producto_detalle(params)
            if detalle:
                return api_response(200, detalle)
            return api_response(404, {'error': 'Producto no encontrado'})

        # Análisis
        if path == '/api/analisis/inventario':
            return api_response(200, get_analisis_inventario(params))

        # Regionales
        if path == '/api/regionales':
            return api_response(200, get_regionales())

        # Export CSV
        if path.startswith('/api/export/'):
            tipo = path.split('/')[3]
            return export_csv(tipo, params)

        return api_response(404, {'error': 'Endpoint no encontrado'})

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return api_response(500, {'error': str(e)})
