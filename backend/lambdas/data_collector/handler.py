"""
Data Collector Lambda - StockIQ
Extrae datos de APIs SOAP (Ventas e Inventario) y los carga a PostgreSQL
"""

import json
import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal
import xml.etree.ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_values
import boto3

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_http_session():
    """Crea una sesión HTTP con reintentos y configuración robusta"""
    session = requests.Session()
    
    # Configurar reintentos
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

# ============================================
# CONFIGURACIÓN
# ============================================

# APIs SOAP
VENTAS_API_URL = "https://gspapiest.fomplus.com/srvAPI.asmx"
INVENTARIO_API_URL = "https://gspapi.fomplus.com/srvAPI.asmx"

# Token (en producción usar Secrets Manager)
API_TOKEN = os.environ.get('API_TOKEN', '0db03ce0e7f6ad6d153f7d53585fff6b')

# Parámetros de empresa/base de datos
EMPRESA = os.environ.get('EMPRESA', 'GSPSAS')  # Ajustar según tu config
BASE_DATOS = os.environ.get('BASE_DATOS', 'GSPSAS')  # Ajustar según tu config

# ============================================
# FUNCIONES SOAP
# ============================================

def call_soap_ventas(fecha_inicio: str, fecha_fin: str, token: str) -> dict:
    """
    Llama a la API SOAP de ventas
    
    Args:
        fecha_inicio: Fecha inicio formato 'YYYY-MM-DD'
        fecha_fin: Fecha fin formato 'YYYY-MM-DD'
        token: Token de autenticación
    
    Returns:
        dict con los datos de ventas
    """
    
    # Convertir fechas al formato esperado por la API
    fecha_ini_dt = f"{fecha_inicio}T00:00:00"
    fecha_fin_dt = f"{fecha_fin}T23:59:59"
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GenerarInfoVentas xmlns="http://tempuri.org/">
      <strPar_Empresa>{EMPRESA}</strPar_Empresa>
      <datPar_FecIni>{fecha_ini_dt}</datPar_FecIni>
      <datPar_FecFin>{fecha_fin_dt}</datPar_FecFin>
      <objPar_Objeto>{token}</objPar_Objeto>
    </GenerarInfoVentas>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://tempuri.org/GenerarInfoVentas',
        'User-Agent': 'StockIQ/1.0'
    }
    
    logger.info(f"Llamando API Ventas: {fecha_inicio} a {fecha_fin}")
    
    session = get_http_session()
    response = session.post(VENTAS_API_URL, data=soap_body.encode('utf-8'), headers=headers, timeout=300)
    response.raise_for_status()
    
    # Parsear respuesta XML y extraer JSON
    return parse_soap_response(response.text, 'GenerarInfoVentasResult')


def call_soap_inventario(fecha: str, bodega: str, token: str, pagina: int = 1, filas: int = 1000) -> dict:
    """
    Llama a la API SOAP de inventario
    
    Args:
        fecha: Fecha formato 'YYYY-MM-DD'
        bodega: Código de bodega (vacío para todas)
        token: Token de autenticación
        pagina: Número de página
        filas: Registros por página
    
    Returns:
        dict con los datos de inventario
    """
    
    fecha_dt = f"{fecha}T00:00:00"
    
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GenerarInformacionInventarios xmlns="http://tempuri.org/">
      <datPar_Fecha>{fecha_dt}</datPar_Fecha>
      <strPar_Bodega>{bodega}</strPar_Bodega>
      <bolPar_ConSaldo>true</bolPar_ConSaldo>
      <bolPar_ConImg>false</bolPar_ConImg>
      <strPar_Basedatos>{BASE_DATOS}</strPar_Basedatos>
      <strPar_Token>{token}</strPar_Token>
      <strError></strError>
      <intPar_Filas>{filas}</intPar_Filas>
      <intPar_Pagina>{pagina}</intPar_Pagina>
      <intPar_LisPre>1</intPar_LisPre>
      <bolPar_ConSer>false</bolPar_ConSer>
    </GenerarInformacionInventarios>
  </soap:Body>
</soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        'SOAPAction': 'http://tempuri.org/GenerarInformacionInventarios',
        'User-Agent': 'StockIQ/1.0'
    }
    
    logger.info(f"Llamando API Inventario: {fecha}, Bodega: {bodega or 'TODAS'}, Página: {pagina}")
    
    session = get_http_session()
    response = session.post(INVENTARIO_API_URL, data=soap_body.encode('utf-8'), headers=headers, timeout=300)
    response.raise_for_status()
    
    return parse_soap_response(response.text, 'GenerarInformacionInventariosResult')


def parse_soap_response(xml_text: str, result_tag: str) -> dict:
    """
    Parsea la respuesta SOAP y extrae el JSON del resultado
    """
    try:
        # Caso especial: la API de ventas devuelve JSON seguido de XML
        # Buscar si hay un array JSON al inicio o en medio del texto
        if '[{' in xml_text:
            # Encontrar el inicio del JSON array
            start_idx = xml_text.find('[{')
            # Encontrar el final del JSON array (último }])
            end_idx = xml_text.rfind('}]') + 2
            
            if start_idx != -1 and end_idx > start_idx:
                json_str = xml_text[start_idx:end_idx]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass
        
        # Intento estándar: parsear como XML SOAP
        # Remover namespaces para facilitar el parseo
        xml_clean = xml_text.replace('xmlns=', 'xmlns_original=')
        root = ET.fromstring(xml_clean)
        
        # Buscar el tag de resultado en cualquier nivel
        for elem in root.iter():
            if result_tag in elem.tag:
                # El contenido puede ser JSON string
                if elem.text:
                    try:
                        return json.loads(elem.text)
                    except json.JSONDecodeError:
                        return {'raw': elem.text}
        
        logger.warning(f"No se encontró el tag {result_tag} en la respuesta")
        return {'error': 'Tag no encontrado', 'xml': xml_text[:500]}
        
    except ET.ParseError as e:
        # Si falla el parseo XML, intentar extraer JSON directamente
        if '[{' in xml_text:
            start_idx = xml_text.find('[{')
            end_idx = xml_text.rfind('}]') + 2
            if start_idx != -1 and end_idx > start_idx:
                json_str = xml_text[start_idx:end_idx]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass
        
        logger.error(f"Error parseando XML: {e}")
        return {'error': str(e), 'xml': xml_text[:500]}


# ============================================
# FUNCIONES DE BASE DE DATOS
# ============================================

def get_db_connection():
    """
    Obtiene conexión a PostgreSQL usando credenciales de Secrets Manager
    """
    # En desarrollo, usar variables de entorno
    if os.environ.get('LOCAL_DEV'):
        return psycopg2.connect(
            host=os.environ['DB_HOST'],
            port=os.environ.get('DB_PORT', 5432),
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
    
    # En producción, usar Secrets Manager
    secrets_client = boto3.client('secretsmanager')
    secret_arn = os.environ['DB_SECRET_ARN']
    
    response = secrets_client.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    
    return psycopg2.connect(
        host=secret['host'],
        port=secret['port'],
        database=secret['dbname'],
        user=secret['username'],
        password=secret['password']
    )


def upsert_productos(conn, productos: list):
    """
    Inserta o actualiza productos en la base de datos
    """
    if not productos:
        return 0
    
    query = """
    INSERT INTO productos (referencia, codigo, nombre, unidad_medida, clase, grupo, linea, marca_codigo)
    VALUES %s
    ON CONFLICT (referencia) DO UPDATE SET
        codigo = EXCLUDED.codigo,
        nombre = EXCLUDED.nombre,
        unidad_medida = EXCLUDED.unidad_medida,
        clase = EXCLUDED.clase,
        grupo = EXCLUDED.grupo,
        linea = EXCLUDED.linea,
        updated_at = CURRENT_TIMESTAMP
    """
    
    # Preparar datos
    values = []
    for p in productos:
        values.append((
            p.get('REFERENCIA') or p.get('REFER'),
            p.get('CODIGO'),
            p.get('NOMBRE') or p.get('NOMREF'),
            p.get('UNIDADMED') or p.get('UNDMED'),
            p.get('CLASE'),
            p.get('GRUPO'),
            p.get('LINEA'),
            p.get('MARCA')  # Esto necesitará mapeo al código de marca
        ))
    
    with conn.cursor() as cur:
        execute_values(cur, query, values)
    
    conn.commit()
    return len(values)


def insert_ventas(conn, ventas: list):
    """
    Inserta ventas en la base de datos (evita duplicados)
    """
    if not ventas:
        return 0
    
    query = """
    INSERT INTO ventas (
        tipo_movimiento, prefijo, numero_documento, fecha, hora,
        cedula_cliente, nombre_cliente, codigo_seccion, nombre_seccion,
        bodega_codigo, referencia, cantidad, valor_unitario, valor_total,
        porcentaje_descuento, valor_descuento, valor_costo, valor_utilidad,
        porcentaje_utilidad, porcentaje_iva, vendedor_codigo, vendedor_nombre
    ) VALUES %s
    ON CONFLICT (prefijo, numero_documento, referencia) DO NOTHING
    """
    
    values = []
    for v in ventas:
        # Parsear hora
        hora = None
        if v.get('HORA'):
            try:
                hora = v['HORA']
            except:
                pass
        
        values.append((
            v.get('TIPMOV'),
            v.get('PREFIJO'),
            v.get('NUMDOC'),
            v.get('FECHA'),
            hora,
            v.get('CEDULA'),
            v.get('NOMCED'),
            v.get('CODSEC'),
            v.get('NOMSEC'),
            v.get('BODEGA'),
            v.get('REFER'),
            Decimal(str(v.get('CANTID', 0))),
            Decimal(str(v.get('VALUND', 0))),
            Decimal(str(v.get('VALTOT', 0))),
            Decimal(str(v.get('PORDES', 0))),
            Decimal(str(v.get('VALDES', 0))),
            Decimal(str(v.get('VCOSTO', 0))),
            Decimal(str(v.get('VALUTI', 0))),
            Decimal(str(v.get('PORUTI', 0))),
            Decimal(str(v.get('PORIVA', 0))),
            v.get('VENDED'),
            v.get('NOMVEN')
        ))
    
    with conn.cursor() as cur:
        execute_values(cur, query, values)
    
    conn.commit()
    return len(values)


def upsert_inventario(conn, inventario: list, fecha_snapshot: str):
    """
    Inserta o actualiza snapshot de inventario
    """
    if not inventario:
        return 0
    
    query_actual = """
    INSERT INTO inventario_actual (bodega_codigo, referencia, cantidad, valor_costo, valor_venta, observacion)
    VALUES %s
    ON CONFLICT (bodega_codigo, referencia) DO UPDATE SET
        cantidad = EXCLUDED.cantidad,
        valor_costo = EXCLUDED.valor_costo,
        valor_venta = EXCLUDED.valor_venta,
        observacion = EXCLUDED.observacion,
        ultima_actualizacion = CURRENT_TIMESTAMP
    """
    
    query_snapshot = """
    INSERT INTO inventario_snapshot (fecha_snapshot, bodega_codigo, referencia, cantidad, valor_costo, valor_venta, observacion)
    VALUES %s
    ON CONFLICT (fecha_snapshot, bodega_codigo, referencia) DO UPDATE SET
        cantidad = EXCLUDED.cantidad,
        valor_costo = EXCLUDED.valor_costo,
        valor_venta = EXCLUDED.valor_venta,
        observacion = EXCLUDED.observacion
    """
    
    values_actual = []
    values_snapshot = []
    
    for i in inventario:
        bodega = i.get('BODEGA')
        referencia = i.get('REFERENCIA')
        cantidad = Decimal(str(i.get('CANTIDAD', 0)))
        valor_costo = Decimal(str(i.get('VCOSTO', 0)))
        valor_venta = Decimal(str(i.get('VVENTA', 0)))
        observacion = (i.get('OBSERV1') or '').strip() or None
        
        values_actual.append((bodega, referencia, cantidad, valor_costo, valor_venta, observacion))
        values_snapshot.append((fecha_snapshot, bodega, referencia, cantidad, valor_costo, valor_venta, observacion))
    
    with conn.cursor() as cur:
        execute_values(cur, query_actual, values_actual)
        execute_values(cur, query_snapshot, values_snapshot)
    
    conn.commit()
    return len(values_actual)


# ============================================
# FUNCIONES DE EXTRACCIÓN
# ============================================

def extraer_ventas(fecha_inicio: str, fecha_fin: str) -> list:
    """
    Extrae ventas de la API SOAP
    """
    resultado = call_soap_ventas(fecha_inicio, fecha_fin, API_TOKEN)
    
    # La respuesta puede venir en diferentes formatos
    if isinstance(resultado, list):
        return resultado
    elif isinstance(resultado, dict):
        # Buscar el array de ventas en la respuesta
        if 'ventas' in resultado:
            return resultado['ventas']
        elif 'data' in resultado:
            return resultado['data']
        elif 'resultado' in resultado:
            return resultado['resultado']
    
    logger.warning(f"Formato de respuesta ventas no reconocido: {type(resultado)}")
    return []


def extraer_inventario(fecha: str) -> list:
    """
    Extrae inventario de todas las bodegas (con paginación)
    """
    todos_los_items = []
    pagina = 1
    filas_por_pagina = 1000
    
    while True:
        resultado = call_soap_inventario(fecha, "", API_TOKEN, pagina, filas_por_pagina)
        
        items = []
        if isinstance(resultado, list):
            items = resultado
        elif isinstance(resultado, dict):
            if 'inventario' in resultado:
                items = resultado['inventario']
            elif 'data' in resultado:
                items = resultado['data']
            elif 'resultado' in resultado:
                items = resultado['resultado']
        
        if not items:
            break
        
        todos_los_items.extend(items)
        logger.info(f"Página {pagina}: {len(items)} items extraídos")
        
        # Si vienen menos items que el tamaño de página, es la última
        if len(items) < filas_por_pagina:
            break
        
        pagina += 1
        
        # Límite de seguridad
        if pagina > 100:
            logger.warning("Se alcanzó el límite de 100 páginas")
            break
    
    return todos_los_items


# ============================================
# HANDLERS LAMBDA
# ============================================

def handler_ventas(event, context):
    """
    Handler para extraer ventas
    """
    logger.info(f"Iniciando extracción de ventas: {event}")
    
    # Obtener rango de fechas (por defecto: ayer)
    fecha_fin = event.get('fecha_fin', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
    fecha_inicio = event.get('fecha_inicio', fecha_fin)
    
    try:
        # Extraer ventas
        ventas = extraer_ventas(fecha_inicio, fecha_fin)
        logger.info(f"Ventas extraídas: {len(ventas)}")
        
        if not ventas:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No hay ventas para el período',
                    'fecha_inicio': fecha_inicio,
                    'fecha_fin': fecha_fin
                })
            }
        
        # Conectar a BD y guardar
        conn = get_db_connection()
        try:
            # Extraer productos únicos y guardarlos primero
            productos_unicos = {v.get('REFER'): v for v in ventas if v.get('REFER')}.values()
            n_productos = upsert_productos(conn, list(productos_unicos))
            logger.info(f"Productos actualizados: {n_productos}")
            
            # Guardar ventas
            n_ventas = insert_ventas(conn, ventas)
            logger.info(f"Ventas insertadas: {n_ventas}")
            
        finally:
            conn.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extracción completada',
                'fecha_inicio': fecha_inicio,
                'fecha_fin': fecha_fin,
                'ventas_procesadas': len(ventas),
                'ventas_insertadas': n_ventas,
                'productos_actualizados': n_productos
            })
        }
        
    except Exception as e:
        logger.error(f"Error en extracción de ventas: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def handler_inventario(event, context):
    """
    Handler para extraer inventario
    """
    logger.info(f"Iniciando extracción de inventario: {event}")
    
    # Fecha del snapshot (por defecto: hoy)
    fecha = event.get('fecha', datetime.now().strftime('%Y-%m-%d'))
    
    try:
        # Extraer inventario
        inventario = extraer_inventario(fecha)
        logger.info(f"Items de inventario extraídos: {len(inventario)}")
        
        if not inventario:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No hay datos de inventario',
                    'fecha': fecha
                })
            }
        
        # Conectar a BD y guardar
        conn = get_db_connection()
        try:
            # Extraer productos únicos y guardarlos primero
            productos_unicos = {i.get('REFERENCIA'): i for i in inventario if i.get('REFERENCIA')}.values()
            n_productos = upsert_productos(conn, list(productos_unicos))
            logger.info(f"Productos actualizados: {n_productos}")
            
            # Guardar inventario
            n_inventario = upsert_inventario(conn, inventario, fecha)
            logger.info(f"Inventario actualizado: {n_inventario}")
            
        finally:
            conn.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Extracción completada',
                'fecha': fecha,
                'items_procesados': len(inventario),
                'items_actualizados': n_inventario,
                'productos_actualizados': n_productos
            })
        }
        
    except Exception as e:
        logger.error(f"Error en extracción de inventario: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def handler(event, context):
    """
    Handler principal - puede ejecutar ventas, inventario o ambos
    """
    tipo = event.get('tipo', 'ambos')
    
    resultados = {}
    
    if tipo in ['ventas', 'ambos']:
        resultados['ventas'] = handler_ventas(event, context)
    
    if tipo in ['inventario', 'ambos']:
        resultados['inventario'] = handler_inventario(event, context)
    
    return {
        'statusCode': 200,
        'body': json.dumps(resultados, default=str)
    }


# ============================================
# PARA PRUEBAS LOCALES
# ============================================

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    os.environ['LOCAL_DEV'] = 'true'
    
    # Probar extracción de ventas
    print("=== Probando extracción de ventas ===")
    resultado_ventas = handler_ventas({
        'fecha_inicio': '2024-01-01',
        'fecha_fin': '2024-01-01'
    }, None)
    print(json.dumps(resultado_ventas, indent=2))
    
    # Probar extracción de inventario
    print("\n=== Probando extracción de inventario ===")
    resultado_inv = handler_inventario({
        'fecha': '2024-01-15'
    }, None)
    print(json.dumps(resultado_inv, indent=2))