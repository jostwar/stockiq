#!/usr/bin/env python3
"""
Script para probar el Data Collector localmente
Ejecutar desde la raíz del proyecto:
    python backend/lambdas/data_collector/test_local.py
"""

import os
import sys
import json
from datetime import datetime, timedelta

# Agregar el directorio al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ============================================
# CONFIGURACIÓN - EDITAR ESTOS VALORES
# ============================================

DB_CONFIG = {
    'host': 'inventory-platform-db.cmal9qmniwdx.us-east-1.rds.amazonaws.com',
    'port': '5432',
    'name': 'inventory_db',
    'user': 'inventory_admin',
    'password': 'Gsp2026*'  # <-- CAMBIAR
}

# ============================================
# CONFIGURAR ENTORNO
# ============================================

os.environ['LOCAL_DEV'] = 'true'
os.environ['DB_HOST'] = DB_CONFIG['host']
os.environ['DB_PORT'] = DB_CONFIG['port']
os.environ['DB_NAME'] = DB_CONFIG['name']
os.environ['DB_USER'] = DB_CONFIG['user']
os.environ['DB_PASSWORD'] = DB_CONFIG['password']

# Ahora importar el handler
from handler import (
    call_soap_ventas,
    call_soap_inventario,
    handler_ventas,
    handler_inventario,
    API_TOKEN
)


def test_conexion_api_ventas():
    """Prueba la conexión a la API de ventas"""
    print("\n" + "="*50)
    print("TEST: Conexión API Ventas")
    print("="*50)
    
    fecha = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        resultado = call_soap_ventas(fecha, fecha, API_TOKEN)
        print(f"✅ Conexión exitosa")
        print(f"   Tipo respuesta: {type(resultado)}")
        
        if isinstance(resultado, list):
            print(f"   Registros: {len(resultado)}")
            if resultado:
                print(f"   Ejemplo: {json.dumps(resultado[0], indent=2, default=str)[:500]}")
        elif isinstance(resultado, dict):
            print(f"   Keys: {list(resultado.keys())}")
            
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_conexion_api_inventario():
    """Prueba la conexión a la API de inventario"""
    print("\n" + "="*50)
    print("TEST: Conexión API Inventario")
    print("="*50)
    
    fecha = datetime.now().strftime('%Y-%m-%d')
    
    try:
        resultado = call_soap_inventario(fecha, "", API_TOKEN, pagina=1, filas=10)
        print(f"✅ Conexión exitosa")
        print(f"   Tipo respuesta: {type(resultado)}")
        
        if isinstance(resultado, list):
            print(f"   Registros: {len(resultado)}")
            if resultado:
                print(f"   Ejemplo: {json.dumps(resultado[0], indent=2, default=str)[:500]}")
        elif isinstance(resultado, dict):
            print(f"   Keys: {list(resultado.keys())}")
            
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_conexion_bd():
    """Prueba la conexión a la base de datos"""
    print("\n" + "="*50)
    print("TEST: Conexión Base de Datos")
    print("="*50)
    
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['name'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM almacenes")
            almacenes = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM marcas")
            marcas = cur.fetchone()[0]
        
        conn.close()
        
        print(f"✅ Conexión exitosa")
        print(f"   Almacenes: {almacenes}")
        print(f"   Marcas: {marcas}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_extraccion_ventas():
    """Prueba la extracción completa de ventas"""
    print("\n" + "="*50)
    print("TEST: Extracción Ventas (últimos 7 días)")
    print("="*50)
    
    fecha_fin = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    try:
        resultado = handler_ventas({
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin
        }, None)
        
        print(f"✅ Extracción completada")
        print(f"   Resultado: {json.dumps(json.loads(resultado['body']), indent=2)}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_extraccion_inventario():
    """Prueba la extracción de inventario"""
    print("\n" + "="*50)
    print("TEST: Extracción Inventario (hoy)")
    print("="*50)
    
    fecha = datetime.now().strftime('%Y-%m-%d')
    
    try:
        resultado = handler_inventario({
            'fecha': fecha
        }, None)
        
        print(f"✅ Extracción completada")
        print(f"   Resultado: {json.dumps(json.loads(resultado['body']), indent=2)}")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("\n" + "#"*60)
    print("# DATA COLLECTOR - PRUEBAS LOCALES")
    print("#"*60)
    
    # Verificar password
    if DB_CONFIG['password'] == 'TU_PASSWORD_AQUI':
        print("\n⚠️  ADVERTENCIA: Debes configurar el password de la BD")
        print("   Edita este archivo y cambia 'TU_PASSWORD_AQUI'")
        return
    
    tests = [
        ("Conexión BD", test_conexion_bd),
        ("Conexión API Ventas", test_conexion_api_ventas),
        ("Conexión API Inventario", test_conexion_api_inventario),
    ]
    
    resultados = []
    for nombre, test_func in tests:
        try:
            resultado = test_func()
            resultados.append((nombre, resultado))
        except Exception as e:
            print(f"❌ Error en {nombre}: {e}")
            resultados.append((nombre, False))
    
    # Resumen
    print("\n" + "="*50)
    print("RESUMEN")
    print("="*50)
    for nombre, exito in resultados:
        status = "✅" if exito else "❌"
        print(f"  {status} {nombre}")
    
    # Si las conexiones funcionan, preguntar si quiere cargar datos
    if all(r[1] for r in resultados):
        print("\n" + "-"*50)
        respuesta = input("¿Deseas ejecutar la carga de datos? (s/n): ")
        if respuesta.lower() == 's':
            print("\nEjecutando carga de ventas...")
            test_extraccion_ventas()
            
            print("\nEjecutando carga de inventario...")
            test_extraccion_inventario()


if __name__ == "__main__":
    main()
