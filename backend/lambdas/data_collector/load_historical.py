#!/usr/bin/env python3
"""
Script para carga histórica de ventas
Carga ventas desde enero 2025 hasta hoy, mes por mes
"""

import os
import sys
from datetime import datetime, timedelta
from calendar import monthrange
import time

# Configurar entorno
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ============================================
# CONFIGURACIÓN - EDITAR ESTOS VALORES
# ============================================

from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    'host': os.environ.get('DB_HOST', ''),
    'port': os.environ.get('DB_PORT', '5432'),
    'name': os.environ.get('DB_NAME', ''),
    'user': os.environ.get('DB_USER', ''),
    'password': os.environ.get('DB_PASSWORD', '')
}

FECHA_INICIO = '2025-01-01'

os.environ['LOCAL_DEV'] = 'true'

from handler import handler_ventas


def generar_rangos_semanales(fecha_inicio: str, fecha_fin: str):
    """
    Genera rangos de fechas semana por semana (7 días)
    """
    inicio = datetime.strptime(fecha_inicio, '%Y-%m-%d')
    fin = datetime.strptime(fecha_fin, '%Y-%m-%d')
    
    rangos = []
    current = inicio
    
    while current <= fin:
        # Fin de la semana (6 días después)
        semana_fin = current + timedelta(days=6)
        
        # No pasar de la fecha fin
        if semana_fin > fin:
            semana_fin = fin
        
        rangos.append({
            'inicio': current.strftime('%Y-%m-%d'),
            'fin': semana_fin.strftime('%Y-%m-%d'),
            'periodo': f"{current.strftime('%Y-%m-%d')} a {semana_fin.strftime('%Y-%m-%d')}"
        })
        
        # Siguiente semana
        current = semana_fin + timedelta(days=1)
    
    return rangos


def main():
    print("\n" + "#" * 60)
    print("# CARGA HISTÓRICA DE VENTAS (SEMANAL)")
    print("#" * 60)
    
    # Verificar password
    if DB_CONFIG['password'] == 'TU_PASSWORD_AQUI':
        print("\n⚠️  ERROR: Debes configurar el password de la BD")
        print("   Edita este archivo y cambia 'TU_PASSWORD_AQUI'")
        return
    
    # Generar rangos semanales
    fecha_fin = datetime.now().strftime('%Y-%m-%d')
    rangos = generar_rangos_semanales(FECHA_INICIO, fecha_fin)
    
    print(f"\n📅 Período: {FECHA_INICIO} a {fecha_fin}")
    print(f"📦 Total de semanas a cargar: {len(rangos)}")
    print("\n" + "-" * 50)
    
    # Confirmar
    respuesta = input("\n¿Deseas continuar con la carga? (s/n): ")
    if respuesta.lower() != 's':
        print("Cancelado.")
        return
    
    # Cargar semana por semana
    total_ventas = 0
    total_productos = 0
    errores = []
    
    for i, rango in enumerate(rangos, 1):
        print(f"\n[{i}/{len(rangos)}] Cargando {rango['periodo']}...")
        
        try:
            resultado = handler_ventas({
                'fecha_inicio': rango['inicio'],
                'fecha_fin': rango['fin']
            }, None)
            
            import json
            body = json.loads(resultado['body'])
            
            if 'error' in body:
                print(f"    ❌ Error: {body['error'][:100]}")
                errores.append(rango['periodo'])
            else:
                ventas = body.get('ventas_insertadas', 0)
                productos = body.get('productos_actualizados', 0)
                total_ventas += ventas
                total_productos += productos
                print(f"    ✅ Ventas: {ventas:,} | Productos: {productos}")
            
            # Pausa para no saturar la API
            time.sleep(1)
            
        except Exception as e:
            print(f"    ❌ Error: {e}")
            errores.append(rango['periodo'])
    
    # Resumen final
    print("\n" + "=" * 60)
    print("RESUMEN FINAL")
    print("=" * 60)
    print(f"  ✅ Total ventas cargadas: {total_ventas:,}")
    print(f"  ✅ Total productos: {total_productos:,}")
    
    if errores:
        print(f"  ❌ Semanas con error: {len(errores)}")
        for e in errores[:5]:  # Mostrar solo las primeras 5
            print(f"      - {e}")
        if len(errores) > 5:
            print(f"      ... y {len(errores) - 5} más")
    else:
        print(f"  ✅ Todas las semanas cargadas correctamente")


if __name__ == "__main__":
    main()
