"""
Script para actualizar datos de estacionamiento y mantener histórico.
Puede ejecutarse manualmente o programarse con cron/Task Scheduler.

Guarda datos en:
1. Base de datos PostgreSQL/SQLite (fuente principal)
2. Archivos CSV (backup)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Database imports
try:
    from database.connection import init_db, session_scope, get_db_info
    from database.repository import ParkingRepository
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# Configuracion
CITY = "Basel"
API_URL = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100088/records?select=published%2Clast_downloaded%2Cgeo_point_2d%2Cname%2Ctotal%2Cfree%2Cstatus%2Cid%2Caddress%2Clot_type%2Clink&limit=100&lang=de&timezone=Europe%2FZurich"

# Carpetas
RAW_DIR = Path("data/raw") / CITY
PROCESSED_DIR = Path("data/processed")
HISTORICAL_DIR = Path("data/historical")

# Crear carpetas si no existen
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)

# Archivos
CURRENT_FILE = PROCESSED_DIR / f"{CITY}_parking.csv"
HISTORICAL_FILE = HISTORICAL_DIR / f"{CITY}_parking_history.csv"


def fetch_fresh_data():
    """Obtiene datos frescos de la API de Basel."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Obteniendo datos frescos de {CITY}...")
    
    try:
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Guardar JSON crudo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = RAW_DIR / f"parking_{timestamp}.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"  Datos crudos guardados en {raw_file}")
        return data
        
    except requests.RequestException as e:
        print(f"  Error al obtener datos: {e}")
        return None


def process_data(data):
    """Procesa los datos crudos y retorna un DataFrame."""
    records = data.get("results", [])
    
    if not records:
        print("  No se encontraron registros")
        return None
    
    df = pd.DataFrame(records)
    
    # Convertir tipos
    df["total"] = pd.to_numeric(df.get("total"), errors="coerce")
    df["free"] = pd.to_numeric(df.get("free"), errors="coerce")
    
    # Calcular ocupación
    df["occupied"] = df.apply(
        lambda row: row["total"] - row["free"] if pd.notnull(row["total"]) else None, 
        axis=1
    )
    df["occupancy_pct"] = df.apply(
        lambda row: (row["occupied"] / row["total"] * 100) if pd.notnull(row["total"]) and row["total"] > 0 else None, 
        axis=1
    )
    
    # Renombrar y seleccionar columnas
    df = df.rename(columns={
        "name": "parking_name",
        "total": "capacity",
        "free": "free_spaces",
        "status": "status",
        "published": "timestamp",
        "link": "url",
        "geo_point_2d": "coords",
        "address": "address",
        "lot_type": "lot_type"
    })
    
    columns = ["parking_name", "capacity", "free_spaces", "occupied", "occupancy_pct", 
               "status", "timestamp", "address", "lot_type", "url", "coords"]
    
    return df[[c for c in columns if c in df.columns]]


def save_current(df):
    """Guarda los datos actuales (sobrescribe)."""
    df.to_csv(CURRENT_FILE, index=False)
    print(f"  Datos actuales guardados en {CURRENT_FILE}")


def append_to_history(df):
    """Agrega los datos al archivo histórico."""
    # Agregar timestamp de captura
    df = df.copy()
    df['captured_at'] = datetime.now().isoformat()
    
    if HISTORICAL_FILE.exists():
        # Cargar histórico existente
        historical_df = pd.read_csv(HISTORICAL_FILE)
        # Concatenar
        combined_df = pd.concat([historical_df, df], ignore_index=True)
        # Eliminar duplicados exactos (mismo parking, mismo timestamp original)
        combined_df = combined_df.drop_duplicates(subset=['parking_name', 'timestamp'], keep='last')
    else:
        combined_df = df
    
    combined_df.to_csv(HISTORICAL_FILE, index=False)
    n_records = len(combined_df)
    print(f"  Histórico actualizado: {n_records} registros totales en {HISTORICAL_FILE}")
    
    return n_records


def get_historical_stats():
    """Obtiene estadísticas del archivo histórico."""
    if not HISTORICAL_FILE.exists():
        return None
    
    df = pd.read_csv(HISTORICAL_FILE)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    stats = {
        'total_records': len(df),
        'unique_parkings': df['parking_name'].nunique(),
        'date_range_start': df['timestamp'].min().strftime('%Y-%m-%d %H:%M'),
        'date_range_end': df['timestamp'].max().strftime('%Y-%m-%d %H:%M'),
        'unique_timestamps': df['timestamp'].nunique()
    }
    
    return stats


def update_all():
    """Ejecuta todo el proceso de actualización."""
    print("=" * 60)
    print("ACTUALIZACIÓN DE DATOS DE ESTACIONAMIENTO")
    print("=" * 60)
    
    # 1. Obtener datos frescos
    data = fetch_fresh_data()
    if data is None:
        return False
    
    # 2. Procesar datos
    df = process_data(data)
    if df is None:
        return False
    
    print(f"  Procesados {len(df)} estacionamientos")
    
    # 3. Guardar en base de datos (si esta disponible)
    if DB_AVAILABLE:
        save_to_database(data, df)
    
    # 4. Guardar datos actuales en CSV (backup)
    save_current(df)
    
    # 5. Agregar al historico CSV (backup)
    n_historical = append_to_history(df)
    
    # 6. Mostrar estadisticas
    if DB_AVAILABLE:
        show_db_stats()
    else:
        stats = get_historical_stats()
        if stats:
            print("\n[ESTADISTICAS DEL HISTORICO - CSV]")
            print(f"  - Registros totales: {stats['total_records']}")
            print(f"  - Estacionamientos unicos: {stats['unique_parkings']}")
            print(f"  - Timestamps unicos: {stats['unique_timestamps']}")
            print(f"  - Rango de fechas: {stats['date_range_start']} a {stats['date_range_end']}")
    
    print("\n[OK] Actualizacion completada exitosamente")
    print("=" * 60)
    
    return True


def save_to_database(raw_data: dict, processed_df: pd.DataFrame):
    """Guarda los datos en la base de datos."""
    print("\n  [DB] Guardando en base de datos...")
    
    init_db()
    
    with session_scope() as session:
        records = raw_data.get('results', [])
        
        for record in records:
            parking_name = record.get('name')
            
            # Parse coordinates
            coords = record.get('geo_point_2d', {})
            lat = coords.get('lat') if coords else None
            lon = coords.get('lon') if coords else None
            
            # Upsert location
            location = ParkingRepository.upsert_location(
                session,
                city=CITY,
                parking_name=parking_name,
                address=record.get('address'),
                lot_type=record.get('lot_type'),
                capacity=record.get('total'),
                latitude=lat,
                longitude=lon,
                url=record.get('link'),
                external_id=record.get('id')
            )
            
            source_ts = pd.to_datetime(record.get('published'))
            
            # Insert raw data
            ParkingRepository.insert_raw_data(
                session,
                location_id=location.id,
                city=CITY,
                source_timestamp=source_ts,
                raw_json=record,
                free_spaces=record.get('free'),
                total_spaces=record.get('total'),
                status=record.get('status')
            )
            
            # Calculate processed values
            total = record.get('total')
            free = record.get('free')
            occupied = (total - free) if total is not None and free is not None else None
            occupancy_pct = (occupied / total * 100) if total and total > 0 and occupied is not None else None
            
            # Insert processed data
            ParkingRepository.insert_processed_data(
                session,
                location_id=location.id,
                city=CITY,
                parking_name=parking_name,
                timestamp=source_ts,
                capacity=total,
                free_spaces=free,
                occupied=occupied,
                occupancy_pct=occupancy_pct,
                status=record.get('status')
            )
        
        print(f"  [DB] {len(records)} registros guardados")


def show_db_stats():
    """Muestra estadisticas de la base de datos."""
    db_info = get_db_info()
    print(f"\n[ESTADISTICAS - {db_info['type']}]")
    
    with session_scope() as session:
        stats = ParkingRepository.get_historical_stats(session, CITY)
        if stats:
            print(f"  - Registros totales: {stats['total_records']}")
            print(f"  - Estacionamientos unicos: {stats['unique_parkings']}")
            print(f"  - Timestamps unicos: {stats['unique_timestamps']}")
            print(f"  - Rango: {stats['date_start']} a {stats['date_end']}")


if __name__ == "__main__":
    success = update_all()
    sys.exit(0 if success else 1)
