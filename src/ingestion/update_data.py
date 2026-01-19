"""
Script para actualizar datos de estacionamiento y mantener histórico.
Puede ejecutarse manualmente o programarse con cron/Task Scheduler.
"""
import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Configuración
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
    
    # 3. Guardar datos actuales
    save_current(df)
    
    # 4. Agregar al histórico
    n_historical = append_to_history(df)
    
    # 5. Mostrar estadísticas
    stats = get_historical_stats()
    if stats:
        print("\n[ESTADISTICAS DEL HISTORICO]")
        print(f"  - Registros totales: {stats['total_records']}")
        print(f"  - Estacionamientos unicos: {stats['unique_parkings']}")
        print(f"  - Timestamps unicos: {stats['unique_timestamps']}")
        print(f"  - Rango de fechas: {stats['date_range_start']} a {stats['date_range_end']}")
    
    print("\n[OK] Actualizacion completada exitosamente")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    success = update_all()
    sys.exit(0 if success else 1)
