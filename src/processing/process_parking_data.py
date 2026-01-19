import json
import pandas as pd
from pathlib import Path

# Configuración
CITY = "Basel"
RAW_DIR = Path(f"data/raw/{CITY}")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_data():
    # Tomar el JSON más reciente
    raw_file = max(RAW_DIR.glob("parking_*.json"), key=lambda f: f.stat().st_mtime)
    
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("results", [])
    
    if not records:
        print(f"[{CITY}] No records found")
        return pd.DataFrame()

    # Crear DataFrame
    df = pd.DataFrame(records)

    # Calcular plazas ocupadas y porcentaje de ocupación
    df["total"] = pd.to_numeric(df.get("total"), errors="coerce")
    df["free"] = pd.to_numeric(df.get("free"), errors="coerce")
    df["occupied"] = df.apply(lambda row: row["total"] - row["free"] if pd.notnull(row["total"]) else None, axis=1)
    df["occupancy_pct"] = df.apply(lambda row: (row["occupied"] / row["total"] * 100) if pd.notnull(row["total"]) else None, axis=1)

    # Seleccionar columnas de interés y renombrar
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
    })[["parking_name", "capacity", "free_spaces", "occupied", "occupancy_pct", "status", "timestamp", "address", "lot_type", "url", "coords"]]

    # Guardar CSV procesado
    processed_file = PROCESSED_DIR / f"{CITY}_parking.csv"
    df.to_csv(processed_file, index=False)
    print(f"Processed data saved to {processed_file}")
    return df

if __name__ == "__main__":
    df = process_data()
    print(df.head())
