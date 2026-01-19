import json
import pandas as pd
from pathlib import Path

# Configuración
CITY = "Aarhus"
RAW_DIR = Path(f"data/raw/{CITY}")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_data():
    # Tomar el JSON más reciente
    raw_file = max(RAW_DIR.glob("parking_*.json"), key=lambda f: f.stat().st_mtime)
    
    with open(raw_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("result", {}).get("records", [])
    
    if not records:
        print(f"[{CITY}] No records found")
        return pd.DataFrame()

    # Crear DataFrame
    df = pd.DataFrame(records)
    
    # Seleccionar y renombrar columnas principales
    df = df.rename(columns={
        "garageCode": "name",
        "totalSpaces": "capacity",
        "vehicleCount": "occupied",
        "date": "timestamp"
    })
    
    # Guardar CSV procesado
    processed_file = PROCESSED_DIR / f"{CITY}_parking.csv"
    df.to_csv(processed_file, index=False)
    print(f"Processed data saved to {processed_file}")
    return df

if __name__ == "__main__":
    df = process_data()
    print(df.head())
