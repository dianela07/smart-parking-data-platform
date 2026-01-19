import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
from datetime import datetime

# Database imports
try:
    from database.connection import init_db, session_scope
    from database.repository import ParkingRepository
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

app = FastAPI(title="Basel Parking Prediction API")

# Cargar modelo
model = joblib.load("models/basel_parking_model.pkl")

# Cargar metadata del modelo
try:
    model_metadata = joblib.load("models/model_metadata.pkl")
except:
    model_metadata = {'version': 'unknown', 'mae': None}

# Cargar datos de capacidad de garages
df = pd.read_csv("data/processed/Basel_parking.csv")
# Mapeo de capacidad y free_spaces por parking
garage_info = df.set_index('parking_name')[['capacity', 'free_spaces']].to_dict(orient='index')

# Esquema para input
class PredictionRequest(BaseModel):
    garage: str
    datetime: str  # ISO format, e.g., 2026-01-19T15:00:00

@app.get("/")
def root():
    return {
        "message": "Basel Parking Prediction API",
        "model_version": model_metadata.get('version', 'unknown'),
        "model_mae": model_metadata.get('mae'),
        "database": "connected" if DB_AVAILABLE else "not available"
    }

@app.post("/predict")
def predict(data: PredictionRequest):
    garage = data.garage
    dt = datetime.fromisoformat(data.datetime)
    
    if garage not in garage_info:
        return {"error": f"Garage {garage} not found."}
    
    capacity = garage_info[garage]['capacity']
    free_spaces = garage_info[garage]['free_spaces']
    
    hour = dt.hour
    weekday = dt.weekday()
    
    # Preparar features para el modelo (sin free_spaces)
    pred = model.predict([[hour, weekday, capacity]])[0]
    pred = max(0, min(capacity, int(pred)))  # asegurar limites
    
    # Guardar prediccion en base de datos
    if DB_AVAILABLE:
        try:
            with session_scope() as session:
                location = ParkingRepository.get_location(session, "Basel", garage)
                if location:
                    ParkingRepository.insert_prediction(
                        session,
                        location_id=location.id,
                        city="Basel",
                        parking_name=garage,
                        target_datetime=dt,
                        predicted_occupied=pred,
                        capacity=int(capacity) if pd.notna(capacity) else None,
                        model_version=model_metadata.get('version'),
                        model_mae=model_metadata.get('mae')
                    )
        except Exception as e:
            print(f"Error guardando prediccion: {e}")
    
    return {
        "garage": garage,
        "datetime": data.datetime,
        "predicted_occupied": pred,
        "capacity": int(capacity) if pd.notna(capacity) else None,
        "predicted_free": int(capacity - pred) if pd.notna(capacity) else None,
        "model_version": model_metadata.get('version')
    }


@app.get("/health")
def health_check():
    """Endpoint para verificar estado del servicio."""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "database": "connected" if DB_AVAILABLE else "not available",
        "garages_count": len(garage_info)
    }
