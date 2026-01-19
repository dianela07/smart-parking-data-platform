from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
from datetime import datetime

app = FastAPI(title="Basel Parking Prediction API")

# Cargar modelo
model = joblib.load("models/basel_parking_model.pkl")

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
    return {"message": "Basel Parking Prediction API"}

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
    pred = max(0, min(capacity, int(pred)))  # asegurar límites
    
    return {
        "garage": garage,
        "datetime": data.datetime,
        "predicted_occupied": pred,
        "capacity": int(capacity),
        "predicted_free": int(capacity - pred)
    }
