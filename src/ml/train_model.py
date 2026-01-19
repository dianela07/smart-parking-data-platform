import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import joblib
import os

# Crear carpeta 'models' si no existe
os.makedirs("models", exist_ok=True)

# Cargar datos procesados de Basel
df = pd.read_csv("data/processed/Basel_parking.csv")

# Convertir timestamp a datetime y extraer features temporales
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['hour'] = df['timestamp'].dt.hour
df['weekday'] = df['timestamp'].dt.weekday

# Manejar filas con 'occupied' o 'capacity' null
df = df[df['occupied'].notnull() & df['capacity'].notnull()]

# Target: 'occupied'
# Features: hour, weekday, capacity, free_spaces
df['free_spaces'] = pd.to_numeric(df['free_spaces'], errors='coerce')
df = df[df['free_spaces'].notnull()]  # eliminar filas donde free_spaces no es numérico

X = df[['hour', 'weekday', 'capacity', 'free_spaces']]
y = df['occupied']

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Entrenar modelo
model = RandomForestRegressor(n_estimators=200, random_state=42)
model.fit(X_train, y_train)

# Evaluación
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
print(f"MAE: {mae:.2f}")

# Guardar modelo
joblib.dump(model, "models/basel_parking_model.pkl")
print("Modelo guardado en models/basel_parking_model.pkl")
