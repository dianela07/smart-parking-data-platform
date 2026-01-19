import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import joblib
import os
from pathlib import Path

# Crear carpeta 'models' si no existe
os.makedirs("models", exist_ok=True)

# Archivos de datos
CURRENT_FILE = Path("data/processed/Basel_parking.csv")
HISTORICAL_FILE = Path("data/historical/Basel_parking_history.csv")


def load_training_data():
    """
    Carga datos para entrenamiento.
    Prioriza datos históricos si existen, sino genera datos sintéticos.
    """
    if HISTORICAL_FILE.exists():
        print("Cargando datos historicos reales...")
        df = pd.read_csv(HISTORICAL_FILE)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filtrar registros con datos válidos
        df = df[df['occupied'].notnull() & df['capacity'].notnull()]
        df = df[df['capacity'] > 0]
        
        if len(df) >= 50:  # Mínimo de datos para entrenar
            print(f"  [OK] {len(df)} registros historicos cargados")
            df['hour'] = df['timestamp'].dt.hour
            df['weekday'] = df['timestamp'].dt.weekday
            return df, False
        else:
            print(f"  [AVISO] Solo {len(df)} registros, insuficientes. Generando datos sinteticos...")
    else:
        print("No hay datos historicos. Generando datos sinteticos...")
    
    # Cargar datos actuales como base para sintéticos
    df = pd.read_csv(CURRENT_FILE)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['occupied'].notnull() & df['capacity'].notnull()]
    
    return df, True


def generate_synthetic_data(df, n_samples_per_parking=200):
    """
    Genera datos sintéticos basados en patrones típicos de estacionamiento:
    - Mayor ocupación en horas laborales (8-18h)
    - Menor ocupación en noches y fines de semana
    """
    print(f"  Generando {n_samples_per_parking} muestras por estacionamiento...")
    synthetic_data = []
    
    for _, row in df.iterrows():
        parking_name = row['parking_name']
        capacity = row['capacity']
        
        if pd.isna(capacity) or capacity <= 0:
            continue
            
        base_occupancy = row['occupied'] / capacity if capacity > 0 else 0.5
        
        for _ in range(n_samples_per_parking):
            hour = np.random.randint(0, 24)
            weekday = np.random.randint(0, 7)
            
            # Patrón de ocupación basado en hora y día
            if 8 <= hour <= 10 or 17 <= hour <= 19:
                hour_factor = 1.2  # Horas pico
            elif 10 < hour < 17:
                hour_factor = 1.0  # Horario laboral
            elif 6 <= hour < 8 or 19 <= hour <= 22:
                hour_factor = 0.7  # Mañana/noche temprana
            else:
                hour_factor = 0.3  # Noche
            
            # Fines de semana tienen menos ocupación
            day_factor = 0.6 if weekday >= 5 else 1.0
            
            # Calcular ocupación con variación aleatoria
            occupancy_rate = base_occupancy * hour_factor * day_factor
            occupancy_rate += np.random.normal(0, 0.1)
            occupancy_rate = np.clip(occupancy_rate, 0.05, 0.95)
            
            occupied = int(occupancy_rate * capacity)
            
            synthetic_data.append({
                'parking_name': parking_name,
                'hour': hour,
                'weekday': weekday,
                'capacity': capacity,
                'occupied': occupied
            })
    
    return pd.DataFrame(synthetic_data)


def train_model():
    """Entrena el modelo de predicción."""
    print("=" * 60)
    print("ENTRENAMIENTO DEL MODELO DE PREDICCIÓN")
    print("=" * 60)
    
    # Cargar datos
    df, needs_synthetic = load_training_data()
    
    if needs_synthetic:
        df = generate_synthetic_data(df)
        print(f"  [OK] {len(df)} muestras sinteticas generadas")
    else:
        df['hour'] = df['timestamp'].dt.hour
        df['weekday'] = df['timestamp'].dt.weekday
    
    # Features y target
    X = df[['hour', 'weekday', 'capacity']]
    y = df['occupied']
    
    print(f"\nDatos de entrenamiento:")
    print(f"  - Total muestras: {len(X)}")
    print(f"  - Features: hour, weekday, capacity")
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Entrenar modelo
    print("\nEntrenando RandomForest...")
    model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    # Evaluacion
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    print(f"  [OK] MAE (Error Absoluto Medio): {mae:.2f} espacios")
    
    # Guardar modelo
    model_path = "models/basel_parking_model.pkl"
    joblib.dump(model, model_path)
    print(f"\nModelo guardado en {model_path}")
    
    # Guardar metadata
    metadata = {
        'trained_at': pd.Timestamp.now().isoformat(),
        'n_samples': len(X),
        'mae': mae,
        'is_synthetic': needs_synthetic,
        'features': ['hour', 'weekday', 'capacity']
    }
    joblib.dump(metadata, "models/model_metadata.pkl")
    
    print("=" * 60)
    return model, mae


if __name__ == "__main__":
    train_model()
