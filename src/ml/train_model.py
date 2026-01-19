import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from pathlib import Path
from datetime import datetime

# Database imports
try:
    from database.connection import init_db, session_scope, get_db_info
    from database.repository import ParkingRepository
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# Crear carpeta 'models' si no existe
os.makedirs("models", exist_ok=True)

# Archivos de datos (fallback)
CURRENT_FILE = Path("data/processed/Basel_parking.csv")
HISTORICAL_FILE = Path("data/historical/Basel_parking_history.csv")
CITY = "Basel"


def load_training_data():
    """
    Carga datos para entrenamiento.
    Prioriza: 1) Base de datos, 2) CSV historico, 3) Datos sinteticos
    """
    MIN_RECORDS = 50
    
    # Intentar cargar desde base de datos
    if DB_AVAILABLE:
        try:
            init_db()
            db_info = get_db_info()
            print(f"Conectando a {db_info['type']}...")
            
            with session_scope() as session:
                df = ParkingRepository.get_training_data(session, CITY)
                
                if len(df) >= MIN_RECORDS:
                    print(f"  [OK] {len(df)} registros cargados desde base de datos")
                    df['weekday'] = df['day_of_week'] if 'day_of_week' in df.columns else 0
                    return df, False
                else:
                    print(f"  [AVISO] Solo {len(df)} registros en DB, insuficientes")
        except Exception as e:
            print(f"  [ERROR] No se pudo conectar a DB: {e}")
    
    # Fallback: Cargar desde CSV historico
    if HISTORICAL_FILE.exists():
        print("Cargando datos historicos desde CSV...")
        df = pd.read_csv(HISTORICAL_FILE)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filtrar registros con datos validos
        df = df[df['occupied'].notnull() & df['capacity'].notnull()]
        df = df[df['capacity'] > 0]
        
        if len(df) >= MIN_RECORDS:
            print(f"  [OK] {len(df)} registros historicos cargados")
            df['hour'] = df['timestamp'].dt.hour
            df['weekday'] = df['timestamp'].dt.weekday
            return df, False
        else:
            print(f"  [AVISO] Solo {len(df)} registros, insuficientes. Generando datos sinteticos...")
    else:
        print("No hay datos historicos. Generando datos sinteticos...")
    
    # Fallback final: Generar datos sinteticos
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
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)
    
    print(f"  [OK] MAE: {mae:.2f} espacios")
    print(f"  [OK] RMSE: {rmse:.2f} espacios")
    print(f"  [OK] R2 Score: {r2:.3f}")
    
    # Guardar modelo
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = "models/basel_parking_model.pkl"
    joblib.dump(model, model_path)
    print(f"\nModelo guardado en {model_path}")
    
    # Guardar metadata (archivo)
    metadata = {
        'version': version,
        'trained_at': pd.Timestamp.now().isoformat(),
        'n_samples': len(X),
        'mae': mae,
        'rmse': rmse,
        'r2_score': r2,
        'is_synthetic': needs_synthetic,
        'features': ['hour', 'weekday', 'capacity']
    }
    joblib.dump(metadata, "models/model_metadata.pkl")
    
    # Guardar metadata en DB
    if DB_AVAILABLE:
        try:
            with session_scope() as session:
                ParkingRepository.save_model_metadata(
                    session,
                    version=version,
                    n_samples=len(X),
                    features=['hour', 'weekday', 'capacity'],
                    mae=mae,
                    rmse=rmse,
                    r2_score=r2,
                    is_synthetic=needs_synthetic,
                    model_path=model_path
                )
                print("  [OK] Metadata guardada en base de datos")
        except Exception as e:
            print(f"  [AVISO] No se pudo guardar metadata en DB: {e}")
    
    print("=" * 60)
    return model, mae


if __name__ == "__main__":
    train_model()
