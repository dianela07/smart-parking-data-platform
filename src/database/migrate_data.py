"""
Migration script to import existing CSV data into the database.
Run this once to populate the database with historical data.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pathlib import Path
from datetime import datetime
import ast
import json

from database.connection import init_db, session_scope
from database.models import ParkingLocation, RawParkingData, ProcessedParkingData
from database.repository import ParkingRepository


def parse_coords(coord_str):
    """Parse coordinate string to lat/lon."""
    try:
        coords = ast.literal_eval(coord_str)
        return coords.get('lat'), coords.get('lon')
    except:
        return None, None


def migrate_csv_data(city: str = "Basel"):
    """
    Migrate CSV data to database.
    """
    print("=" * 60)
    print("MIGRACION DE DATOS CSV A BASE DE DATOS")
    print("=" * 60)
    
    # Initialize database
    init_db()
    
    # Paths
    current_file = Path(f"data/processed/{city}_parking.csv")
    historical_file = Path(f"data/historical/{city}_parking_history.csv")
    
    with session_scope() as session:
        # ─────────────────────────────────────────────────────────
        # Step 1: Import current data to create locations
        # ─────────────────────────────────────────────────────────
        if current_file.exists():
            print(f"\n[1/3] Importando datos actuales desde {current_file}...")
            df = pd.read_csv(current_file)
            
            location_map = {}  # parking_name -> location_id
            
            for _, row in df.iterrows():
                lat, lon = parse_coords(str(row.get('coords', '')))
                
                location = ParkingRepository.upsert_location(
                    session, 
                    city=city,
                    parking_name=row['parking_name'],
                    address=row.get('address'),
                    lot_type=row.get('lot_type'),
                    capacity=int(row['capacity']) if pd.notna(row.get('capacity')) else None,
                    latitude=lat,
                    longitude=lon,
                    url=row.get('url')
                )
                location_map[row['parking_name']] = location.id
            
            print(f"  - {len(location_map)} ubicaciones creadas/actualizadas")
            session.commit()
        else:
            print(f"[AVISO] No se encontro {current_file}")
            return
        
        # ─────────────────────────────────────────────────────────
        # Step 2: Import historical data
        # ─────────────────────────────────────────────────────────
        if historical_file.exists():
            print(f"\n[2/3] Importando datos historicos desde {historical_file}...")
            hist_df = pd.read_csv(historical_file)
            hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
            
            # Remove duplicates
            hist_df = hist_df.drop_duplicates(subset=['parking_name', 'timestamp'])
            
            imported = 0
            skipped = 0
            
            for _, row in hist_df.iterrows():
                parking_name = row['parking_name']
                
                if parking_name not in location_map:
                    skipped += 1
                    continue
                
                location_id = location_map[parking_name]
                timestamp = row['timestamp']
                
                # Check if already exists
                existing = session.query(ProcessedParkingData).filter(
                    ProcessedParkingData.location_id == location_id,
                    ProcessedParkingData.timestamp == timestamp
                ).first()
                
                if existing:
                    skipped += 1
                    continue
                
                # Insert processed data
                ParkingRepository.insert_processed_data(
                    session,
                    location_id=location_id,
                    city=city,
                    parking_name=parking_name,
                    timestamp=timestamp,
                    capacity=int(row['capacity']) if pd.notna(row.get('capacity')) else None,
                    free_spaces=int(row['free_spaces']) if pd.notna(row.get('free_spaces')) else None,
                    occupied=int(row['occupied']) if pd.notna(row.get('occupied')) else None,
                    occupancy_pct=float(row['occupancy_pct']) if pd.notna(row.get('occupancy_pct')) else None,
                    status=row.get('status')
                )
                imported += 1
            
            print(f"  - {imported} registros importados")
            print(f"  - {skipped} registros omitidos (duplicados o sin ubicacion)")
            session.commit()
        else:
            print(f"\n[2/3] No se encontro archivo historico {historical_file}")
        
        # ─────────────────────────────────────────────────────────
        # Step 3: Import raw JSON files
        # ─────────────────────────────────────────────────────────
        raw_dir = Path(f"data/raw/{city}")
        if raw_dir.exists():
            print(f"\n[3/3] Importando archivos JSON crudos desde {raw_dir}...")
            json_files = list(raw_dir.glob("parking_*.json"))
            
            imported_raw = 0
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    records = data.get('results', [])
                    
                    for record in records:
                        parking_name = record.get('name')
                        if parking_name not in location_map:
                            continue
                        
                        location_id = location_map[parking_name]
                        source_ts = pd.to_datetime(record.get('published'))
                        
                        # Check if exists
                        existing = session.query(RawParkingData).filter(
                            RawParkingData.location_id == location_id,
                            RawParkingData.source_timestamp == source_ts
                        ).first()
                        
                        if not existing:
                            ParkingRepository.insert_raw_data(
                                session,
                                location_id=location_id,
                                city=city,
                                source_timestamp=source_ts,
                                raw_json=record,
                                free_spaces=record.get('free'),
                                total_spaces=record.get('total'),
                                status=record.get('status')
                            )
                            imported_raw += 1
                
                except Exception as e:
                    print(f"  [ERROR] {json_file}: {e}")
            
            print(f"  - {imported_raw} registros crudos importados")
            session.commit()
        
        # ─────────────────────────────────────────────────────────
        # Summary
        # ─────────────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("RESUMEN DE MIGRACION")
        print("=" * 60)
        
        stats = ParkingRepository.get_historical_stats(session, city)
        if stats:
            print(f"  - Ubicaciones: {len(location_map)}")
            print(f"  - Registros procesados: {stats['total_records']}")
            print(f"  - Timestamps unicos: {stats['unique_timestamps']}")
            print(f"  - Rango: {stats['date_start']} a {stats['date_end']}")
        
        print("\n[OK] Migracion completada")


def migrate_raw_json_files(city: str = "Basel"):
    """
    Migrate only raw JSON files to database (for incremental updates).
    """
    from database.connection import session_scope
    
    raw_dir = Path(f"data/raw/{city}")
    if not raw_dir.exists():
        print(f"No existe el directorio {raw_dir}")
        return
    
    with session_scope() as session:
        # Get location map
        locations = ParkingRepository.get_all_locations(session, city)
        location_map = {loc.parking_name: loc.id for loc in locations}
        
        json_files = sorted(raw_dir.glob("parking_*.json"))
        print(f"Encontrados {len(json_files)} archivos JSON")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                records = data.get('results', [])
                for record in records:
                    parking_name = record.get('name')
                    if parking_name in location_map:
                        source_ts = pd.to_datetime(record.get('published'))
                        
                        existing = session.query(RawParkingData).filter(
                            RawParkingData.location_id == location_map[parking_name],
                            RawParkingData.source_timestamp == source_ts
                        ).first()
                        
                        if not existing:
                            ParkingRepository.insert_raw_data(
                                session,
                                location_id=location_map[parking_name],
                                city=city,
                                source_timestamp=source_ts,
                                raw_json=record,
                                free_spaces=record.get('free'),
                                total_spaces=record.get('total'),
                                status=record.get('status')
                            )
            except Exception as e:
                print(f"Error procesando {json_file}: {e}")
        
        session.commit()
        print("Migracion de JSON completada")


if __name__ == "__main__":
    migrate_csv_data("Basel")
