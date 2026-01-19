"""
Repository pattern for database operations.
Provides clean interface for CRUD operations.
"""
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd
from sqlalchemy import func, and_, desc
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .models import (
    ParkingLocation, 
    RawParkingData, 
    ProcessedParkingData, 
    Prediction,
    ModelMetadata
)
from .connection import session_scope, engine


class ParkingRepository:
    """Repository for parking-related database operations."""
    
    # ─────────────────────────────────────────────────────────────
    # Parking Locations
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def upsert_location(session: Session, city: str, parking_name: str, **kwargs) -> ParkingLocation:
        """Insert or update a parking location."""
        location = session.query(ParkingLocation).filter(
            ParkingLocation.city == city,
            ParkingLocation.parking_name == parking_name
        ).first()
        
        if location:
            for key, value in kwargs.items():
                if hasattr(location, key) and value is not None:
                    setattr(location, key, value)
            location.updated_at = datetime.utcnow()
        else:
            location = ParkingLocation(
                city=city,
                parking_name=parking_name,
                **kwargs
            )
            session.add(location)
        
        session.flush()
        return location
    
    @staticmethod
    def get_location(session: Session, city: str, parking_name: str) -> Optional[ParkingLocation]:
        """Get a parking location by city and name."""
        return session.query(ParkingLocation).filter(
            ParkingLocation.city == city,
            ParkingLocation.parking_name == parking_name
        ).first()
    
    @staticmethod
    def get_all_locations(session: Session, city: Optional[str] = None) -> List[ParkingLocation]:
        """Get all parking locations, optionally filtered by city."""
        query = session.query(ParkingLocation).filter(ParkingLocation.is_active == True)
        if city:
            query = query.filter(ParkingLocation.city == city)
        return query.all()
    
    # ─────────────────────────────────────────────────────────────
    # Raw Data
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def insert_raw_data(session: Session, location_id: int, city: str,
                       source_timestamp: datetime, raw_json: dict,
                       free_spaces: int = None, total_spaces: int = None,
                       status: str = None) -> RawParkingData:
        """Insert raw parking data."""
        raw = RawParkingData(
            location_id=location_id,
            city=city,
            source_timestamp=source_timestamp,
            raw_json=raw_json,
            free_spaces=free_spaces,
            total_spaces=total_spaces,
            status=status
        )
        session.add(raw)
        session.flush()
        return raw
    
    @staticmethod
    def get_latest_raw(session: Session, city: str) -> List[RawParkingData]:
        """Get the most recent raw data for each location in a city."""
        subquery = session.query(
            RawParkingData.location_id,
            func.max(RawParkingData.captured_at).label('max_captured')
        ).filter(RawParkingData.city == city).group_by(RawParkingData.location_id).subquery()
        
        return session.query(RawParkingData).join(
            subquery,
            and_(
                RawParkingData.location_id == subquery.c.location_id,
                RawParkingData.captured_at == subquery.c.max_captured
            )
        ).all()
    
    # ─────────────────────────────────────────────────────────────
    # Processed Data
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def insert_processed_data(session: Session, location_id: int, city: str,
                             parking_name: str, timestamp: datetime,
                             capacity: int = None, free_spaces: int = None,
                             occupied: int = None, occupancy_pct: float = None,
                             status: str = None) -> ProcessedParkingData:
        """Insert processed parking data."""
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        is_weekend = day_of_week >= 5
        is_open = status == 'offen' if status else None
        
        processed = ProcessedParkingData(
            location_id=location_id,
            city=city,
            parking_name=parking_name,
            timestamp=timestamp,
            hour=hour,
            day_of_week=day_of_week,
            is_weekend=is_weekend,
            capacity=capacity,
            free_spaces=free_spaces,
            occupied=occupied,
            occupancy_pct=occupancy_pct,
            status=status,
            is_open=is_open
        )
        session.add(processed)
        session.flush()
        return processed
    
    @staticmethod
    def get_processed_data(session: Session, city: str, 
                          start_date: datetime = None, 
                          end_date: datetime = None) -> List[ProcessedParkingData]:
        """Get processed data with optional date filters."""
        query = session.query(ProcessedParkingData).filter(
            ProcessedParkingData.city == city
        )
        if start_date:
            query = query.filter(ProcessedParkingData.timestamp >= start_date)
        if end_date:
            query = query.filter(ProcessedParkingData.timestamp <= end_date)
        return query.order_by(ProcessedParkingData.timestamp).all()
    
    @staticmethod
    def get_latest_processed(session: Session, city: str) -> List[ProcessedParkingData]:
        """Get the most recent processed data for each location."""
        subquery = session.query(
            ProcessedParkingData.location_id,
            func.max(ProcessedParkingData.timestamp).label('max_ts')
        ).filter(ProcessedParkingData.city == city).group_by(ProcessedParkingData.location_id).subquery()
        
        return session.query(ProcessedParkingData).join(
            subquery,
            and_(
                ProcessedParkingData.location_id == subquery.c.location_id,
                ProcessedParkingData.timestamp == subquery.c.max_ts
            )
        ).all()
    
    @staticmethod
    def get_training_data(session: Session, city: str, min_records: int = 50) -> pd.DataFrame:
        """Get data formatted for ML training."""
        query = session.query(
            ProcessedParkingData.hour,
            ProcessedParkingData.day_of_week,
            ProcessedParkingData.capacity,
            ProcessedParkingData.occupied,
            ProcessedParkingData.parking_name
        ).filter(
            ProcessedParkingData.city == city,
            ProcessedParkingData.occupied.isnot(None),
            ProcessedParkingData.capacity.isnot(None),
            ProcessedParkingData.capacity > 0
        )
        
        df = pd.read_sql(query.statement, session.bind)
        return df
    
    @staticmethod
    def get_historical_stats(session: Session, city: str) -> Dict[str, Any]:
        """Get statistics about historical data."""
        result = session.query(
            func.count(ProcessedParkingData.id).label('total_records'),
            func.count(func.distinct(ProcessedParkingData.parking_name)).label('unique_parkings'),
            func.count(func.distinct(ProcessedParkingData.timestamp)).label('unique_timestamps'),
            func.min(ProcessedParkingData.timestamp).label('date_start'),
            func.max(ProcessedParkingData.timestamp).label('date_end')
        ).filter(ProcessedParkingData.city == city).first()
        
        if result and result.total_records > 0:
            return {
                'total_records': result.total_records,
                'unique_parkings': result.unique_parkings,
                'unique_timestamps': result.unique_timestamps,
                'date_start': result.date_start.strftime('%Y-%m-%d %H:%M') if result.date_start else None,
                'date_end': result.date_end.strftime('%Y-%m-%d %H:%M') if result.date_end else None
            }
        return None
    
    # ─────────────────────────────────────────────────────────────
    # Predictions
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def insert_prediction(session: Session, location_id: int, city: str,
                         parking_name: str, target_datetime: datetime,
                         predicted_occupied: int, capacity: int = None,
                         model_version: str = None, model_mae: float = None) -> Prediction:
        """Insert a prediction."""
        pred_occupancy_pct = (predicted_occupied / capacity * 100) if capacity and capacity > 0 else None
        
        prediction = Prediction(
            location_id=location_id,
            city=city,
            parking_name=parking_name,
            target_datetime=target_datetime,
            predicted_occupied=predicted_occupied,
            predicted_occupancy_pct=pred_occupancy_pct,
            capacity=capacity,
            model_version=model_version,
            model_mae=model_mae
        )
        session.add(prediction)
        session.flush()
        return prediction
    
    @staticmethod
    def update_prediction_actual(session: Session, prediction_id: int,
                                actual_occupied: int) -> Prediction:
        """Update a prediction with actual values for accuracy tracking."""
        prediction = session.query(Prediction).get(prediction_id)
        if prediction:
            prediction.actual_occupied = actual_occupied
            if prediction.capacity and prediction.capacity > 0:
                prediction.actual_occupancy_pct = actual_occupied / prediction.capacity * 100
            prediction.prediction_error = abs(prediction.predicted_occupied - actual_occupied)
        return prediction
    
    @staticmethod
    def get_recent_predictions(session: Session, city: str, hours: int = 24) -> List[Prediction]:
        """Get predictions from the last N hours."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return session.query(Prediction).filter(
            Prediction.city == city,
            Prediction.predicted_at >= cutoff
        ).order_by(desc(Prediction.predicted_at)).all()
    
    # ─────────────────────────────────────────────────────────────
    # Model Metadata
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def save_model_metadata(session: Session, version: str, n_samples: int,
                           features: list, mae: float, is_synthetic: bool,
                           model_path: str, **kwargs) -> ModelMetadata:
        """Save model training metadata."""
        # Deactivate current active model
        session.query(ModelMetadata).filter(
            ModelMetadata.is_active == True
        ).update({'is_active': False})
        
        metadata = ModelMetadata(
            version=version,
            n_samples=n_samples,
            features=features,
            mae=mae,
            is_synthetic=is_synthetic,
            model_path=model_path,
            is_active=True,
            **kwargs
        )
        session.add(metadata)
        session.flush()
        return metadata
    
    @staticmethod
    def get_active_model(session: Session) -> Optional[ModelMetadata]:
        """Get the currently active model."""
        return session.query(ModelMetadata).filter(
            ModelMetadata.is_active == True
        ).first()
    
    @staticmethod
    def processed_to_dataframe(session: Session, city: str) -> pd.DataFrame:
        """Convert processed data to pandas DataFrame."""
        data = ParkingRepository.get_latest_processed(session, city)
        
        records = []
        for d in data:
            records.append({
                'parking_name': d.parking_name,
                'capacity': d.capacity,
                'free_spaces': d.free_spaces,
                'occupied': d.occupied,
                'occupancy_pct': d.occupancy_pct,
                'status': d.status,
                'timestamp': d.timestamp,
                'address': d.location.address if d.location else None,
                'lot_type': d.location.lot_type if d.location else None,
                'url': d.location.url if d.location else None,
                'lat': d.location.latitude if d.location else None,
                'lon': d.location.longitude if d.location else None
            })
        
        return pd.DataFrame(records)
