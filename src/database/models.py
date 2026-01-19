"""
SQLAlchemy models for the parking data platform.

Tables:
- parking_locations: Master table with parking lot metadata
- raw_parking_data: Raw API responses (JSON)
- processed_parking_data: Cleaned and transformed data
- predictions: ML model predictions
"""
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, JSON, 
    ForeignKey, Text, Boolean, Index, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class ParkingLocation(Base):
    """
    Master table for parking locations.
    Contains static information about each parking facility.
    """
    __tablename__ = "parking_locations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False, index=True)
    parking_name = Column(String(200), nullable=False)
    address = Column(String(500))
    lot_type = Column(String(100))
    capacity = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    url = Column(String(500))
    external_id = Column(String(100))  # ID from source API
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    raw_data = relationship("RawParkingData", back_populates="location")
    processed_data = relationship("ProcessedParkingData", back_populates="location")
    predictions = relationship("Prediction", back_populates="location")
    
    __table_args__ = (
        UniqueConstraint('city', 'parking_name', name='uq_city_parking'),
        Index('idx_city_active', 'city', 'is_active'),
    )
    
    def __repr__(self):
        return f"<ParkingLocation(id={self.id}, name='{self.parking_name}', city='{self.city}')>"


class RawParkingData(Base):
    """
    Raw data from API responses.
    Stores the original JSON for auditing and reprocessing.
    """
    __tablename__ = "raw_parking_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("parking_locations.id"), nullable=False, index=True)
    city = Column(String(100), nullable=False, index=True)
    source_timestamp = Column(DateTime, nullable=False)  # Timestamp from API
    captured_at = Column(DateTime, default=datetime.utcnow, index=True)  # When we fetched it
    raw_json = Column(JSON)  # Original API response
    free_spaces = Column(Integer)
    total_spaces = Column(Integer)
    status = Column(String(50))
    
    # Relationship
    location = relationship("ParkingLocation", back_populates="raw_data")
    
    __table_args__ = (
        Index('idx_raw_city_timestamp', 'city', 'captured_at'),
        Index('idx_raw_location_timestamp', 'location_id', 'captured_at'),
    )
    
    def __repr__(self):
        return f"<RawParkingData(id={self.id}, location_id={self.location_id}, captured_at='{self.captured_at}')>"


class ProcessedParkingData(Base):
    """
    Processed and cleaned parking data.
    This is the main table for analytics and ML training.
    """
    __tablename__ = "processed_parking_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("parking_locations.id"), nullable=False, index=True)
    city = Column(String(100), nullable=False, index=True)
    parking_name = Column(String(200), nullable=False)
    
    # Temporal features
    timestamp = Column(DateTime, nullable=False, index=True)
    hour = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)  # 0=Monday, 6=Sunday
    is_weekend = Column(Boolean, nullable=False)
    
    # Parking metrics
    capacity = Column(Integer)
    free_spaces = Column(Integer)
    occupied = Column(Integer)
    occupancy_pct = Column(Float)
    
    # Status
    status = Column(String(50))
    is_open = Column(Boolean)
    
    # Metadata
    captured_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    location = relationship("ParkingLocation", back_populates="processed_data")
    
    __table_args__ = (
        Index('idx_proc_city_timestamp', 'city', 'timestamp'),
        Index('idx_proc_location_timestamp', 'location_id', 'timestamp'),
        Index('idx_proc_training', 'city', 'hour', 'day_of_week'),  # For ML queries
    )
    
    def __repr__(self):
        return f"<ProcessedParkingData(id={self.id}, parking='{self.parking_name}', timestamp='{self.timestamp}')>"


class Prediction(Base):
    """
    ML model predictions.
    Stores predictions for tracking accuracy and auditing.
    """
    __tablename__ = "predictions"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("parking_locations.id"), nullable=False, index=True)
    city = Column(String(100), nullable=False, index=True)
    parking_name = Column(String(200), nullable=False)
    
    # Prediction details
    predicted_at = Column(DateTime, default=datetime.utcnow, index=True)
    target_datetime = Column(DateTime, nullable=False)
    
    # Predicted values
    predicted_occupied = Column(Integer, nullable=False)
    predicted_occupancy_pct = Column(Float)
    capacity = Column(Integer)
    
    # Model metadata
    model_version = Column(String(50))
    model_mae = Column(Float)  # Model's MAE at prediction time
    
    # Actual values (filled later for accuracy tracking)
    actual_occupied = Column(Integer)
    actual_occupancy_pct = Column(Float)
    prediction_error = Column(Float)  # Calculated after actual is known
    
    # Relationship
    location = relationship("ParkingLocation", back_populates="predictions")
    
    __table_args__ = (
        Index('idx_pred_location_target', 'location_id', 'target_datetime'),
        Index('idx_pred_accuracy', 'city', 'predicted_at', 'prediction_error'),
    )
    
    def __repr__(self):
        return f"<Prediction(id={self.id}, parking='{self.parking_name}', target='{self.target_datetime}')>"


class ModelMetadata(Base):
    """
    Track ML model versions and performance.
    """
    __tablename__ = "model_metadata"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    version = Column(String(50), unique=True, nullable=False)
    trained_at = Column(DateTime, default=datetime.utcnow)
    
    # Training info
    n_samples = Column(Integer)
    features = Column(JSON)
    is_synthetic = Column(Boolean)
    
    # Performance metrics
    mae = Column(Float)
    rmse = Column(Float)
    r2_score = Column(Float)
    
    # Model path
    model_path = Column(String(500))
    is_active = Column(Boolean, default=False)  # Currently deployed model
    
    notes = Column(Text)
    
    def __repr__(self):
        return f"<ModelMetadata(version='{self.version}', mae={self.mae}, active={self.is_active})>"
