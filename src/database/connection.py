"""
Database connection configuration using SQLAlchemy.
Supports both PostgreSQL (production) and SQLite (development/testing).
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from contextlib import contextmanager

# Environment variable for database URL
# PostgreSQL: postgresql://user:password@localhost:5432/parking_db
# SQLite (default for dev): sqlite:///data/parking.db
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "sqlite:///data/parking.db"
)

# Create engine based on database type
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=False
    )
else:
    engine = create_engine(
        DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_engine():
    """Get the SQLAlchemy engine."""
    return engine


def get_session() -> Session:
    """Get a new database session."""
    return SessionLocal()


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db():
    """Initialize database tables."""
    from .models import Base
    Base.metadata.create_all(bind=engine)
    print(f"Database initialized: {DATABASE_URL}")


def get_db_info():
    """Get database connection info (without password)."""
    if "postgresql" in DATABASE_URL:
        # Hide password in URL
        import re
        safe_url = re.sub(r':([^:@]+)@', ':****@', DATABASE_URL)
        return {"type": "PostgreSQL", "url": safe_url}
    else:
        return {"type": "SQLite", "url": DATABASE_URL}
