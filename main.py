from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Optional

# PostgreSQL database configuration
DATABASE_URL = "postgresql+asyncpg://avnadmin:AVNS_CQ0Vx8KQLHmD510CGlx@pg-4d7e844-sandeepsingh540-42b3.j.aivencloud.com:26982/lastminz_user?ssl=require"

# Initialize FastAPI app
app = FastAPI()
Base = declarative_base()

# Create the async database engine and session maker
engine = create_async_engine(DATABASE_URL, echo=True)
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Track active WebSocket connections
active_location_connections: Dict[str, WebSocket] = {}


# Define the RiderLocation database model
class RiderLocation(Base):
    __tablename__ = "rider_gps_location"
    rider_id = Column(String(10), primary_key=True, index=True)
    current_latitude = Column(Float, nullable=True)
    current_longitude = Column(Float, nullable=True)
    status = Column(String(20), nullable=True)
    last_updated = Column(TIMESTAMP, default=datetime.utcnow, nullable=False)


# Pydantic model to validate incoming WebSocket data
class LocationData(BaseModel):
    rider_id: str
    current_latitude: float
    current_longitude: float
    status: Optional[str] = "Available"  # Default status


# Initialize the database
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.on_event("startup")
async def startup():
    await init_db()


# WebSocket for rider location updates
@app.websocket("/ws/rider_location")
async def websocket_rider_location(websocket: WebSocket):
    await websocket.accept()
    print("Connected to /ws/rider_location WebSocket")

    # Notify the client about successful connection
    await websocket.send_json({"message": "RIDER_GPS_CONNECTION"})

    rider_id = None
    try:
        while True:
            # Receive JSON data from WebSocket
            data = await websocket.receive_json()
            location_data = LocationData(**data)
            rider_id = location_data.rider_id

            # Insert or update rider location in the database
            async with async_session() as session:
                existing_location = await session.get(RiderLocation, location_data.rider_id)
                if existing_location:
                    # Update existing record
                    existing_location.current_latitude = location_data.current_latitude
                    existing_location.current_longitude = location_data.current_longitude
                    existing_location.status = location_data.status
                    existing_location.last_updated = datetime.utcnow()
                else:
                    # Insert new record
                    new_location = RiderLocation(
                        rider_id=location_data.rider_id,
                        current_latitude=location_data.current_latitude,
                        current_longitude=location_data.current_longitude,
                        status=location_data.status,
                        last_updated=datetime.utcnow()
                    )
                    session.add(new_location)

                # Commit changes
                await session.commit()

            # Track active WebSocket connections
            active_location_connections[location_data.rider_id] = websocket

            # Notify the client of successful GPS data receipt
            await websocket.send_json({"message": "RIDER_GPS_RECEIVED"})
            print(f"Location data for rider_id {rider_id} processed successfully.")

    except WebSocketDisconnect:
        # Handle WebSocket disconnection
        if rider_id and rider_id in active_location_connections:
            del active_location_connections[rider_id]
            print(f"Rider {rider_id} disconnected.")

    except Exception as e:
        # Handle any other errors
        print(f"Error updating GPS data for rider {rider_id}: {e}")
        await websocket.send_json({"message": "GPS_FAILED_UPDATION"})
