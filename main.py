from fastapi import FastAPI, status, HTTPException, Request
from fastapi.templating import Jinja2Templates 
from fastapi.responses import HTMLResponse, RedirectResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv
import os
import asyncio 
import pytz 

# --- Configuration ---
load_dotenv()

MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = "EmoGoDB"
COUNTER_COLLECTION = "counters"

app = FastAPI(title="EmoGo Backend API (Final Version)")

# Initialize Jinja2 Templates
templates = Jinja2Templates(directory="templates")

# --- Custom Jinja2 Filter for Date Formatting ---
def date_format_filter(value):
    """Converts datetime objects to a readable string format (Asia/Taipei)."""
    if isinstance(value, datetime):
        # Convert to UTC then to Taipei time
        if value.tzinfo is None:
            value = value.replace(tzinfo=pytz.utc)
        tw_dt = value.astimezone(pytz.timezone('Asia/Taipei'))
        return tw_dt.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)
templates.env.filters['date_format'] = date_format_filter


# Helper function to format MongoDB data
def serialize_mongodb_data(doc_list: List[dict]) -> List[dict]:
    for doc in doc_list:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        if doc.get("timestamp") and isinstance(doc["timestamp"], str):
             try:
                 doc["timestamp"] = datetime.fromisoformat(doc["timestamp"].replace('Z', '+00:00'))
             except ValueError:
                 pass 
    return doc_list

async def get_next_sequence_value(collection_name: str) -> int:
    """Atomically increments the counter and returns the new value."""
    result = await app.mongodb[COUNTER_COLLECTION].find_one_and_update(
        {"_id": collection_name},
        {"$inc": {"seq": 1}},
        upsert=True, 
        return_document=True 
    )
    if result is None or "seq" not in result:
        raise Exception("MongoDB failed to return valid sequence value after attempt to increment.")
    return result["seq"]


# ====================================================================
# I. Pydantic Data Model (Unified Schema)
# ====================================================================

class EmoGoData(BaseModel):
    NAME: str = Field(..., description="User Name")
    TIME: str = Field(..., description="Timestamp of the record (ISO 8601 string)")
    SENTIMENT: str = Field(..., description="Categorical sentiment (e.g., 'Good', 'Very Bad')")
    MOOD_SCORE: int = Field(..., description="Numeric mood score (0-100)")
    LAT: float = Field(..., description="Latitude")
    LON: float = Field(..., description="Longitude")
    VIDEO_LINK: str = Field(..., description="URL of the video uploaded to cloud storage")
    ACTIVITY: Optional[str] = Field(None, description="Activity description")
    
    class Config:
        extra = "allow"


# ====================================================================
# II. Application Lifecycle
# ====================================================================

@app.on_event("startup")
async def startup_db_client():
    try:
        app.mongodb_client = AsyncIOMotorClient(MONGODB_URI)
        app.mongodb = app.mongodb_client.get_database(DB_NAME)
        print("Database connected successfully!")
    except Exception as e:
        print(f"Database connection failed: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    app.mongodb_client.close()
    print("Database connection closed.")


# ====================================================================
# III. Data Collection API
# ====================================================================

@app.post("/api/v1/data/all", status_code=status.HTTP_201_CREATED, tags=["Data Collection"])
async def create_all_data(data: EmoGoData):
    data_dict = data.model_dump(by_alias=True)
    
    try:
        entry_id = await get_next_sequence_value("emogo_entry_id")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate sequential ID: {e}")
    
    # ? FIX: Change ID length to 4 digits (e.g., 0001) as requested
    entry_id_str = str(entry_id).zfill(4) 
    
    # --- 1. GPS Collection ---
    gps_doc = {
        "entry_id": entry_id_str, 
        "user_name": data_dict.get("NAME"),
        "timestamp": data_dict.get("TIME"),
        "latitude": data_dict.get("LAT"),
        "longitude": data_dict.get("LON"),
    }
    await app.mongodb["gps_coordinates"].insert_one(gps_doc)
    
    # --- 2. Sentiments Collection ---
    sentiment_doc = {
        "entry_id": entry_id_str, 
        "user_name": data_dict.get("NAME"),
        "timestamp": data_dict.get("TIME"),
        "sentiment_category": data_dict.get("SENTIMENT"),
        "mood_score": data_dict.get("MOOD_SCORE"),
        "activity": data_dict.get("ACTIVITY"), 
    }
    await app.mongodb["sentiments"].insert_one(sentiment_doc)
    
    # --- 3. Vlogs Collection ---
    vlog_doc = {
        "entry_id": entry_id_str, 
        "user_name": data_dict.get("NAME"),
        "timestamp": data_dict.get("TIME"),
        "video_url": data_dict.get("VIDEO_LINK"),
    }
    await app.mongodb["vlogs"].insert_one(vlog_doc)
    
    return {
        "message": "Data saved to 3 collections successfully",
        "entry_id": entry_id_str, 
    }

@app.get("/data/download/json", tags=["Data Export (Download)"])
async def download_all_json():
    sentiments_cursor = app.mongodb["sentiments"].find().to_list(1000)
    gps_cursor = app.mongodb["gps_coordinates"].find().to_list(1000)
    vlogs_cursor = app.mongodb["vlogs"].find().to_list(1000)

    sentiments_data, gps_data, vlogs_data = await asyncio.gather(
        sentiments_cursor, gps_cursor, vlogs_cursor
    )

    sentiments_data = serialize_mongodb_data(sentiments_data)
    gps_data = serialize_mongodb_data(gps_data)
    vlogs_data = serialize_mongodb_data(vlogs_data)
    
    # Filter incomplete data
    sentiments_data = [d for d in sentiments_data if d.get("entry_id")]
    gps_data = [d for d in gps_data if d.get("entry_id")]
    vlogs_data = [d for d in vlogs_data if d.get("entry_id")]
    
    sentiments_data.sort(key=lambda x: x.get('entry_id', ''))
    gps_data.sort(key=lambda x: x.get('entry_id', ''))
    vlogs_data.sort(key=lambda x: x.get('entry_id', ''))

    # Use Taipei time for export metadata
    tw_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "export_time": tw_time,
        "sentiments": sentiments_data,
        "gps": gps_data,
        "vlogs": vlogs_data,
    }


# ====================================================================
# IV. Data Export API (HTML Dashboard)
# ====================================================================

@app.get("/data/export", response_class=HTMLResponse, tags=["Data Export (Required)"])
async def export_all_data(request: Request):
    sentiments_cursor = app.mongodb["sentiments"].find().to_list(1000)
    gps_cursor = app.mongodb["gps_coordinates"].find().to_list(1000)
    vlogs_cursor = app.mongodb["vlogs"].find().to_list(1000)

    sentiments_data, gps_data, vlogs_data = await asyncio.gather(
        sentiments_cursor, gps_cursor, vlogs_cursor
    )

    sentiments_data = serialize_mongodb_data(sentiments_data)
    gps_data = serialize_mongodb_data(gps_data)
    vlogs_data = serialize_mongodb_data(vlogs_data)
    
    sentiments_data = [d for d in sentiments_data if d.get("entry_id")]
    gps_data = [d for d in gps_data if d.get("entry_id")]
    vlogs_data = [d for d in vlogs_data if d.get("entry_id")]

    sentiments_data.sort(key=lambda x: x.get('entry_id', ''))
    gps_data.sort(key=lambda x: x.get('entry_id', ''))
    vlogs_data.sort(key=lambda x: x.get('entry_id', ''))

    tw_time = datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S")

    return templates.TemplateResponse(
        "dashboard.html", 
        {
            "request": request,
            "sentiments": sentiments_data,
            "gps": gps_data,
            "vlogs": vlogs_data,
            "current_time": tw_time
        }
    )


# ====================================================================
# V. Health Check
# ====================================================================

@app.get("/", tags=["Health Check"])
async def health_check():
    return {"status": "ok", "service": "EmoGo Backend", "version": "v1"}

# ====================================================================
# VI. DATA MANAGEMENT ROUTES
# ====================================================================

@app.delete("/api/v1/data/clear-all", tags=["Admin/Testing"])
async def clear_all_data():
    try:
        await app.mongodb["sentiments"].delete_many({})
        await app.mongodb["gps_coordinates"].delete_many({})
        await app.mongodb["vlogs"].delete_many({})
        await app.mongodb[COUNTER_COLLECTION].delete_many({})
        
        return {
            "message": "All data cleared successfully.",
            "deleted_collections": ["sentiments", "gps_coordinates", "vlogs"],
            "counter_status": "reset"
        }
    except Exception as e:
        print(f"Error during data clear: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear data: {e}")

# ? NEW: Delete Single Entry (Synced with Frontend)
@app.delete("/api/v1/data/entry", tags=["Data Collection"])
async def delete_single_entry(user_name: str, timestamp: str):
    """
    Deletes a specific entry based on user_name and timestamp.
    Also decrements the sequence counter by 1.
    """
    query = {"user_name": user_name, "timestamp": timestamp}
    
    try:
        r1 = await app.mongodb["sentiments"].delete_one(query)
        r2 = await app.mongodb["gps_coordinates"].delete_one(query)
        r3 = await app.mongodb["vlogs"].delete_one(query)
        
        if r1.deleted_count == 0 and r2.deleted_count == 0 and r3.deleted_count == 0:
             raise HTTPException(status_code=404, detail="Entry not found in backend")
        
        # ? Decrement sequence counter by 1 on successful delete
        await app.mongodb[COUNTER_COLLECTION].update_one(
            {"_id": "emogo_entry_id"},
            {"$inc": {"seq": -1}}
        )

        return {"message": "Entry deleted successfully from backend and counter updated"}
        
    except Exception as e:
        print(f"Error deleting entry: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete entry: {e}")