from fastapi import FastAPI, status, HTTPException, Request
from fastapi.templating import Jinja2Templates 
from fastapi.responses import HTMLResponse, RedirectResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional
import os
import asyncio 

# --- Configuration ---
# IMPORTANT: Replace these with your actual credentials for deployment
MONGODB_URI = os.environ.get("MONGODB_URI")
DB_NAME = "EmoGoDB"
COUNTER_COLLECTION = "counters" # New collection for sequential IDs

app = FastAPI(title="EmoGo Backend API (Final Version)")

# Initialize Jinja2 Templates (HTML files must be in the 'templates' folder)
templates = Jinja2Templates(directory="templates")

# --- Custom Jinja2 Filter for Date Formatting (FIXED REGISTRATION) ---
def date_format_filter(value):
    """Converts datetime objects to a readable string format."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)
templates.env.filters['date_format'] = date_format_filter


# Helper function to format MongoDB data for template rendering
def serialize_mongodb_data(doc_list: List[dict]) -> List[dict]:
    """Converts MongoDB documents for safe template rendering."""
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
# I. Pydantic Data Model (Unified Schema) ? FIX: Unified Activity ?
# ====================================================================

class EmoGoData(BaseModel):
    NAME: str = Field(..., description="User Name")
    TIME: str = Field(..., description="Timestamp of the record (ISO 8601 string)")
    SENTIMENT: str = Field(..., description="Categorical sentiment (e.g., 'Good', 'Very Bad')")
    LAT: float = Field(..., description="Latitude")
    LON: float = Field(..., description="Longitude")
    VIDEO_LINK: str = Field(..., description="URL of the video uploaded to cloud storage")
    ACTIVITY: Optional[str] = Field(None, description="Activity/Journal Text (from frontend step 1).") 
    # Removed JOURNAL_TEXT to avoid confusion, using ACTIVITY for text input
    
    class Config:
        extra = "allow"


# ====================================================================
# II. Application Lifecycle (Connect/Disconnect) (Unchanged)
# ====================================================================

@app.on_event("startup")
async def startup_db_client():
    """Connect to MongoDB when the service starts."""
    try:
        app.mongodb_client = AsyncIOMotorClient(MONGODB_URI)
        app.mongodb = app.mongodb_client.get_database(DB_NAME)
        print("Database connected successfully!")
    except Exception as e:
        print(f"Database connection failed: {e}")

@app.on_event("shutdown")
async def shutdown_db_client():
    """Disconnect from MongoDB when the service shuts down."""
    app.mongodb_client.close()
    print("Database connection closed.")


# ====================================================================
# III. Data Collection API (POST Route - Write Data)
# ====================================================================

@app.post("/api/v1/data/all", status_code=status.HTTP_201_CREATED, tags=["Data Collection"])
async def create_all_data(data: EmoGoData):
    """
    Receives unified data object, generates a sequential entry_id, 
    and saves parts into three separate MongoDB collections.
    """
    data_dict = data.model_dump(by_alias=True)
    
    try:
        entry_id = await get_next_sequence_value("emogo_entry_id")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate sequential ID: {e}")
        
    entry_id_str = str(entry_id).zfill(4) 
    
    # --- 1. GPS Collection: Store coordinates ---
    gps_doc = {
        "entry_id": entry_id_str, 
        "user_name": data_dict.get("NAME"),
        "timestamp": data_dict.get("TIME"),
        "latitude": data_dict.get("LAT"),
        "longitude": data_dict.get("LON"),
    }
    await app.mongodb["gps_coordinates"].insert_one(gps_doc)
    
    # --- 2. Sentiments Collection: Store mood and activity ---
    sentiment_doc = {
        "entry_id": entry_id_str, 
        "user_name": data_dict.get("NAME"),
        "timestamp": data_dict.get("TIME"),
        "sentiment_category": data_dict.get("SENTIMENT"),
        "activity": data_dict.get("ACTIVITY"), 
    }
    await app.mongodb["sentiments"].insert_one(sentiment_doc)
    
    # --- 3. Vlogs Collection: Store video link ---
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
    """
    Provides a simple JSON dump of all collected data for TAs to download.
    """
    
    sentiments_cursor = app.mongodb["sentiments"].find().to_list(1000)
    gps_cursor = app.mongodb["gps_coordinates"].find().to_list(1000)
    vlogs_cursor = app.mongodb["vlogs"].find().to_list(1000)

    sentiments_data, gps_data, vlogs_data = await asyncio.gather(
        sentiments_cursor, gps_cursor, vlogs_cursor
    )

    # Note: We must use the serialization helper here for clean JSON output
    sentiments_data = serialize_mongodb_data(sentiments_data)
    gps_data = serialize_mongodb_data(gps_data)
    vlogs_data = serialize_mongodb_data(vlogs_data)
    
    # Filter and sort data (same as dashboard)
    sentiments_data = [d for d in sentiments_data if d.get("entry_id")]
    gps_data = [d for d in gps_data if d.get("entry_id")]
    vlogs_data = [d for d in vlogs_data if d.get("entry_id")]
    sentiments_data.sort(key=lambda x: x.get('entry_id', ''))
    gps_data.sort(key=lambda x: x.get('entry_id', ''))
    vlogs_data.sort(key=lambda x: x.get('entry_id', ''))

    return {
        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sentiments": sentiments_data,
        "gps": gps_data,
        "vlogs": vlogs_data,
    }

# ====================================================================
# IV. Data Export API (REQUIRED: HTML Dashboard) (Unchanged logic)
# ====================================================================

@app.get("/data/export", response_class=HTMLResponse, tags=["Data Export (Required)"])
async def export_all_data(request: Request):
    """
    REQUIRED: Public API endpoint returning an HTML dashboard with all data.
    """
    
    sentiments_cursor = app.mongodb["sentiments"].find().to_list(1000)
    gps_cursor = app.mongodb["gps_coordinates"].find().to_list(1000)
    vlogs_cursor = app.mongodb["vlogs"].find().to_list(1000)

    sentiments_data, gps_data, vlogs_data = await asyncio.gather(
        sentiments_cursor, gps_cursor, vlogs_cursor
    )

    sentiments_data = serialize_mongodb_data(sentiments_data)
    gps_data = serialize_mongodb_data(gps_data)
    vlogs_data = serialize_mongodb_data(vlogs_data)
    
    # Filter out incomplete documents before rendering 
    sentiments_data = [d for d in sentiments_data if d.get("entry_id")]
    gps_data = [d for d in gps_data if d.get("entry_id")]
    vlogs_data = [d for d in vlogs_data if d.get("entry_id")]

    # Sort data by entry_id (which is now sequential) for better presentation
    sentiments_data.sort(key=lambda x: x.get('entry_id', ''))
    gps_data.sort(key=lambda x: x.get('entry_id', ''))
    vlogs_data.sort(key=lambda x: x.get('entry_id', ''))

    return templates.TemplateResponse(
        "dashboard.html", 
        {
            "request": request,
            "sentiments": sentiments_data,
            "gps": gps_data,
            "vlogs": vlogs_data,
            "current_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

# ====================================================================
# V. Video Download API (REQUIRED: Backend URI for download) (Unchanged logic)
# ====================================================================

@app.get("/download/video/{entry_id}", tags=["Video Download"])
async def download_video(entry_id: str):
    """
    Provides a backend URI for video download by redirecting to the stored video_url.
    """
    vlog_entry = await app.mongodb["vlogs"].find_one({"entry_id": entry_id})
    
    if vlog_entry and vlog_entry.get("video_url"):
        video_url = vlog_entry["video_url"]
        
        if video_url.startswith("file://") or video_url.startswith("content://"):
            raise HTTPException(
                status_code=500, 
                detail="Stored video link is a local file URI and cannot be served by the public backend."
            )

        return RedirectResponse(url=video_url, status_code=status.HTTP_302_FOUND)
    else:
        raise HTTPException(status_code=404, detail=f"Video link not found for entry ID: {entry_id}")


# ====================================================================
# VI. Health Check (Unchanged)
# ====================================================================

@app.get("/", tags=["Health Check"])
async def health_check():
    """Service health check."""
    return {"status": "ok", "service": "EmoGo Backend", "version": "v1"}

# ====================================================================
# VII. DATA CLEAR ROUTE (FOR TESTING/RESET) ? NEW SECTION ?
# ====================================================================

@app.delete("/api/v1/data/clear-all", tags=["Admin/Testing"])
async def clear_all_data():
    """
    Clears all data from the three main collections and resets the counter.
    This should be used carefully, primarily for testing purposes.
    """
    try:
        # 1. Delete all documents from the three main data collections
        await app.mongodb["sentiments"].delete_many({})
        await app.mongodb["gps_coordinates"].delete_many({})
        await app.mongodb["vlogs"].delete_many({})
        
        # 2. Reset the sequential ID counter
        await app.mongodb[COUNTER_COLLECTION].delete_many({})
        
        return {
            "message": "All data cleared successfully.",
            "deleted_collections": ["sentiments", "gps_coordinates", "vlogs"],
            "counter_status": "reset"
        }
    except Exception as e:
        print(f"Error during data clear: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear data: {e}")