from typing import List, Dict
from configs import LISTINGS_URL, sendEmailAlert, timestamp_to_datetime
from fastapi import FastAPI, HTTPException, Request
from datetime import datetime, timezone, timedelta
import requests, logging


logger = logging.getLogger(__file__)


app = FastAPI(
    title='Grad INTERN List v1',
    description='Grad INTERN List v1',
    version='0.0.1',
)


# health
@app.get('/health')
async def check_alive(request: Request):
    return {'message': 'alive'}


@app.get("/recent_jobs", response_model=List[Dict])
def get_recent_jobs(minutes: int = 30):
    try:
        response = requests.get(LISTINGS_URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch listings.json: {e}")

    try:
        listings = response.json()
    except ValueError:
        raise HTTPException(status_code=500, detail="Invalid JSON format from listings.json")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=minutes)

    recent_jobs = []

    for job in listings:
        # Skip inactive or invisible listings
        if not job.get("active", False) or not job.get("is_visible", True):
            continue

        date_posted_ts = job.get("date_posted")
        if date_posted_ts is None:
            continue  # skip if no timestamp

        try:
            date_posted = timestamp_to_datetime(int(date_posted_ts))
        except Exception:
            continue  # skip if timestamp invalid

        if date_posted >= cutoff:
            recent_jobs.append(job)

    if recent_jobs:
        logger.info(f"Found {len(recent_jobs)} recent jobs posted in the last {minutes} minutes.")
        sendEmailAlert(recent_jobs)
        
    return recent_jobs
