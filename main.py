import pandas as pd
import requests, logging, io
from typing import List, Dict
from apify_client import ApifyClient
from openpyxl import load_workbook
from fastapi import FastAPI, HTTPException, Request
from datetime import datetime, timezone, timedelta
from configs import APIFY_ACTOR_ID, APIFY_TOKEN, LISTINGS_URL, sendEmailAlert, timestamp_to_datetime


logger = logging.getLogger(__file__)


app = FastAPI(
    title='Grad INTERN List v1',
    description='Grad INTERN List v1',
    version='0.0.1',
)


def fetch_apify_jobs(title="summer 2026 intern", location="United States", rows=150):
    client = ApifyClient(APIFY_TOKEN)
    run_input = {
        "title": title,
        "location": location,
        "publishedAt": "r86400",  # past 24 hours
        "rows": rows,
        "proxy": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
        },
    }

    logger.info("Running Apify LinkedIn Jobs Scraper...")
    run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    logger.info(f"Fetched {len(items)} jobs from Apify.")
    return items


# health
@app.get('/health')
async def check_alive(request: Request):
    return {'message': 'alive'}


@app.get("/recent_jobs", response_model=Dict)
def get_recent_jobs(minutes: int = 60):
    # Fetch simplify listings
    try:
        response = requests.get(LISTINGS_URL, timeout=10)
        response.raise_for_status()
        listings = response.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Failed to fetch listings.json: {e}")

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=minutes)

    recent_jobs = []
    for job in listings:
        if not job.get("active", False) or not job.get("is_visible", True):
            continue
        date_posted_ts = job.get("date_posted")
        if not date_posted_ts:
            continue
        try:
            date_posted = timestamp_to_datetime(int(date_posted_ts))
        except Exception:
            continue
        if date_posted >= cutoff:
            recent_jobs.append(job)


    # Fetch LinkedIn jobs
    apify_jobs = fetch_apify_jobs()
    apify_normalized = []
    for job in apify_jobs:
        apify_normalized.append({
            "Title": job.get("title"),
            "Company Name": job.get("companyName"),
            "Location": job.get("location"),
            "Posted time": job.get("postedTime"),
            "Published at": job.get("publishedAt"),
            "Job Url": job.get("jobUrl"),
            "Applications count": job.get("applicationsCount"),
            "Employment type": job.get("contractType"),
        })

    # Save Excel with clickable URLs
    if recent_jobs or apify_normalized:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            if recent_jobs:
                pd.DataFrame(recent_jobs).to_excel(writer, sheet_name="recent_jobs", index=False)
            if apify_normalized:
                pd.DataFrame(apify_normalized).to_excel(writer, sheet_name="apify_jobs", index=False)
        excel_buffer.seek(0)

        # Make Job Url clickable in both sheets
        wb = load_workbook(excel_buffer)
        for sheet_name in ["recent_jobs", "apify_jobs"]:
            if sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                for idx, cell in enumerate(ws[1], start=1):
                    if cell.value == "Job Url":
                        job_url_col = idx
                        break
                else:
                    job_url_col = None
                if job_url_col:
                    for row in range(2, ws.max_row + 1):
                        cell = ws.cell(row=row, column=job_url_col)
                        if cell.value:
                            cell.hyperlink = cell.value
                            cell.style = "Hyperlink"
        # Save workbook back to BytesIO
        excel_buffer = io.BytesIO()
        wb.save(excel_buffer)
        excel_buffer.seek(0)

        try:
            sendEmailAlert(
                recent_jobs,
                attachment=excel_buffer,
                attachment_name="linkedin_jobs_24h.xlsx"
            )
            logger.info("Email sent with clickable URLs!")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")

    return {
        "recent_jobs_count": len(recent_jobs),
        "apify_jobs_count": len(apify_normalized)
    }