from resend import Emails
import logging, redis, json, requests
from datetime import datetime, timezone, timedelta
from utils import (
    LISTINGS_URL,
    fetch_apify_jobs,
    hash_job,
    timestamp_to_datetime,
    REDIS_URL,
    EMAILS,
)




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)





# Redis connection for tracking job progress/status/result
r = redis.from_url(REDIS_URL, decode_responses=True)
JOB_TTL_SECONDS = 3 * 24 * 60 * 60




# --- Redis helper functions ---
def _set_status(job_id: str, value: str):
    r.setex(f"status:{job_id}", JOB_TTL_SECONDS, value)


def _set_progress(job_id: str, value: int):
    r.setex(f"progress:{job_id}", JOB_TTL_SECONDS, int(value))


def _set_result(job_id: str, obj):
    r.setex(f"result:{job_id}", JOB_TTL_SECONDS, json.dumps(obj))





# --- Normalize Simplify jobs to filtered columns ---
def normalize_simplify_jobs(jobs):
    """Map Simplify jobs to the filtered columns for email table."""
    normalized = []
    for job in jobs:
        normalized.append({
            "title": job.get("title"),
            "company_name": job.get("company_name") or job.get("companyName"),
            "terms": ", ".join(job.get("terms", [])),
            "url": job.get("url") or job.get("jobUrl"),
            "locations": ", ".join(job.get("locations", [])),
            "sponsorship": job.get("sponsorship"),
            "degrees": ", ".join(job.get("degrees", [])),
        })
    return normalized




# --- HTML table generator ---
def generate_html_table(title, jobs, is_linkedin=False):
    """Generate HTML table with clickable Link text for URLs."""
    if not jobs:
        return f"<h3>{title}</h3><p>No new jobs found.</p>"

    # Simplify table: keep only filtered columns
    if not is_linkedin:
        allowed_cols = ["title", "company_name", "terms", "date_posted", "url", "locations", "sponsorship", "degrees"]
        jobs_filtered = [{k: v for k, v in job.items() if k in allowed_cols} for job in jobs]
        headers = allowed_cols
    else:
        jobs_filtered = jobs
        headers = list(jobs[0].keys())

    html = f"""
    <h3 style="font-family: Arial; color: #333;">{title}</h3>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; width: 100%;">
      <thead style="background-color: #f2f2f2;">
        <tr>
    """
    for header in headers:
        html += f"<th>{header}</th>"
    html += "</tr></thead><tbody>"

    for job in jobs_filtered:
        html += "<tr>"
        for header in headers:
            val = job.get(header, "")
            if isinstance(val, list):
                val = ", ".join(val)
            if isinstance(val, (int, float)):
                val = str(val)

            # clickable "Link" for URL columns
            if header in ["url", "company_url", "Job Url"]:
                val = f'<a href="{val}">Link</a>' if val else ""
            else:
                # escape other text
                if isinstance(val, str):
                    val = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html += f"<td>{val}</td>"
        html += "</tr>"
    html += "</tbody></table><br>"
    return html




# --- Main background job function ---
def process_recent_jobs_background(job_id: str, minutes: int = 60):
    """Background job that fetches Simplify and LinkedIn jobs, then emails HTML tables."""
    try:
        _set_status(job_id, "started")
        _set_progress(job_id, 0)
        logger.info(f"Job {job_id} started (threaded)")

        # --- STEP 1: Fetch Simplify listings ---
        try:
            resp = requests.get(LISTINGS_URL, timeout=20)
            resp.raise_for_status()
            listings = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch listings.json: {e}")
            _set_status(job_id, "failed")
            _set_progress(job_id, 100)
            _set_result(job_id, {"error": str(e)})
            return {"error": str(e)}

        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        recent_jobs = [
            j for j in listings
            if j.get("active", False) and j.get("is_visible", True)
            and timestamp_to_datetime(int(j.get("date_posted", 0))) >= cutoff
        ]

        simplify_jobs = normalize_simplify_jobs(recent_jobs)
        _set_progress(job_id, 25)

        # --- STEP 2: Fetch LinkedIn / Apify jobs ---
        apify_jobs = fetch_apify_jobs()
        apify_normalized = []
        new_jobs = []

        for j in apify_jobs:
            norm = {
                "Title": j.get("title"),
                "Company Name": j.get("companyName"),
                "Location": j.get("location"),
                "Posted time": j.get("postedTime"),
                "Job Url": j.get("jobUrl"),
                "Applications count": j.get("applicationsCount"),
                "Employment type": j.get("contractType"),
            }
            apify_normalized.append(norm)
            job_hash = hash_job(norm)
            if not r.exists(job_hash):
                new_jobs.append(norm)
                r.setex(job_hash, JOB_TTL_SECONDS, norm.get("Job Url", ""))

        _set_progress(job_id, 75)

        # --- STEP 3: Build HTML email body ---
        html_content = "<div style='font-family: Arial, sans-serif;'>"
        html_content += generate_html_table("🧩 Simplify Jobs", simplify_jobs)
        html_content += generate_html_table("💼 LinkedIn Jobs", apify_normalized, is_linkedin=True)
        html_content += "</div>"

        # --- STEP 4: Send email directly ---
        Emails.send({
            "from": "alerts@resend.dev",
            "to": EMAILS.split(","),
            "subject": f"⭐️ Grad INTERN List : {len(simplify_jobs)} Simplify + {len(new_jobs)} LinkedIn Jobs ⭐️",
            "html": html_content
        })

        # --- STEP 5: Update Redis and finish ---
        result = {
            "recent_jobs_count": len(simplify_jobs),
            "total_apify_jobs": len(apify_normalized),
            "new_apify_jobs": len(new_jobs),
        }

        _set_progress(job_id, 100)
        _set_status(job_id, "finished")
        _set_result(job_id, result)
        logger.info(f"Job {job_id} finished: {result}")
        return result

    except Exception as exc:
        logger.exception("Unhandled error in background job")
        _set_progress(job_id, 100)
        _set_status(job_id, "failed")
        _set_result(job_id, {"error": str(exc)})
        return {"error": str(exc)}
