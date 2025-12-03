from resend import Emails
import time, json, logging, redis, requests
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from resume_scorer import initialize_resume_scorer, get_job_score
from utils import (
    GOOGLE_GEMINI_API_KEY,
    LISTINGS_URL,
    RESUME_PATH,
    fetch_apify_jobs,
    hash_job,
    timestamp_to_datetime,
    REDIS_URL,
    EMAILS,
)




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




r = redis.from_url(REDIS_URL, decode_responses=True)
JOB_TTL_SECONDS = 3 * 24 * 60 * 60




# ------------------ Redis helpers ------------------
def _set_status(job_id, v):
    r.setex(f"status:{job_id}", JOB_TTL_SECONDS, v)

def _set_progress(job_id, v):
    r.setex(f"progress:{job_id}", JOB_TTL_SECONDS, int(v))

def _set_result(job_id, obj):
    r.setex(f"result:{job_id}", JOB_TTL_SECONDS, json.dumps(obj))




# ------------------ Helper to build description ------------------
def build_job_desc(job, title_key, company_key):
    return (
        f"Title: {job.get(title_key, '')}\n"
        f"Company: {job.get(company_key, '')}\n"
        f"Location: {job.get('locations', job.get('Location', ''))}\n"
        f"Terms: {job.get('terms', job.get('Employment type', ''))}\n"
        f"Sponsorship: {job.get('sponsorship', '')}\n"
        f"Degrees: {job.get('degrees', '')}"
    )




# ------------------ Retry-with-backoff scoring ------------------
def score_job(job, title_key, company_key, retries=4, base_wait=30):
    for attempt in range(retries):
        try:
            job_desc = build_job_desc(job, title_key, company_key)
            score = get_job_score(job.get(title_key, ""), job_desc, job.get(company_key, ""))
            if "resume_score" in job:
                job["resume_score"] = score
            else:
                job["Resume Score"] = score
            return job
        except Exception as e:
            err = str(e)
            if ("RESOURCE_EXHAUSTED" in err or "429" in err) and attempt < retries - 1:
                wait = (attempt + 1) * base_wait
                logger.warning(f"[Backoff] Gemini quota hit (attempt {attempt+1}/{retries}), retrying in {wait}s...")
                time.sleep(wait)
                continue
            else:
                logger.warning(f"Scoring failed for {job.get(title_key)}: {e}")
                break
    return job




# ------------------ HTML helpers ------------------
def normalize_simplify_jobs(jobs):
    out = []
    for j in jobs:
        out.append({
            "title": j.get("title"),
            "company_name": j.get("company_name") or j.get("companyName"),
            "terms": ", ".join(j.get("terms", [])),
            "date_posted": j.get("date_posted"),
            "url": j.get("url") or j.get("jobUrl"),
            "locations": ", ".join(j.get("locations", [])),
            "sponsorship": j.get("sponsorship"),
            "degrees": ", ".join(j.get("degrees", [])),
            "resume_score": j.get("resume_score", 0),
        })
    return out




def generate_html_table(title, jobs, is_linkedin=False):
    if not jobs:
        return f"<h3>{title}</h3><p>No new jobs found.</p>"
    if not is_linkedin:
        allowed = ["title", "company_name", "terms", "date_posted", "url",
                   "locations", "sponsorship", "degrees",
                   "resume_score"]
        jobs = [{k: v for k, v in j.items() if k in allowed} for j in jobs]
        headers = allowed
    else:
        headers = list(jobs[0].keys())
    html = f"<h3>{title}</h3><table border=1 cellpadding=8 cellspacing=0 style='border-collapse:collapse;width:100%;font-family:Arial;'>"
    html += "<thead style='background:#f2f2f2;'><tr>" + "".join(f"<th>{h}</th>" for h in headers) + "</tr></thead><tbody>"
    for j in jobs:
        html += "<tr>"
        for h in headers:
            v = j.get(h, "")
            if isinstance(v, list): v = ", ".join(v)
            if isinstance(v, (int, float)): v = str(v)
            if h in ["url", "company_url", "Job Url"]:
                v = f'<a href="{v}">Link</a>' if v else ""
            html += f"<td>{v}</td>"
        html += "</tr>"
    html += "</tbody></table><br>"
    return html




# ------------------ Main background job ------------------
def process_recent_jobs_background(job_id: str, minutes: int = 120):
    try:
        _set_status(job_id, "started")
        _set_progress(job_id, 0)
        logger.info(f"Job {job_id} started")

        # STEP 1 – Simplify listings
        resp = requests.get(LISTINGS_URL, timeout=20)
        resp.raise_for_status()
        listings = resp.json()
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        recent = [
            j for j in listings
            if j.get("active") and j.get("is_visible")
            and timestamp_to_datetime(int(j.get("date_posted", 0))) >= cutoff
        ]
        simplify_jobs = normalize_simplify_jobs(recent)

        # DEDUPLICATE simplify jobs
        simplify_new_jobs = []
        for j in simplify_jobs:
            h = hash_job(j)
            if not r.exists(h):
                simplify_new_jobs.append(j)
                r.setex(h, JOB_TTL_SECONDS, j.get("url", ""))
        logger.info(f"{len(simplify_new_jobs)} new Simplify jobs found (out of {len(simplify_jobs)}).")
        _set_progress(job_id, 25)

        # STEP 2 – Apify (LinkedIn)
        apify_norm, apify_new_jobs = [], []
        try:
            apify_jobs = fetch_apify_jobs()
            for j in apify_jobs:
                n = {
                    "Title": j.get("title"),
                    "Company Name": j.get("companyName"),
                    "Location": j.get("location"),
                    "Posted time": j.get("postedTime"),
                    "Job Url": j.get("jobUrl"),
                    "Applications count": j.get("applicationsCount"),
                    "Employment type": j.get("contractType"),
                    "Resume Score": 0,
                }
                apify_norm.append(n)
                h = hash_job(n)
                if not r.exists(h):
                    apify_new_jobs.append(n)
                    r.setex(h, JOB_TTL_SECONDS, n.get("Job Url", ""))
            logger.info(f"{len(apify_new_jobs)} new LinkedIn jobs found (out of {len(apify_norm)}).")
        except Exception as e:
            logger.warning(f"Failed to fetch Apify jobs: {e}. Continuing with Simplify jobs only.")
        _set_progress(job_id, 75)

        # STEP 3 – Resume scoring (only new jobs)
        if GOOGLE_GEMINI_API_KEY and RESUME_PATH:
            logger.info("Initializing resume scorer…")
            initialize_resume_scorer(resume_url=RESUME_PATH)
            all_jobs = []
            for j in simplify_new_jobs:
                if j.get("title") and j.get("company_name"):
                    all_jobs.append(("simplify", j))
            for j in apify_new_jobs:
                if j.get("Title") and j.get("Company Name"):
                    all_jobs.append(("linkedin", j))

            logger.info(f"Scoring {len(all_jobs)} new jobs in parallel…")
            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = []
                for src, j in all_jobs:
                    if src == "simplify":
                        futs.append(ex.submit(score_job, j, "title", "company_name"))
                    else:
                        futs.append(ex.submit(score_job, j, "Title", "Company Name"))
                for _ in as_completed(futs):
                    pass
            logger.info("✅ Resume scoring completed.")
        else:
            logger.info("Resume scoring skipped – missing API key or resume path.")
        _set_progress(job_id, 85)

        # STEP 4 – Email summary
        html = "<div style='font-family:Arial;'>"
        html += generate_html_table("🧩 Simplify Jobs", simplify_new_jobs)
        html += generate_html_table("💼 LinkedIn Jobs", apify_new_jobs, is_linkedin=True)
        html += "</div>"

        Emails.send({
            "from": "alerts@resend.dev",
            "to": EMAILS.split(","),
            "subject": f"⭐ Grad INTERN List : {len(simplify_new_jobs)} New Simplify + {len(apify_new_jobs)} New LinkedIn Jobs ⭐",
            "html": html,
        })

        result = {
            "recent_jobs_count": len(simplify_jobs),
            "new_simplify_jobs": len(simplify_new_jobs),
            "total_apify_jobs": len(apify_norm),
            "new_apify_jobs": len(apify_new_jobs),
        }
        _set_progress(job_id, 100)
        _set_status(job_id, "finished")
        _set_result(job_id, result)
        logger.info(f"Job {job_id} finished → {result}")
        return result

    except Exception as e:
        logger.exception("Unhandled error in background job")
        _set_progress(job_id, 100)
        _set_status(job_id, "failed")
        _set_result(job_id, {"error": str(e)})
        return {"error": str(e)}
