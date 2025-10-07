import os, resend, base64, hashlib
from dotenv import load_dotenv
from datetime import datetime, timezone
from apify_client import ApifyClient

load_dotenv()

def env_get(env_var: str) -> str:
    val = os.environ.get(env_var)
    if not val:
        raise KeyError(f"Env variable '{env_var}' is not set!")
    return val

LISTINGS_URL = env_get("LISTINGS_URL")
RESEND_APIKEY = env_get("RESEND_APIKEY")
EMAILS = env_get("EMAILS")
APIFY_TOKEN = env_get("APIFY_TOKEN")
APIFY_ACTOR_ID = env_get("APIFY_ACTOR_ID")
REDIS_URL = env_get("REDIS_URL")
GOOGLE_GEMINI_API_KEY = env_get("GOOGLE_GEMINI_API_KEY")
RESUME_PATH = env_get("RESUME_PATH")

resend.api_key = RESEND_APIKEY




def timestamp_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)




def hash_job(job: dict) -> str:
    title = job.get('Title') or job.get('title', '')
    company = job.get('Company Name') or job.get('companyName', '')
    url = job.get('Job Url') or job.get('jobUrl', '')
    key_fields = f"{title}-{company}-{url}"
    return hashlib.sha256(key_fields.encode()).hexdigest()




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
    run = client.actor(APIFY_ACTOR_ID).call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    return items
