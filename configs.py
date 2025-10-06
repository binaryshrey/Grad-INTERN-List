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

resend.api_key = RESEND_APIKEY




# Utility function to convert UNIX timestamp to datetime
def timestamp_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)




# --- Helper to hash jobs so we can detect new ones ---
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
    



def format_jobs_html_table(jobs):
    """Generate an HTML table from a list of job dicts"""
    if not jobs:
        return "<p>No new internships from Simpllify in the last 60 minutes.</p>"

    html = """
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
      <thead>
        <tr style="background-color: #f2f2f2;">
          <th>Company Name</th>
          <th>Title</th>
          <th>URL</th>
          <th>Terms</th>
          <th>Date Posted</th>
          <th>Locations</th>
          <th>Sponsorships</th>
          <th>Degrees</th>
        </tr>
      </thead>
      <tbody>
    """

    for job in jobs:
        date_posted = datetime.fromtimestamp(job.get("date_posted", 0)).strftime("%m/%d/%Y")
        locations = ", ".join(job.get("locations", []))
        terms = ", ".join(job.get("terms", []))
        degrees = ", ".join(job.get("degrees", []))
        sponsorship = job.get("sponsorship", "N/A")
        url = job.get("url", "#")
        company_name = job.get("company_name", "N/A")
        title = job.get("title", "N/A")

        html += f"""
        <tr>
            <td>{company_name}</td>
            <td>{title}</td>
            <td><a href="{url}">Link</a></td>
            <td>{terms}</td>
            <td>{date_posted}</td>
            <td>{locations}</td>
            <td>{sponsorship}</td>
            <td>{degrees}</td>
        </tr>
        """

    html += "</tbody></table>"
    return html




def sendEmailAlert(recent_jobs, attachment=None, attachment_name="linkedin_jobs_24h.xlsx"):
    html_content = format_jobs_html_table(recent_jobs)

    email_data = {
        "from": "alerts@resend.dev",
        "to": EMAILS,
        "subject": f"⭐️ Grad INTERN List : {len(recent_jobs)} New Internship(s) Posted ⭐️",
        "html": html_content
    }

    if attachment:
        encoded_attachment = base64.b64encode(attachment.getvalue()).decode("utf-8")
        email_data["attachments"] = [
            {
                "content": encoded_attachment,
                "filename": attachment_name,
                "type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
        ]

    import resend
    r = resend.Emails.send(email_data)
    return r