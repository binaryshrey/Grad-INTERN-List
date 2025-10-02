import os, resend
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta


load_dotenv()


def env_get(env_var: str) -> str:
    val = os.environ.get(env_var)
    if not val:
        raise KeyError(f"Env variable '{env_var}' is not set!")
    return val


LISTINGS_URL = env_get("LISTINGS_URL")
RESEND_APIKEY = env_get("RESEND_APIKEY")
EMAILS = env_get("EMAILS")

resend.api_key = RESEND_APIKEY


# Utility function to convert UNIX timestamp to datetime
def timestamp_to_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def format_jobs_html_table(jobs):
    """Generate an HTML table from a list of job dicts"""
    if not jobs:
        return "<p>No new internships in the last 30 minutes.</p>"

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


def sendEmailAlert(recent_jobs):
    html_content = format_jobs_html_table(recent_jobs)

    r = resend.Emails.send({
        "from": "alerts@resend.dev",
        "to": EMAILS,
        "subject": f"⭐️ Grad INTERN List : {len(recent_jobs)} New Internship(s) Posted ⭐️",
        "html": html_content
    })






